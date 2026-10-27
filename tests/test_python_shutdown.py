"""Exercise the compiled PyO3 API in disposable Python processes, without a GPU.

Run after cargo build --features python --lib and cargo build --bins:
python -m unittest discover -s tests -p test_python_shutdown.py
"""
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
BINS = Path(os.environ.get('GPU_QUEUE_TEST_BIN_DIR', str(ROOT / 'target/debug')))
EXTENSION = Path(os.environ.get('GPU_QUEUE_TEST_EXTENSION', str(BINS / 'libgpu_arbiter.so')))


def worker(scenario, socket_path, checkpoint, directory):
    import gpu_queue
    checkpoint = Path(checkpoint)

    def save(reason):
        if scenario == 'reentrant':
            lease.shutdown_started(reason.request_id)
        checkpoint.write_text(json.dumps({'reason': reason.reason, 'request_id': reason.request_id}))
        if scenario == 'slow_handler':
            time.sleep(20)

    expected = {'no_estimate': None, 'explicit_no_estimate': -1}.get(scenario, 60)
    lease = gpu_queue.Lease(socket=socket_path, shutdown_handler=save, expected_runtime_secs=expected,
                            shutdown_grace_secs=0.5 if scenario == 'slow_handler' else 2,
                            independent=scenario == 'independent', runtime_dir=directory,
                            daemon_executable=str(BINS / 'gpu-switch'))
    if scenario == 'collected':
        import gc
        def cyclic_lease():
            def callback(reason):
                result.shutdown_started(reason.request_id)
            result = gpu_queue.Lease(socket=socket_path, shutdown_handler=callback)
            return result
        lease = cyclic_lease()
        lease.acquire()
        claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
        claim.write_text('mem=1 sm=50 type=C')
        claim.unlink()
        del lease
        gc.collect()
        with gpu_queue.Lease(socket=socket_path):
            claim.write_text('mem=1 sm=50 type=C')
            claim.unlink()
        return
    if scenario == 'queued_interrupt':
        import threading
        def interrupt():
            time.sleep(0.2)
            os.kill(os.getpid(), signal.SIGINT)
        threading.Thread(target=interrupt, daemon=True).start()
        try:
            lease.acquire()
        except KeyboardInterrupt:
            checkpoint.write_text('interrupted while queued')
            return
        raise AssertionError('queued acquisition ignored the interrupt')
    try:
        with lease:
            if scenario == 'fork':
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=50 type=C')
                child = os.fork()
                if child == 0:
                    try:
                        lease.shutdown_started('parent-request')
                    except RuntimeError:
                        os._exit(0)
                    os._exit(2)
                _, status = os.waitpid(child, 0)
                assert os.waitstatus_to_exitcode(status) == 0
                with socket.socket(socket.AF_UNIX) as connection:
                    connection.connect(socket_path)
                    connection.sendall(b'{"op":"status"}\n')
                    response = json.loads(connection.makefile().readline())
                    assert any(job['state'] == 'running' for job in response['status']['jobs']), response
                claim.unlink()
                return
            if scenario == 'retained':
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=0 type=C')
            if scenario == 'failure':
                raise ValueError('deliberate worker failure')
            if scenario in ('normal', 'independent', 'no_estimate', 'explicit_no_estimate'):
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=50 type=C')
                time.sleep(0.4)
                claim.unlink()
                return
            if scenario == 'catch':
                try:
                    while True:
                        time.sleep(0.02)
                except gpu_queue.GpuLeaseInterrupted as reason:
                    assert reason.permanent
                    try:
                        lease.shutdown_started('stale-request')
                    except RuntimeError:
                        pass
                    else:
                        raise AssertionError('stale acknowledgement was accepted')
                    lease.run_shutdown_handler()
                    return
            while True:
                time.sleep(0.02)
    except gpu_queue.GpuLeaseInterrupted as reason:
        assert checkpoint.exists()
        assert reason.reason == json.loads(checkpoint.read_text())['reason']
        if scenario == 'retained':
            time.sleep(0.6)
        return


class PythonShutdownTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='gpu-queue-python-')
        self.directory = Path(self.temp.name)
        self.gpu = self.directory / 'gpu'
        self.gpu.mkdir()
        self.bin = self.directory / 'bin'
        self.bin.mkdir()
        (self.bin / 'nvidia-smi').symlink_to(BINS / 'ninfer-mock')
        package = self.directory / 'gpu_queue'
        package.mkdir()
        (package / '__init__.py').write_text('from ._native import Lease, GpuLeaseInterrupted\n')
        (package / '_native.so').symlink_to(EXTENSION)
        self.socket = self.directory / 'host.sock'
        self.state = self.directory / 'state'
        self.environment = dict(os.environ, PYTHONPATH=str(self.directory),
                                NINFER_MOCK_GPU_DIR=str(self.gpu),
                                PATH=str(self.bin) + ':' + os.environ.get('PATH', ''),
                                GPU_SWITCH_SOCKET=str(self.directory / 'absent-system.sock'))
        config = self.directory / 'config.json'
        config.write_text(json.dumps({'listen': [{'name': 'host', 'path': str(self.socket)}],
            'nvidia_smi': str(self.bin / 'nvidia-smi'), 'state_dir': str(self.state),
            'startup_idle_secs': 0.2, 'idle_secs': 0.3, 'gpu_poll_interval_secs': 0.05,
            'sigterm_to_sigkill_secs': 0.1}))
        self.log = (self.directory / 'daemon.log').open('w')
        self.daemon = subprocess.Popen([str(BINS / 'gpu-switch'), '--config', str(config)],
            env=self.environment, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 5
        while not self.socket.exists():
            if time.monotonic() > deadline:
                self.fail('daemon did not start')
            time.sleep(0.02)

    def tearDown(self):
        self.daemon.terminate()
        self.daemon.wait(timeout=5)
        self.log.close()
        self.temp.cleanup()

    def run_worker(self, scenario):
        checkpoint = self.directory / 'checkpoint.json'
        runtime = self.directory / 'independent'
        process = subprocess.run([sys.executable, str(Path(__file__).resolve()), 'worker', scenario,
            str(self.socket), str(checkpoint), str(runtime)], env=self.environment,
            capture_output=True, text=True, timeout=8)
        return process, checkpoint

    def events(self):
        path = self.state / 'failures.jsonl'
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            events = [json.loads(line) for line in path.read_text().splitlines()]
            if any(event['event'] == 'terminal_failure' for event in events):
                return events
            time.sleep(0.02)
        return events

    def test_unwinding_runs_checkpoint_with_the_interrupt_reason(self):
        process, checkpoint = self.run_worker('unwind')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertIn('VRAM', json.loads(checkpoint.read_text())['reason'])
        self.assertTrue(any(event['event'] == 'shutdown_handler_started' for event in self.events()))

    def test_catching_interrupt_can_run_cleanup_explicitly(self):
        process, checkpoint = self.run_worker('catch')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertTrue(checkpoint.exists())

    def test_checkpoint_callback_can_acknowledge_its_own_request(self):
        process, checkpoint = self.run_worker('reentrant')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertTrue(checkpoint.exists())

    def test_forked_child_does_not_release_the_parent_grant(self):
        process, _ = self.run_worker('fork')
        self.assertEqual(process.returncode, 0, process.stderr)

    def test_collecting_a_callback_cycle_releases_the_native_session(self):
        process, _ = self.run_worker('collected')
        self.assertEqual(process.returncode, 0, process.stderr)

    def test_stalled_checkpoint_still_reaches_termination(self):
        process, checkpoint = self.run_worker('slow_handler')
        self.assertEqual(process.returncode, -signal.SIGTERM, process.stderr)
        self.assertTrue(checkpoint.exists())
        self.assertTrue(any(event['event'] == 'shutdown_handler_started' for event in self.events()))

    def test_gpu_context_teardown_uses_the_existing_shutdown_deadline(self):
        process, checkpoint = self.run_worker('retained')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertTrue(checkpoint.exists())
        self.assertFalse(any(event['event'] == 'unclean_release' for event in self.events()))

    def test_queued_acquisition_remains_interruptible(self):
        blocker = subprocess.Popen([str(BINS / 'ninfer-mock'), 'lease-client',
            '--exclusive', '--mem', '1', '--sm', '50', '--hold-ms', '2000'],
            env=dict(self.environment, GPU_SWITCH_SOCKET=str(self.socket)),
            stdout=subprocess.PIPE, text=True)
        try:
            for line in blocker.stdout:
                if line.strip() == 'granted':
                    break
            process, checkpoint = self.run_worker('queued_interrupt')
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertIn('interrupted', checkpoint.read_text())
        finally:
            blocker.terminate()
            blocker.wait(timeout=5)
            blocker.stdout.close()

    def test_python_failure_is_recorded(self):
        process, _ = self.run_worker('failure')
        self.assertNotEqual(process.returncode, 0)
        self.assertTrue(any('deliberate worker failure' in str(event) for event in self.events()))

    def test_gpu_use_then_clean_teardown_succeeds(self):
        process, checkpoint = self.run_worker('normal')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertFalse(checkpoint.exists())

    def test_missing_estimate_warns_and_explicit_unknown_suppresses_it(self):
        for scenario, warns in [('no_estimate', True), ('explicit_no_estimate', False)]:
            process, _ = self.run_worker(scenario)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual('warning: GPU duration estimate omitted' in process.stderr, warns, process.stderr)

    def test_independent_mode_starts_the_existing_daemon_engine(self):
        process, _ = self.run_worker('independent')
        runtime_socket = self.directory / 'independent/queue.sock'
        try:
            self.assertEqual(process.returncode, 0, process.stderr)
            with socket.socket(socket.AF_UNIX) as connection:
                connection.connect(str(runtime_socket))
                pid, _, _ = struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + 5
            while runtime_socket.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertFalse(runtime_socket.exists())
        finally:
            if runtime_socket.exists():
                with socket.socket(socket.AF_UNIX) as connection:
                    connection.connect(str(runtime_socket))
                    pid, _, _ = struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                    os.kill(pid, signal.SIGKILL)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'worker':
        worker(*sys.argv[2:])
    else:
        unittest.main()
