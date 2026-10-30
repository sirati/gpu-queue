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
    directory = Path(directory)

    def save(reason):
        if scenario.startswith('vram_'):
            (Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())).unlink(missing_ok=True)
        if scenario == 'runtime_limit':
            assert lease.pending_shutdown().request_id == reason.request_id
            (Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())).unlink()
        if scenario == 'reentrant':
            lease.shutdown_started(reason.request_id)
        checkpoint.write_text(json.dumps({'reason': reason.reason, 'request_id': reason.request_id,
                                          'deadline': reason.deadline}))
        if scenario == 'restart_cleanup':
            while not (directory / 'finish').exists():
                time.sleep(0.02)
            lease.shutdown_started(reason.request_id)
        if scenario == 'slow_handler':
            time.sleep(20)

    expected = {'no_estimate': None, 'explicit_no_estimate': -1}.get(scenario, 60)
    lease = gpu_queue.Lease(socket=socket_path, shutdown_handler=save, expected_runtime_secs=expected,
                            category='accuracy' if scenario == 'category' else 'other',
                            max_runtime_secs=1 if scenario == 'runtime_limit' else None,
                            mode='shared' if scenario in ('restart_shared', 'vram_waiter', 'vram_shared') else 'exclusive',
                            vram=(16 << 30) if scenario == 'vram_waiter' else ((31 << 30) if scenario == 'vram_shared' else ((1 << 30) if scenario == 'restart_shared' else 0)),
                            shutdown_grace_secs=0.5 if scenario == 'slow_handler' else (5 if scenario == 'restart_cleanup' else 2),
                            independent=scenario == 'independent', runtime_dir=str(directory),
                            daemon_executable=str(BINS / 'gpu-switch'),
                            watchdog='lenient' if scenario == 'watchdog_lenient' else 'standard')
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
            if scenario.startswith('watchdog_'):
                # VRAM without compute, past the standard 3-second first-compute deadline.
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=0 type=C')
                if scenario == 'watchdog_expect_idle':
                    for invalid in (0, -1, float('nan'), float('inf'), 61):
                        try:
                            lease.expect_idle(invalid)
                        except RuntimeError:
                            pass
                        else:
                            raise AssertionError('unbounded idle expectation accepted')
                    deadline = lease.expect_idle(5)
                    assert lease.expect_idle(5) == deadline
                    try:
                        lease.expect_idle(10)
                    except RuntimeError as error:
                        assert 'once per grant' in str(error)
                    else:
                        raise AssertionError('idle expectation renewed')
                (Path(directory) / 'ready').write_text(str(os.getpid()))
                time.sleep(3.6)
                claim.write_text('mem=1 sm=50 type=C')
                time.sleep(2)
                claim.unlink()
                return
            if scenario.startswith('vram_'):
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=20480 sm=0 type=C' if scenario in ('vram_retain', 'vram_expire') else ('mem=1 sm=0 type=C' if scenario == 'vram_shared' else 'mem=1 sm=50 type=C'))
                if scenario in ('vram_retain', 'vram_expire', 'vram_shared'):
                    for invalid in (0, -1, float('nan'), float('inf')):
                        try:
                            lease.retain_vram(invalid)
                        except RuntimeError:
                            pass
                        else:
                            raise AssertionError('invalid duration accepted')
                    seconds = .25 if scenario == 'vram_expire' else 30
                    deadline = lease.retain_vram(seconds)
                    assert lease.retain_vram(seconds) == deadline
                    try:
                        lease.retain_vram(seconds + 1)
                    except RuntimeError as error:
                        assert 'renewed' in str(error)
                    else:
                        raise AssertionError('retention renewed')
                    checkpoint.write_text(json.dumps({'deadline': deadline}))
                (directory / 'ready').write_text(str(os.getpid()))
                while not (directory / 'finish').exists():
                    if (directory / 'shrink').exists():
                        claim.write_text('mem=4096 sm=0 type=C')
                    if (directory / 'recheck').exists():
                        assert lease.retain_vram(30) == deadline
                        (directory / 'rechecked').touch()
                    time.sleep(.02)
                claim.unlink()
                return
            if scenario.startswith('restart_') and scenario != 'restart_cleanup':
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=50 type=C')
                (directory / 'ready').write_text(str(os.getpid()))
                while not (directory / 'finish').exists():
                    time.sleep(0.02)
                if scenario == 'restart_fork':
                    child = os.fork()
                    if child == 0:
                        try:
                            lease.shutdown_started('parent-request')
                        except RuntimeError:
                            os._exit(0)
                        os._exit(2)
                    _, status = os.waitpid(child, 0)
                    assert os.waitstatus_to_exitcode(status) == 0
                claim.unlink()
                if scenario == 'restart_failure':
                    raise ValueError('failure during daemon restart')
                return
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
            if scenario == 'runtime_limit':
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=50 type=C')
            if scenario == 'retained':
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=0 type=C')
            if scenario == 'failure':
                raise ValueError('deliberate worker failure')
            if scenario == 'failure_before_teardown':
                # An uncaught-exception hook reports while the process still owns its context.
                claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid())
                claim.write_text('mem=1 sm=50 type=C')
                lease.fail('deliberate failure reported before teardown')
                time.sleep(1)
                claim.unlink()
                sys.stdout.flush()
                os._exit(1)
            if scenario == 'cancel':
                lease.cancel('compiler continuation required; attempt incomplete')
                return
            if scenario in ('normal', 'independent', 'no_estimate', 'explicit_no_estimate', 'category'):
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
    def test_owner_cancellation_is_terminal_and_never_done(self):
        process, _ = self.run_worker('cancel')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.wait_for(lambda: any(job['state'] == 'cancelled' for job in self.status()))
        record = next(job for job in self.status() if job['state'] == 'cancelled')
        self.assertEqual(record['error'], 'compiler continuation required; attempt incomplete')

    def test_native_category_reaches_daemon_history(self):
        process, _ = self.run_worker('category')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.wait_for(lambda: any(job['category'] == 'accuracy' and job['state'] == 'done'
                                  for job in self.status()))

    def test_vram_retention_releases_exclusivity_and_admits_by_actual_free_memory(self):
        retained, directory = self.start_worker('vram_retain', 'retainer')
        self.wait_for(lambda: (directory / 'ready').exists())
        record = next(job for job in self.status() if job['runner_pid'] == retained.pid)
        self.assertEqual((record['mode'], record['vram']), ('shared', 0))
        deadline = record['vram_retention_deadline']
        waiter, waiting = self.start_worker('vram_waiter', 'waiter')
        self.wait_for(lambda: any(job['runner_pid'] == waiter.pid for job in self.status()))
        self.assertFalse((waiting / 'ready').exists())
        exclusive, excluding = self.start_worker('vram_exclusive', 'exclusive')
        self.wait_for(lambda: any(job['runner_pid'] == exclusive.pid for job in self.status()))
        self.assertFalse((excluding / 'ready').exists())
        # More than the initial-compute deadline, with no compute at all.
        time.sleep(3.2)
        self.assertIsNone(retained.poll())
        (directory / 'shrink').touch()
        self.wait_for(lambda: (waiting / 'ready').exists())
        self.assertFalse((excluding / 'ready').exists())
        self.assertEqual(next(job for job in self.status() if job['runner_pid'] == retained.pid)
                         ['vram_retention_deadline'], deadline)
        for path in (directory, waiting):
            (path / 'finish').touch()
        self.assertEqual(retained.wait(timeout=5), 0)
        self.assertEqual(waiter.wait(timeout=5), 0)
        self.wait_for(lambda: (excluding / 'ready').exists())
        (excluding / 'finish').touch()
        self.assertEqual(exclusive.wait(timeout=5), 0)

    def test_vram_retention_deadline_and_identity_survive_service_restart(self):
        process, directory = self.start_worker('vram_retain', 'retainer')
        self.wait_for(lambda: (directory / 'ready').exists())
        before = next(job for job in self.status() if job['runner_pid'] == process.pid)
        self.daemon.terminate()
        self.daemon.wait(timeout=5)
        (directory / 'recheck').touch()
        time.sleep(.25)
        self.assertFalse((directory / 'rechecked').exists())
        self.start_daemon()
        self.wait_for(lambda: (directory / 'rechecked').exists())
        after = next(job for job in self.status() if job['runner_pid'] == process.pid)
        self.assertEqual((after['id'], after['vram_retention_deadline'], after['mode']),
                         (before['id'], before['vram_retention_deadline'], 'shared'))
        (directory / 'finish').touch()
        self.assertEqual(process.wait(timeout=5), 0)

    def test_vram_retention_expiry_is_its_own_permanent_watchdog_reason(self):
        process, _ = self.start_worker('vram_expire', 'expired')
        process.wait(timeout=5)
        self.wait_for(lambda: any(job['terminal_status'] == 'vram_retention_expired' for job in self.status()))
        record = next(job for job in self.status() if job['runner_pid'] == process.pid)
        self.assertEqual(record['terminal_status'], 'vram_retention_expired')
        self.assertIn('VRAM retention deadline exceeded', record['error'])

    def test_shared_vram_retention_drops_the_original_reservation(self):
        process, directory = self.start_worker('vram_shared', 'shared-retainer')
        self.wait_for(lambda: (directory / 'ready').exists())
        before = next(job for job in self.status() if job['runner_pid'] == process.pid)
        self.assertEqual((before['mode'], before['vram']), ('shared', 0))
        waiter, waiting = self.start_worker('vram_waiter', 'waiter')
        self.wait_for(lambda: (waiting / 'ready').exists())
        # Original 31 GiB reservation would block this 16 GiB waiter.
        time.sleep(3.2)
        self.assertIsNone(process.poll())
        for path in (directory, waiting):
            (path / 'finish').touch()
        self.assertEqual(process.wait(timeout=5), 0)
        self.assertEqual(waiter.wait(timeout=5), 0)

    def test_vram_retention_refuses_another_process_even_with_a_copied_token(self):
        process, directory = self.start_worker('vram_retain', 'retainer')
        self.wait_for(lambda: (directory / 'ready').exists())
        saved_path = self.state / 'state.json'
        self.wait_for(lambda: saved_path.exists() and any(
            job['runner_pid'] == process.pid and job.get('retention')
            for side in json.loads(saved_path.read_text())['sides'] for job in side['jobs']))
        saved = json.loads(saved_path.read_text())
        job = next(job for side in saved['sides'] for job in side['jobs'] if job['runner_pid'] == process.pid)
        with socket.socket(socket.AF_UNIX) as connection:
            connection.connect(str(self.socket))
            connection.sendall((json.dumps({'op': 'retain_vram', 'seconds': 30,
                'resume': {'id': job['id'], 'token': job['token'], 'key': job['key'], 'running': True}}) + '\n').encode())
            reply = json.loads(connection.makefile().readline())
        self.assertFalse(reply['ok'])
        self.assertIn('authenticated lease owner', reply['error'])
        (directory / 'finish').touch()
        self.assertEqual(process.wait(timeout=5), 0)

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

    def wait_for(self, predicate):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail('restart condition timed out: ' + (self.directory / 'daemon.log').read_text())

    def status(self):
        result = subprocess.run([str(BINS / 'gpu-run'), 'status', '--json'],
            env=dict(self.environment, GPU_SWITCH_SOCKET=str(self.socket)),
            capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)['jobs']

    def start_worker(self, scenario, name):
        directory = self.directory / name
        directory.mkdir()
        checkpoint = directory / 'checkpoint.json'
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'worker', scenario,
            str(self.socket), str(checkpoint), str(directory)], env=self.environment,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self.stop_worker, process)
        return process, directory

    @staticmethod
    def stop_worker(process):
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)

    def restart_daemon(self, lose_state=False):
        self.daemon.terminate()
        self.daemon.wait(timeout=5)
        if lose_state:
            (self.state / 'state.json').unlink()
        # Longer than the GPU poll interval, so EOF and a refused connect are
        # actually exercised before the replacement daemon appears.
        time.sleep(0.25)
        self.start_daemon()

    def start_daemon(self):
        self.daemon = subprocess.Popen([str(BINS / 'gpu-switch'), '--config', str(self.directory / 'config.json')],
            env=self.environment, stdout=self.log, stderr=self.log)
        self.wait_for(self.socket.exists)

    def test_lenient_policy_and_declared_idle_outlast_the_first_compute_deadline(self):
        for scenario in ('watchdog_lenient', 'watchdog_expect_idle'):
            with self.subTest(scenario=scenario):
                process, directory = self.start_worker(scenario, scenario)
                self.wait_for((directory / 'ready').exists)
                job = next(job for job in self.status() if job['pid'] == process.pid)
                self.assertEqual(job['watchdog'], scenario.removeprefix('watchdog_').replace('expect_idle', 'standard'))
                self.assertEqual('expected_idle_until' in job, scenario == 'watchdog_expect_idle')
                _, stderr = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 0, stderr)

    def test_native_release_and_failure_during_daemon_downtime_are_delivered(self):
        # The usual fixture allows only 0.2 seconds without VRAM. Give this
        # explicit 0.25-second outage the production one-second allowance.
        config = self.directory / 'config.json'
        settings = json.loads(config.read_text())
        settings['startup_idle_secs'] = 1
        config.write_text(json.dumps(settings))
        self.restart_daemon()
        for scenario in ['restart_exclusive', 'restart_failure']:
            with self.subTest(scenario=scenario):
                process, directory = self.start_worker(scenario, scenario)
                self.wait_for((directory / 'ready').exists)
                original = next(job for job in self.status() if job['pid'] == process.pid)
                self.daemon.terminate()
                self.daemon.wait(timeout=5)
                (directory / 'finish').touch()
                time.sleep(0.25)
                self.assertIsNone(process.poll(), 'teardown must await the resumed authority')
                self.start_daemon()
                _, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 1 if scenario == 'restart_failure' else 0, stderr)
                self.wait_for(lambda: next(job for job in self.status() if job['id'] == original['id'])['state']
                              == ('failed' if scenario == 'restart_failure' else 'done'))
                if scenario == 'restart_failure':
                    self.assertIn('failure during daemon restart', (self.state / 'failures.jsonl').read_text())

    def test_native_running_leases_survive_repeated_daemon_restarts(self):
        for scenario in ['restart_exclusive', 'restart_shared', 'restart_fork']:
            with self.subTest(scenario=scenario):
                process, directory = self.start_worker(scenario, scenario)
                self.wait_for((directory / 'ready').exists)
                original = next(job for job in self.status() if job['pid'] == process.pid)
                for count in [1, 2]:
                    before = (self.directory / 'daemon.log').read_text().count('resumed by pid')
                    self.restart_daemon()
                    self.wait_for(lambda: (self.directory / 'daemon.log').read_text().count('resumed by pid') > before)
                    resumed = next(job for job in self.status() if job['id'] == original['id'])
                    self.assertEqual(resumed['state'], 'running')
                    self.assertEqual(resumed['pid'], process.pid)
                    self.assertEqual(resumed['runs'], 1)
                    self.assertIsNone(process.poll())
                (directory / 'finish').touch()
                _, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 0, stderr)
                self.assertFalse((directory / 'checkpoint.json').exists())

    def test_native_queued_lease_keeps_its_place_across_restart(self):
        holder, first_dir = self.start_worker('restart_exclusive', 'holder')
        self.wait_for((first_dir / 'ready').exists)
        queued, next_dir = self.start_worker('restart_exclusive', 'queued')
        self.wait_for(lambda: any(job['state'] == 'queued' for job in self.status()))
        original = next(job for job in self.status() if job['state'] == 'queued')
        self.restart_daemon()
        self.wait_for(lambda: (self.directory / 'daemon.log').read_text().count('resumed by pid') >= 2)
        resumed = next(job for job in self.status() if job['id'] == original['id'])
        self.assertEqual(resumed['state'], 'queued')
        self.assertEqual(resumed['runner_pid'], queued.pid)
        self.assertFalse((next_dir / 'ready').exists())
        (first_dir / 'finish').touch()
        _, stderr = holder.communicate(timeout=5)
        self.assertEqual(holder.returncode, 0, stderr)
        self.wait_for((next_dir / 'ready').exists)
        (next_dir / 'finish').touch()
        _, stderr = queued.communicate(timeout=5)
        self.assertEqual(queued.returncode, 0, stderr)

    def test_native_queued_reconnect_remains_interruptible_while_daemon_is_down(self):
        _, first_dir = self.start_worker('restart_exclusive', 'holder')
        self.wait_for((first_dir / 'ready').exists)
        queued, directory = self.start_worker('queued_interrupt', 'queued')
        self.wait_for(lambda: any(job['state'] == 'queued' for job in self.status()))
        self.daemon.terminate()
        self.daemon.wait(timeout=5)
        _, stderr = queued.communicate(timeout=3)
        self.assertEqual(queued.returncode, 0, stderr)
        self.assertEqual((directory / 'checkpoint.json').read_text(), 'interrupted while queued')

    def test_native_checkpoint_survives_restart_without_duplicate_interrupt(self):
        process, directory = self.start_worker('restart_cleanup', 'cleanup')
        checkpoint = directory / 'checkpoint.json'
        self.wait_for(checkpoint.exists)
        shutdown = json.loads(checkpoint.read_text())
        original = next(job for job in self.status() if job['pid'] == process.pid)
        self.assertEqual(original['state'], 'stopping')
        self.restart_daemon()
        self.wait_for(lambda: 'resumed by pid' in (self.directory / 'daemon.log').read_text())
        resumed = next(job for job in self.status() if job['id'] == original['id'])
        self.assertEqual(resumed['state'], 'stopping')
        (directory / 'finish').touch()
        _, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, stderr)
        requests = [json.loads(line) for line in (self.state / 'failures.jsonl').read_text().splitlines()]
        self.assertEqual(sum(event['event'] == 'shutdown_requested' for event in requests), 1)
        for event in requests:
            if event.get('shutdown'):
                self.assertEqual(event['shutdown']['request_id'], shutdown['request_id'])
                self.assertEqual(event['shutdown']['deadline'], shutdown['deadline'])

    def test_native_running_lease_refuses_restart_without_authoritative_state(self):
        process, directory = self.start_worker('restart_exclusive', 'lost-state')
        self.wait_for((directory / 'ready').exists)
        self.restart_daemon(lose_state=True)
        _, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, -signal.SIGTERM, stderr)
        self.assertIn('without retained daemon state', (self.directory / 'daemon.log').read_text() + stderr)
        self.assertFalse(any(job['state'] == 'running' for job in self.status()))

    def test_native_runtime_limit_records_reason_and_runs_checkpoint(self):
        started = time.monotonic()
        process, checkpoint = self.run_worker('runtime_limit')
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertGreaterEqual(time.monotonic() - started, 1)
        self.assertIn('GPU runtime limit exceeded', json.loads(checkpoint.read_text())['reason'])
        self.assertTrue(any(event['event'] == 'terminal_failure' and
                            'GPU runtime limit exceeded' in str(event) for event in self.events()))

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

    def test_a_failure_report_waits_for_the_reporting_process_to_exit(self):
        process, _ = self.run_worker('failure_before_teardown')
        self.assertEqual(process.returncode, 1, process.stderr)
        events = self.events()
        self.assertTrue(any('reported before teardown' in str(event) for event in events))
        self.assertFalse(any(event['event'] == 'unclean_release' for event in events))

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
