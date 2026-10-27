"""Run real historical clients against the new daemon, without a physical GPU.

Set GPU_QUEUE_TEST_BIN_DIR to the new binaries and GPU_QUEUE_LEGACY_BIN_DIR to
an installed older package, then run unittest discovery for this file.
The shared fixture supplies the mock GPU and disposable daemon only.
"""
import json
import os
from pathlib import Path
import socket
import selectors
import subprocess
import sys
import time
import unittest

import test_python_shutdown as fixture

LEGACY = os.environ.get('GPU_QUEUE_LEGACY_BIN_DIR', '')


@unittest.skipUnless(LEGACY, 'requires an actual historical GPU manager package')
class BackwardCompatibilityTests(unittest.TestCase):
    def setUp(self):
        fixture.PythonShutdownTests.setUp(self)
        self.old = Path(LEGACY)

    def tearDown(self):
        fixture.PythonShutdownTests.tearDown(self)

    def command(self, binary, *args):
        return subprocess.run([str(binary), *args],
            env=dict(self.environment, GPU_SWITCH_SOCKET=str(self.socket),
                     XDG_STATE_HOME=str(self.directory / 'client-state')),
            capture_output=True, text=True, timeout=15)

    def old_command(self, *args):
        result = self.command(self.old / 'gpu-run', *args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def status(self):
        return json.loads(self.old_command('status', '--json'))

    def job(self, ident):
        return next(job for job in self.status()['jobs'] if job['id'] == ident)

    def wait_for(self, predicate):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        self.fail('compatibility condition timed out: ' + (self.directory / 'daemon.log').read_text())

    def restart(self, executable):
        self.daemon.terminate()
        self.daemon.wait(timeout=5)
        self.daemon = subprocess.Popen([str(executable), '--config', str(self.directory / 'config.json')],
            env=self.environment, stdout=self.log, stderr=self.log)
        self.wait_for(lambda: self.socket.exists())

    def submit(self, mode, milliseconds=200, executable=None, estimate=()):
        binary = executable or self.old / 'gpu-run'
        result = self.command(binary, 'submit', *mode, *estimate, '--',
            str(fixture.BINS / 'ninfer-mock'), 'gpu-hold', '--mem', '1', '--sm', '50',
            '--ms', str(milliseconds))
        self.assertEqual(result.returncode, 0, result.stderr)
        return int(result.stdout.strip())

    def test_old_cli_submits_and_waits_without_estimate_in_both_modes(self):
        for mode in [('--exclusive',), ('--shared', '--vram', '1G')]:
            ident = self.submit(mode)
            self.old_command('status')
            self.status()
            self.old_command('wait', str(ident))
            self.assertEqual(self.job(ident)['state'], 'done')

    def test_old_status_reads_new_unknown_and_positive_estimates(self):
        for estimate, expected in [('-1', None), ('60', 60)]:
            ident = self.submit(('--exclusive',), executable=fixture.BINS / 'gpu-run',
                                estimate=('--expected-secs', estimate))
            self.old_command('status')
            result = self.command(fixture.BINS / 'gpu-run', 'status', '--json')
            self.assertEqual(result.returncode, 0, result.stderr)
            job = next(job for job in json.loads(result.stdout)['jobs'] if job['id'] == ident)
            self.assertEqual(job['expected_runtime_secs'], expected)
            self.old_command('wait', str(ident))

    def test_legacy_lease_request_without_new_fields_is_accepted(self):
        blocker = self.submit(('--exclusive',), milliseconds=5000)
        self.wait_for(lambda: self.job(blocker)['state'] == 'running')
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(5)
            connection.connect(str(self.socket))
            reader = connection.makefile()
            try:
                connection.sendall(b'{"op":"lease","mode":"exclusive","vram":0,"label":"old-client"}\n')
                reply = json.loads(reader.readline())
                self.assertTrue(reply['ok'], reply)
                self.assertEqual(reply['lease_protocol'], 2)
                self.assertIn('estimate omitted', reply['warning'])
                connection.sendall(b'{"op":"release"}\n')
            finally:
                reader.close()
        self.old_command('cancel', str(blocker))

    def test_old_running_and_queued_runners_reattach_after_daemon_upgrade(self):
        self.restart(self.old / 'gpu-switch')
        first = self.submit(('--exclusive',), milliseconds=10000)
        self.wait_for(lambda: self.job(first)['state'] == 'running' and bool(list(self.gpu.iterdir())))
        second = self.submit(('--exclusive',))
        self.assertEqual(self.job(second)['state'], 'queued')
        original = self.job(first)
        self.restart(fixture.BINS / 'gpu-switch')
        self.wait_for(lambda: (self.directory / 'daemon.log').read_text().count('resumed by pid') >= 2)
        resumed = self.job(first)
        self.assertEqual(resumed['state'], 'running')
        self.assertEqual(resumed['pid'], original['pid'])
        self.assertEqual(resumed['runner_pid'], original['runner_pid'])
        self.assertEqual(resumed['runs'], 1)
        self.assertEqual(self.job(second)['state'], 'queued')
        self.old_command('cancel', str(first))
        self.old_command('wait', str(second))
        self.assertEqual(self.job(second)['state'], 'done')

    @unittest.skipUnless(fixture.EXTENSION.exists(), 'requires the compiled native Python extension')
    def test_new_native_python_lease_reattaches_from_historical_daemon(self):
        self.restart(self.old / 'gpu-switch')
        directory = self.directory / 'native'
        directory.mkdir()
        process = subprocess.Popen([sys.executable, str(Path(fixture.__file__).resolve()),
            'worker', 'restart_exclusive', str(self.socket), str(directory / 'checkpoint.json'),
            str(directory)], env=self.environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.wait_for((directory / 'ready').exists)
            original = next(job for job in self.status()['jobs'] if job['pid'] == process.pid)
            self.restart(fixture.BINS / 'gpu-switch')
            self.wait_for(lambda: 'resumed by pid' in (self.directory / 'daemon.log').read_text())
            resumed = self.job(original['id'])
            self.assertEqual(resumed['pid'], process.pid)
            self.assertEqual(resumed['state'], 'running')
            self.assertEqual(resumed['runs'], 1)
            (directory / 'finish').touch()
            _, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertFalse((directory / 'checkpoint.json').exists())
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)

    @unittest.skipUnless(os.environ.get('GPU_QUEUE_LEGACY_TOOLS_ROOT'), 'requires the historical Python tools tree')
    def test_old_python_lease_client_reattaches_after_upgrade(self):
        self.restart(self.old / 'gpu-switch')
        driver = Path(__file__).parent / 'interop/switch_client.py'
        process = subprocess.Popen([sys.executable, str(driver), os.environ['GPU_QUEUE_LEGACY_TOOLS_ROOT'],
            'exclusive', '0', 'legacy-python', '0'],
            env=dict(self.environment, GPU_SWITCH_SOCKET=str(self.socket)),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with selectors.DefaultSelector() as ready:
                ready.register(process.stdout, selectors.EVENT_READ)
                self.assertTrue(ready.select(timeout=5), 'old Python client did not acquire')
            self.assertEqual(process.stdout.readline().strip(), 'granted')
            original = next(job for job in self.status()['jobs'] if job['label'] == 'legacy-python')
            self.restart(fixture.BINS / 'gpu-switch')
            self.wait_for(lambda: 'resumed by pid' in (self.directory / 'daemon.log').read_text())
            resumed = self.job(original['id'])
            self.assertEqual(resumed['state'], 'running')
            self.assertEqual(resumed['pid'], process.pid)
            stdout, stderr = process.communicate('\n', timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertIn('released', stdout)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)


if __name__ == '__main__':
    unittest.main()
