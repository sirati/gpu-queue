"""CPU-only tests of the actual Rust daemon and its wire protocol."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

BINS = Path(os.environ.get('GPU_QUEUE_TEST_BIN_DIR', Path(__file__).resolve().parents[1] / 'target/debug'))


def request(state, operation, **fields):
    address = '\0rust-compile-' + str(os.getuid()) + '-' + hashlib.sha256(os.fsencode(state.resolve())).hexdigest()[:24]
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(30)
        connection.connect(address)
        connection.sendall(json.dumps({'protocol': 1, 'op': operation, **fields}).encode() + b'\n')
        answer = json.loads(connection.makefile('rb').readline())
        if not answer['ok']:
            raise RuntimeError(answer['error'])
        return answer['value']


def job(name, argv, cwd, **fields):
    return {'job_id': name, 'argv': list(argv), 'cwd': str(cwd), **fields}


def group(name, jobs=(), max_running=0):
    return {'group_id': name, 'jobs': list(jobs), 'max_running': max_running}


class ActorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rust-compiler-actor-')
        self.directory = Path(self.temporary.name)
        self.state = self.directory / 'state'
        self.state.mkdir()
        self.log = (self.directory / 'daemon.log').open('ab')
        self.daemon = None
        self.start()

    def start(self):
        self.daemon = subprocess.Popen([str(BINS / 'compile-queue'), '--state-dir', str(self.state),
            '--minimum-ram', '0', '--average-ram', '1048576'], stdout=self.log, stderr=self.log, start_new_session=True)
        self.wait(lambda: self.health())

    def health(self):
        try:
            return request(self.state, 'health')
        except (ConnectionError, FileNotFoundError):
            return None

    def wait(self, predicate, seconds=20):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            if self.daemon.poll() is not None:
                self.fail((self.directory / 'daemon.log').read_text())
            time.sleep(.02)
        self.fail('condition timed out: ' + (self.directory / 'daemon.log').read_text())

    def tearDown(self):
        if self.daemon.poll() is None:
            self.daemon.terminate()
        try:
            self.daemon.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.daemon.kill()
            self.daemon.wait(timeout=5)
            raise
        finally:
            self.log.close()
            self.temporary.cleanup()

    def command(self, name, pause=0, **fields):
        code = ('import os,time\nfrom pathlib import Path\n'
            f'Path({str(self.directory / (name + ".started"))!r}).write_text(str(os.getpid()))\n'
            f'time.sleep({pause})\n'
            'assert os.environ["CUDA_VISIBLE_DEVICES"] == ""\n'
            'assert len(os.sched_getaffinity(0)) == 1\n')
        return job(name, (sys.executable, '-S', '-c', code), self.directory, **fields)

    def submit(self, declaration):
        return request(self.state, 'submit_group', group=declaration)

    def result(self, name):
        path = self.state / 'results' / (name + '.json')
        return json.loads(path.read_text()) if path.exists() else None

    def test_parallel_publication_is_idempotent_without_ownership_files(self):
        def publish(index):
            return self.submit(group('group-' + str(index)))
        with ThreadPoolExecutor(max_workers=64) as clients:
            results = list(clients.map(publish, range(256)))
        self.assertTrue(all(not row['existing'] for row in results))
        self.assertTrue(publish(0)['existing'])
        self.assertEqual(len(tuple((self.state / 'groups').glob('*.json'))), 256)
        self.assertFalse(tuple(self.state.glob('*.lock')))

    def test_kernel_endpoint_disappears_after_crash(self):
        old = self.daemon.pid
        self.daemon.kill()
        self.daemon.wait(timeout=5)
        self.start()
        self.assertNotEqual(old, self.daemon.pid)
        self.assertIn('direct-compilation', self.health()['features'])
        self.assertFalse(tuple(self.state.glob('*.lock')))

    def test_graceful_replacement_drains_direct_compiler_and_preserves_queued_job(self):
        self.submit(group('drain', (self.command('first', .8), self.command('second')), max_running=1))
        record = self.state / 'running/first.json'
        self.wait((self.directory / 'first.started').exists)
        retained = json.loads(record.read_text())
        self.assertEqual(int((self.directory / 'first.started').read_text()), retained['pid'])
        self.daemon.terminate()
        self.daemon.wait(timeout=5)
        self.assertEqual(self.result('first')['outcome'], 'success')
        self.assertIsNone(self.result('second'))
        self.start()
        self.assertEqual(self.wait(lambda: self.result('second'))['outcome'], 'success')

    def test_crash_retains_custody_and_never_invents_success_or_reexecutes(self):
        self.submit(group('crash', (self.command('once', .8), self.command('after')), max_running=1))
        self.wait((self.directory / 'once.started').exists)
        original_pid = int((self.directory / 'once.started').read_text())
        self.daemon.kill();self.daemon.wait(timeout=5);self.start()
        result = self.wait(lambda: self.result('once'))
        self.assertEqual(result['outcome'], 'lost')
        self.assertIsNone(result['returncode'])
        self.assertIn('exact signal/cause unknown', result['detail'])
        self.assertEqual(int((self.directory / 'once.started').read_text()), original_pid)
        self.assertEqual(self.wait(lambda: self.result('after'))['outcome'], 'success')

    def test_pending_cancellation_preserves_running_member_and_first_receipt(self):
        self.submit(group('cancel', (self.command('running', .8), self.command('pending')), max_running=1))
        self.wait((self.directory / 'running.started').exists)
        result = request(self.state, 'cancel', group_id='cancel', reason='owner withdrew pending work')
        self.assertEqual([v['status'] for v in result['members']], ['running', 'cancelled'])
        self.assertEqual(self.result('pending')['outcome'], 'cancelled')
        self.assertEqual(self.wait(lambda: self.result('running'))['outcome'], 'success')
        self.assertFalse((self.directory / 'pending.started').exists())
        original = (self.state / 'results/pending.json').read_bytes()
        request(self.state, 'cancel', group_id='cancel', reason='another reason')
        self.assertEqual(original, (self.state / 'results/pending.json').read_bytes())

    def test_running_cancellation_escalates_and_retains_reason(self):
        code = ('import signal,time\nfrom pathlib import Path\n'
                'signal.signal(signal.SIGINT,signal.SIG_IGN)\n'
                'signal.signal(signal.SIGTERM,signal.SIG_IGN)\n'
                f'Path({str(self.directory / "ready")!r}).touch()\ntime.sleep(60)')
        self.submit(group('running-cancel', (job('stubborn',(sys.executable,'-S','-c',code),self.directory),)))
        self.wait((self.directory / 'ready').exists)
        start = time.monotonic()
        request(self.state,'cancel',group_id='running-cancel',include_running=True,reason='owner cancelled')
        result = self.wait(lambda:self.result('stubborn'))
        self.assertEqual(result['outcome'],'cancelled')
        self.assertEqual(result['returncode'],-signal.SIGKILL)
        self.assertEqual(result['detail'],'owner cancelled')
        self.assertGreaterEqual(time.monotonic()-start,2)

    def test_concurrent_compiler_exits_keep_exact_status_without_restarting(self):
        commands = tuple(job(f'exit-{index}',
            (sys.executable, '-S', '-c',
             f'import time; time.sleep({.2 + (index % 7) * .01}); raise SystemExit(7)'),
            self.directory) for index in range(32))
        self.submit(group('exact-exits', commands))
        status = self.wait(lambda: (answer if (answer := request(self.state,
            'group_status', group_id='exact-exits'))['complete'] else None), seconds=60)
        self.assertEqual({item['returncode'] for item in status['jobs']}, {7})
        self.assertEqual({item['outcome'] for item in status['jobs']}, {'fail'})

    def test_dependencies_and_nonzero_exit_are_terminal(self):
        failed = job('failed',(sys.executable,'-S','-c','raise SystemExit(7)'),self.directory)
        dependent = self.command('dependent',dependencies=['failed'])
        independent = self.command('independent')
        self.submit(group('chain',(failed,dependent,independent)))
        self.assertEqual(self.wait(lambda:self.result('failed'))['returncode'],7)
        self.assertEqual(self.wait(lambda:self.result('dependent'))['detail'],'compile prerequisite failed')
        self.assertEqual(self.wait(lambda:self.result('independent'))['outcome'],'success')
        self.assertFalse((self.directory / 'dependent.started').exists())

    def test_cycles_and_duplicate_identities_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'cyclic'):
            self.submit(group('cycle',(self.command('a',dependencies=['b']),self.command('b',dependencies=['a']))))
        command = self.command('unique')
        self.submit(group('first-group',(command,)))
        with self.assertRaisesRegex(RuntimeError,'duplicate'):
            self.submit(group('second-group',(command,)))

    def test_unrelated_state_is_rejected_without_import(self):
        self.daemon.terminate();self.daemon.wait(timeout=5)
        (self.state / 'format.json').write_text('{"format":"legacy","version":1}')
        self.daemon = subprocess.Popen([str(BINS / 'compile-queue'),'--state-dir',str(self.state)],stdout=self.log,stderr=self.log)
        self.assertNotEqual(self.daemon.wait(timeout=5),0)
        self.assertIn('unsupported compiler state format',(self.directory / 'daemon.log').read_text())


if __name__ == '__main__':
    unittest.main()
