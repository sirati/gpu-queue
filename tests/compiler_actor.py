"""CPU-only tests of the actual Rust daemon and its wire protocol."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from built import BINS
import daemon_log
from held import Held
import waits

#: One day, in seconds: every declared runtime and configured patience of this suite. Twice it exceeds the containment
#: limit of every gate that runs the suite, so none of them can end a step before the containment ends the run.
ONE_DAY = 86400

#: The options of every daemon this suite starts, except the one test of the preflight's own bound.
PATIENT = ['--preflight-patience-secs', str(ONE_DAY)]

#: What a compile job of this suite checks once it is released: the actor hides CUDA and pins one CPU.
CHECKS = 'assert os.environ["CUDA_VISIBLE_DEVICES"] == ""\nassert len(os.sched_getaffinity(0)) == 1'

#: A whole line in which a predecessor refuses a hand-over it accepted and keeps its authority.
REFUSAL = re.compile(
    r'^compiler handover (?:refused; original authority retained|activation refused): (?P<reason>.*)\n', re.M)


def request(state, operation, **fields):
    address = '\0rust-compile-' + str(os.getuid()) + '-' + hashlib.sha256(os.fsencode(state.resolve())).hexdigest()[:24]
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.connect(address)
        connection.sendall(json.dumps({'protocol': 1, 'op': operation, **fields}).encode() + b'\n')
        answer = json.loads(connection.makefile('rb').readline())
        if not answer['ok']:
            raise RuntimeError(answer['error'])
        return answer['value']


def job(name, argv, cwd):
    return {'job_id': name, 'argv': list(argv), 'cwd': str(cwd), 'expected_runtime_secs': ONE_DAY}


def group(name, jobs=(), max_running=0):
    return {'group_id': name, 'jobs': list(jobs), 'max_running': max_running}


class ActorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rust-compiler-actor-')
        self.directory = Path(self.temporary.name)
        self.state = self.directory / 'state'
        self.state.mkdir()
        self.log_path = daemon_log.path(self.id())
        self.log = self.log_path.open('wb')
        self.daemon = None
        self.holds = []
        self.start(PATIENT)

    def start(self, options):
        self.daemon = subprocess.Popen([str(BINS / 'compile-queue'), '--state-dir', str(self.state),
            '--minimum-ram', '0', '--average-ram', '1048576', '--request-deadline-secs', str(ONE_DAY), *options],
            stdout=self.log, stderr=self.log, start_new_session=True)
        self.wait(lambda: self.health())

    def health(self):
        try:
            return request(self.state, 'health')
        except (ConnectionError, FileNotFoundError, json.JSONDecodeError):
            return None

    def wait(self, predicate):
        def condition():
            # Observed before the predicate, so a daemon exiting in between is
            # seen by the predicate first: waiting for that exit cannot fail on it.
            gone = self.daemon.poll() is not None and not self.health()
            result = predicate()
            if not result and gone:
                self.fail(self.log_path.read_text())
            return result
        return waits.until('the actor condition', condition)

    def handover(self, predecessor):
        """Whether the accepted hand-over from `predecessor` was acknowledged (R-g). It waits for the first of its two
        outcomes: health names a pid other than `predecessor`'s, or this test's log, which every daemon of its chain
        writes, holds a whole refusal line. A refusal must be the successor's readiness timing out, and `predecessor`
        must still be the authority."""
        def outcome():
            health = self.health()
            refusal = REFUSAL.search(self.log_path.read_text())
            acknowledged = bool(health) and health['pid'] != predecessor
            return (acknowledged or refusal) and {'acknowledged': acknowledged, 'refusal': refusal}
        seen = self.wait(outcome)
        if seen['refusal']:
            self.assertEqual(seen['refusal']['reason'], 'successor readiness timed out')
            self.assertEqual(self.health()['pid'], predecessor)
        return seen['acknowledged']

    def tearDown(self):
        for held in self.holds:
            held.release()
        health=self.health()
        if health and health['pid'] != self.daemon.pid:
            os.kill(health['pid'],signal.SIGTERM)
        if self.daemon.poll() is None:
            self.daemon.terminate()
        try:
            self.daemon.wait()
        finally:
            self.log.close()
            self.temporary.cleanup()

    def hold(self, name):
        """A held command `name` in this test's directory, which tearDown releases at the latest."""
        held = Held(self.directory, name)
        self.holds.append(held)
        return held

    def held_command(self, name, declared, prologue=''):
        """The job `name`, with the further fields `declared`, and its `Held`: it runs `prologue`, publishes its
        started marker and makes its checks once the test releases it."""
        held = self.hold(name)
        return {**job(name, held.program(prologue, CHECKS), self.directory), **declared}, held

    def command(self, name, declared):
        """The job `name`, with the further fields `declared`, released before it is declared: it needs no lifetime."""
        declaration, held = self.held_command(name, declared)
        held.release()
        return declaration

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
        self.daemon.wait()
        self.start(PATIENT)
        self.assertNotEqual(old, self.daemon.pid)
        self.assertIn('direct-compilation', self.health()['features'])
        self.assertFalse(tuple(self.state.glob('*.lock')))

    def test_graceful_replacement_drains_direct_compiler_and_preserves_queued_job(self):
        first_job, first = self.held_command('first', {})
        second_job, second = self.held_command('second', {})
        self.submit(group('drain', (first_job, second_job), max_running=1))
        record = self.state / 'running/first.json'
        self.wait(first.started.exists)
        # The child can write its marker before the actor durably publishes its
        # post-spawn PID. Wait for that publication, not merely for child startup.
        retained = self.wait(lambda: value if (value := json.loads(record.read_text()))['pid'] is not None else None)
        self.assertEqual(first.pid(), retained['pid'])
        self.daemon.terminate()
        self.wait(lambda: (health := self.health()) and health['draining'] and health['pid'] == self.daemon.pid)
        first.release()
        self.daemon.wait()
        self.assertEqual(self.result('first')['outcome'], 'success')
        self.assertFalse(second.started.exists())
        self.assertIsNone(self.result('second'))
        self.start(PATIENT)
        self.wait(second.started.exists)
        second.release()
        self.assertEqual(self.wait(lambda: self.result('second'))['outcome'], 'success')

    def test_crash_retains_custody_and_never_invents_success_or_reexecutes(self):
        once_job, once = self.held_command('once', {})
        self.submit(group('crash', (once_job, self.command('after', {})), max_running=1))
        self.wait(once.started.exists)
        original_pid = once.pid()
        self.daemon.kill();self.daemon.wait();self.start(PATIENT)
        once.release()
        result = self.wait(lambda: self.result('once'))
        self.assertEqual(result['outcome'], 'lost')
        self.assertIsNone(result['returncode'])
        self.assertIn('exact signal/cause unknown', result['detail'])
        self.assertEqual(once.pid(), original_pid)
        self.assertEqual(self.wait(lambda: self.result('after'))['outcome'], 'success')

    def test_pending_cancellation_preserves_running_member_and_first_receipt(self):
        running_job, running = self.held_command('running', {})
        self.submit(group('cancel', (running_job, self.command('pending', {})), max_running=1))
        self.wait(running.started.exists)
        result = request(self.state, 'cancel', group_id='cancel', reason='owner withdrew pending work')
        self.assertEqual([v['status'] for v in result['members']], ['running', 'cancelled'])
        self.assertEqual(self.result('pending')['outcome'], 'cancelled')
        running.release()
        self.assertEqual(self.wait(lambda: self.result('running'))['outcome'], 'success')
        self.assertFalse((self.directory / 'pending.started').exists())
        original = (self.state / 'results/pending.json').read_bytes()
        request(self.state, 'cancel', group_id='cancel', reason='another reason')
        self.assertEqual(original, (self.state / 'results/pending.json').read_bytes())

    def test_running_cancellation_escalates_and_retains_reason(self):
        ignoring = ('import signal\n'
                    'signal.signal(signal.SIGINT,signal.SIG_IGN)\n'
                    'signal.signal(signal.SIGTERM,signal.SIG_IGN)')
        stubborn_job, stubborn = self.held_command('stubborn', {}, ignoring)
        self.submit(group('running-cancel', (stubborn_job,)))
        self.wait(stubborn.started.exists)
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
            'group_status', group_id='exact-exits'))['complete'] else None))
        self.assertEqual({item['returncode'] for item in status['jobs']}, {7})
        self.assertEqual({item['outcome'] for item in status['jobs']}, {'fail'})

    def test_dependencies_and_nonzero_exit_are_terminal(self):
        failed = job('failed',(sys.executable,'-S','-c','raise SystemExit(7)'),self.directory)
        dependent = self.command('dependent',{'dependencies':['failed']})
        independent = self.command('independent',{})
        self.submit(group('chain',(failed,dependent,independent)))
        self.assertEqual(self.wait(lambda:self.result('failed'))['returncode'],7)
        self.assertEqual(self.wait(lambda:self.result('dependent'))['detail'],'compile prerequisite failed')
        self.assertEqual(self.wait(lambda:self.result('independent'))['outcome'],'success')
        self.assertFalse((self.directory / 'dependent.started').exists())

    def successor_image(self, name):
        path=self.directory/name
        shutil.copy2(BINS/'compile-queue',path)
        # An ELF permits trailing data. Distinct immutable image bytes exercise
        # the upgrade protocol while retaining this test daemon implementation.
        with path.open('ab') as output:output.write(name.encode())
        return str(path)

    def test_identical_successor_requests_never_replace_the_daemon(self):
        original=self.health()['pid']
        identical=self.directory/'identical-copy'
        shutil.copy2(BINS/'compile-queue',identical)
        for _ in range(4):
            self.assertTrue(request(self.state,'upgrade',executable=str(identical))['already_current'])
            self.assertEqual(self.health()['pid'],original)

    def test_separate_successors_admit_queued_work_before_original_children_exit(self):
        held_job, held = self.held_command('held', {})
        self.submit(group('handover', (held_job, self.command('queued', {})), max_running=1))
        self.wait(held.started.exists)
        original_pid=held.pid()
        for count in range(2):
            old=self.health()['pid']
            request(self.state,'upgrade',executable=self.successor_image('successor-'+str(count)))
            acknowledged=self.handover(old)
            self.assertTrue(Path('/proc',str(original_pid)).exists())
            name='during-handover-'+str(count)
            self.submit(group(name,(self.command(name,{}),)))
            self.assertEqual(self.wait(lambda:self.result(name))['outcome'],'success')
            if not acknowledged:
                break
        held.release()
        self.assertEqual(self.wait(lambda:self.result('held'))['returncode'],0)
        self.assertEqual(self.wait(lambda:self.result('queued'))['returncode'],0)
        self.assertEqual(held.pid(),original_pid)
        # The successor, not the original Popen, is now the authority.
        os.kill(self.health()['pid'],signal.SIGTERM)
        self.wait(lambda: self.health() is None)

    def test_failed_successor_keeps_original_listener_and_running_custody(self):
        held_job, held = self.held_command('held', {})
        self.submit(group('kept',(held_job,)))
        self.wait(held.started.exists)
        original=self.health()['pid']
        bad=self.directory/'not-executable'
        bad.write_text('not a daemon')
        with self.assertRaises(RuntimeError):
            request(self.state,'upgrade',executable=str(bad))
        held.release()
        self.assertEqual(self.wait(lambda:self.result('held'))['returncode'],0)
        self.assertEqual(self.health()['pid'],original)
        self.submit(group('still-works',(self.command('after-failure',{}),)))
        self.assertEqual(self.wait(lambda:self.result('after-failure'))['returncode'],0)

    def test_live_handover_preserves_native_host_ram_claim(self):
        request(self.state, 'host_memory_acquire', claim_id='host-fixtures', memory_bytes=8388608, label='both-source batch')
        self.wait(lambda: self.health()['host_memory']['granted'] == 1)
        original = self.health()['pid']
        request(self.state, 'upgrade', executable=self.successor_image('host-memory-successor'))
        self.handover(original)
        claim = request(self.state, 'host_memory_status', claim_id='host-fixtures')
        self.assertEqual(claim['state'], 'granted')
        self.assertEqual(self.health()['host_memory']['claimed_bytes'], 8388608)
        self.assertEqual(self.health()['running'], 0)
        request(self.state, 'host_memory_release', claim_id='host-fixtures')
        self.assertEqual(self.health()['host_memory']['granted'], 0)

    def test_old_successor_cannot_downgrade_live_authority(self):
        held_job, held = self.held_command('held', {})
        self.submit(group('protocol-kept', (held_job,)))
        self.wait(held.started.exists)
        original = self.health()['pid']
        old = self.directory / 'old-successor'
        old.write_text('#!' + sys.executable + '\n' + """
import json, os, socket
channel = socket.socket(fileno=int(os.environ['COMPILE_QUEUE_HANDOFF_FD']))
channel.recvmsg(1, socket.CMSG_SPACE(4))
reader = channel.makefile('rb')
reader.readline()
channel.sendall(json.dumps({'ready': os.getpid(), 'handoff_protocol': 2}).encode() + b'\\n')
reply = reader.readline()
raise SystemExit(2 if reply else 0)
""")
        old.chmod(0o700)
        with self.assertRaisesRegex(RuntimeError, 'capabilities'):
            request(self.state, 'upgrade', executable=str(old))
        held.release()
        self.assertEqual(self.wait(lambda: self.result('held'))['returncode'], 0)
        self.assertEqual(self.health()['pid'], original)
        self.submit(group('protocol-after', (self.command('after-protocol', {}),)))
        self.assertEqual(self.wait(lambda: self.result('after-protocol'))['outcome'], 'success')

    def test_stalled_successor_preflight_never_blocks_queue_requests(self):
        slow = self.hold('slow-successor')
        executable = slow.executable(self.directory / 'slow-successor', epilogue='raise SystemExit(2)')
        original = self.health()['pid']
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(request, self.state, 'upgrade', executable=executable)
            self.wait(slow.started.exists)
            self.assertFalse(pending.done())
            self.assertEqual(self.health()['pid'], original)
            self.submit(group('during-preflight', (self.command('preflight-work', {}),)))
            self.assertEqual(self.wait(lambda: self.result('preflight-work'))['outcome'], 'success')
            slow.release()
            with self.assertRaisesRegex(RuntimeError, 'cannot advertise the required native handover capabilities'):
                pending.result()
        self.assertEqual(self.health()['pid'], original)

    def test_a_successor_that_never_answers_is_refused_when_its_preflight_patience_ends(self):
        self.daemon.terminate()
        self.daemon.wait()
        self.start(['--preflight-patience-secs', '1'])
        silent = self.hold('silent-successor').executable(self.directory / 'silent-successor')
        original = self.health()['pid']
        with self.assertRaisesRegex(RuntimeError, 'successor capability preflight timed out'):
            request(self.state, 'upgrade', executable=silent)
        self.assertEqual(self.health()['pid'], original)

    def test_successor_crash_retains_original_reaper_and_exact_exit(self):
        held_job, held = self.held_command('held', {})
        self.submit(group('reaper-crash',(held_job,self.command('queued',{})),max_running=1))
        self.wait(held.started.exists)
        original=self.health()['pid']
        request(self.state,'upgrade',executable=self.successor_image('successor-before-crash'))
        acknowledged=self.handover(original)
        original_reaper=self.daemon
        if acknowledged:
            os.kill(self.health()['pid'],signal.SIGKILL)
            self.wait(lambda:self.health() is None)
            self.start(PATIENT)
        held.release()
        result=self.wait(lambda:self.result('held'))
        self.assertEqual(result['outcome'],'success')
        self.assertEqual(result['returncode'],0)
        self.assertEqual(self.wait(lambda:self.result('queued'))['returncode'],0)
        if acknowledged:
            original_reaper.wait()

    def test_cycles_and_duplicate_identities_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'cyclic'):
            self.submit(group('cycle',(self.command('a',{'dependencies':['b']}),self.command('b',{'dependencies':['a']}))))
        command = self.command('unique',{})
        self.submit(group('first-group',(command,)))
        with self.assertRaisesRegex(RuntimeError,'duplicate'):
            self.submit(group('second-group',(command,)))

    def test_unrelated_state_is_rejected_without_import(self):
        self.daemon.terminate();self.daemon.wait()
        (self.state / 'format.json').write_text('{"format":"legacy","version":1}')
        self.daemon = subprocess.Popen([str(BINS / 'compile-queue'),'--state-dir',str(self.state)],stdout=self.log,stderr=self.log)
        self.assertNotEqual(self.daemon.wait(),0)
        self.assertIn('unsupported compiler state format',self.log_path.read_text())


if __name__ == '__main__':
    unittest.main()
