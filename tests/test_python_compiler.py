"""Real PyO3 calls into the disposable Rust compiler daemon, without a GPU."""
from concurrent.futures import ThreadPoolExecutor
import asyncio
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
BINS = Path(os.environ.get('GPU_QUEUE_TEST_BIN_DIR', ROOT / 'target/debug'))
EXTENSION = Path(os.environ.get('GPU_QUEUE_TEST_EXTENSION', BINS / 'libgpu_arbiter.so'))
# A client extension of compile service generation 16 (gpu-queue d179805), for the compatibility check.
LEGACY_EXTENSION = os.environ.get('GPU_QUEUE_LEGACY_EXTENSION', '')
# The bin directory a package build compiled into the extension (python-package.nix); a development build has none.
PACKAGE_BIN = os.environ.get('GPU_QUEUE_TEST_PACKAGE_BIN', '')
# The outage gates wait out the compile service's patience (PLAN-I §8); run them with GPU_QUEUE_OUTAGE_GATES=1.
OUTAGE_GATES = os.environ.get('GPU_QUEUE_OUTAGE_GATES', '') == '1'
# The client's own bounds, and the scheduling tolerance of their timing assertions on a loaded host.
AUTHORITY_PATIENCE, PROBE_INTERVAL, CLEANUP_PATIENCE, TOLERANCE = 60, 5, 10, 3
BOOTSTRAP = ("import importlib.util,sys; spec=importlib.util.spec_from_file_location('gpu_queue._native',sys.argv[2]); "
             "native=importlib.util.module_from_spec(spec); spec.loader.exec_module(native); "
             "sys.modules['gpu_queue._native']=native;\n")
# The health of a current compile service, and of one of an earlier generation, which names no capability.
CAPABLE_HEALTH = {'protocol': 1, 'host_memory': {'protocol': 1}, 'capabilities': ['transfer', 'purpose']}
EARLIER_HEALTH = {'protocol': 1, 'host_memory': {'protocol': 1}}


def answered(connection, value):
    """Answers the request on `connection` with `value`, as the compile service does."""
    connection.sendall(json.dumps({'ok': True, 'value': value}).encode() + b'\n')


class PythonCompilerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('gpu_queue._native', EXTENSION)
        native = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(native)
        sys.modules['gpu_queue._native'] = native
        sys.path.insert(0, str(ROOT / 'python'))
        import gpu_queue
        cls.api = gpu_queue

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='pyo3-compiler-')
        self.directory = Path(self.temporary.name)
        self.state = self.directory / 'state'
        self.state.mkdir()
        self.log = (self.directory / 'daemon.log').open('wb')
        self.daemon = subprocess.Popen([str(BINS / 'compile-queue'), '--state-dir', str(self.state),
            '--minimum-ram', '0', '--average-ram', '1048576'], stdout=self.log, stderr=self.log, start_new_session=True)
        self.queue = self.api.CompilerQueue(state_dir=str(self.state))
        probe = self.api.CompilerQueue(state_dir=str(self.state), request_timeout=2)
        deadline = time.monotonic() + 10
        while True:
            try:
                probe.health()
                break
            except RuntimeError:
                if self.daemon.poll() is not None or time.monotonic() > deadline:
                    self.fail((self.directory / 'daemon.log').read_text())
                time.sleep(.02)

    def tearDown(self):
        self.daemon.terminate()
        self.daemon.wait(timeout=10)
        self.log.close()
        self.temporary.cleanup()

    def token(self):
        """A token nothing cancels, as a synchronous caller passes."""
        return self.api.CancelToken()

    def job(self, name, code, **kwargs):
        return self.api.CompileJob(name, [sys.executable, '-S', '-c', code], str(self.directory),
                                   **{'expected_runtime_secs': 60, **kwargs})

    def test_host_memory_claim_has_no_compile_job_or_affinity_side_effect(self):
        before_affinity = os.sched_getaffinity(0)
        with self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state),
                                      label='both-source fixtures') as claim:
            self.assertEqual(claim.memory_bytes, 8 << 20)
            health = self.queue.health()
            self.assertEqual(health['running'], 0)
            self.assertEqual(health['subqueues'], [])
            self.assertEqual(health['host_memory']['granted'], 1)
            self.assertEqual(health['host_memory']['claimed_bytes'], 8 << 20)
            self.assertEqual(os.sched_getaffinity(0), before_affinity)
        self.assertEqual(self.queue.health()['host_memory']['granted'], 0)

    def test_compiles_declare_their_runtime_and_results_report_the_contract(self):
        with self.assertRaises(TypeError):
            self.api.CompileJob('timeless', [sys.executable, '-S', '-c', 'pass'], str(self.directory))
        with self.assertWarnsRegex(UserWarning, 'very low priority'):
            self.queue.submit(self.api.CompileGroup('large', [self.job('large', 'pass', memory_bytes=11_000_000_000)]))
        self.queue.cancel('large', 'the warning was the point')
        self.queue.submit(self.api.CompileGroup('quick', [self.job('quick', 'pass')]))
        result, = self.queue.wait('quick', self.token()).jobs
        self.assertEqual(result.expected_runtime_secs, 60)
        self.assertTrue(any('less than half its declared 60 s' in warning for warning in result.warnings),
                        result.warnings)
        self.assertEqual(result.to_json()['warnings'], result.warnings)

    def test_claims_and_submissions_carry_their_priority(self):
        with self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state), priority=10):
            claim, = self.queue.health()['host_memory']['claims']
            self.assertEqual(claim['priority'], 10)
        self.queue.submit(self.api.CompileGroup('urgent', [self.job('quick', 'pass')]), priority=-10)
        self.queue.wait('urgent', self.token())
        with self.assertRaisesRegex(ValueError, 'between -10 and 10'):
            self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state), priority=11)
        with self.assertRaisesRegex(ValueError, 'between -10 and 10'):
            self.queue.submit(self.api.CompileGroup('invalid', [self.job('never', 'pass')]), priority=11)

    def test_host_memory_refuses_release_until_registered_worker_joins(self):
        with self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state)) as claim:
            worker = subprocess.Popen([sys.executable, '-S', '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE)
            try:
                claim.track_worker(worker.pid)
                with self.assertRaisesRegex(RuntimeError, 'join'):
                    claim.release()
            finally:
                worker.communicate(timeout=5)
        self.assertEqual(self.queue.health()['host_memory']['granted'], 0)

    def test_host_memory_rejects_impossible_requests_before_preparation(self):
        capacity = self.capacity()
        with self.assertRaisesRegex(RuntimeError, 'capacity'):
            with self.api.HostMemoryClaim(memory_bytes=capacity + 1, state_dir=str(self.state)):
                self.fail('impossible RAM claim reached CPU preparation')
        self.assertEqual(self.queue.health()['host_memory']['granted'], 0)

    def test_host_memory_crash_releases_the_original_claim(self):
        script = """
import os, sys
from gpu_queue import CancelToken, HostMemoryClaim
claim = HostMemoryClaim(memory_bytes=8388608, state_dir=sys.argv[1])
claim.acquire(CancelToken())
print(claim.claim_id, flush=True)
sys.stdin.read()
os._exit(0)
"""
        environment = {**os.environ, 'PYTHONPATH': str(ROOT / 'python') + os.pathsep + os.environ.get('PYTHONPATH', '')}
        # The child loads the same extension without modifying installed packages.
        bootstrap = "import importlib.util,sys; spec=importlib.util.spec_from_file_location('gpu_queue._native',sys.argv[2]); native=importlib.util.module_from_spec(spec); spec.loader.exec_module(native); sys.modules['gpu_queue._native']=native;\n"
        child = subprocess.Popen([sys.executable, '-c', bootstrap + script, str(self.state), str(EXTENSION)],
                                 env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.assertTrue(child.stdout.readline().strip())
            self.assertEqual(self.queue.health()['host_memory']['granted'], 1)
            child.communicate(timeout=5)
            deadline = time.monotonic() + 5
            while self.queue.health()['host_memory']['granted']:
                if time.monotonic() > deadline:
                    self.fail('crashed claim owner retained RAM admission')
                time.sleep(.02)
        finally:
            if child.poll() is None:
                child.kill(); child.communicate(timeout=5)

    def capacity(self):
        self.eventually(lambda: self.queue.health()['resources'])
        return self.queue.health()['resources']['memory_capacity_bytes']

    def run_owner(self, script, *arguments):
        bootstrap = "import importlib.util,sys; spec=importlib.util.spec_from_file_location('gpu_queue._native',sys.argv[2]); native=importlib.util.module_from_spec(spec); spec.loader.exec_module(native); sys.modules['gpu_queue._native']=native;\n"
        environment = {**os.environ, 'PYTHONPATH': str(ROOT / 'python') + os.pathsep + os.environ.get('PYTHONPATH', '')}
        return subprocess.Popen([sys.executable, '-c', bootstrap + script, str(self.state), str(EXTENSION), *arguments],
                                env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_host_memory_owner_crash_keeps_living_registered_worker_charged(self):
        script = """
import os, subprocess, sys
from gpu_queue import CancelToken, HostMemoryClaim
claim = HostMemoryClaim(memory_bytes=8388608, state_dir=sys.argv[1])
claim.acquire(CancelToken())
worker = subprocess.Popen([sys.executable, '-S', '-c', 'import time; time.sleep(60)'], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
claim.track_worker(worker.pid)
print(worker.pid, flush=True)
sys.stdin.read()
os._exit(0)
"""
        bootstrap = "import importlib.util,sys; spec=importlib.util.spec_from_file_location('gpu_queue._native',sys.argv[2]); native=importlib.util.module_from_spec(spec); spec.loader.exec_module(native); sys.modules['gpu_queue._native']=native;\n"
        environment = {**os.environ, 'PYTHONPATH': str(ROOT / 'python') + os.pathsep + os.environ.get('PYTHONPATH', '')}
        owner = subprocess.Popen([sys.executable, '-c', bootstrap + script, str(self.state), str(EXTENSION)], env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        worker = 0
        try:
            worker = int(owner.stdout.readline())
            owner.communicate(timeout=5)
            time.sleep(.3)
            self.assertEqual(self.queue.health()['host_memory']['granted'], 1)
            os.kill(worker, signal.SIGTERM)
            deadline = time.monotonic() + 5
            while self.queue.health()['host_memory']['granted']:
                if time.monotonic() > deadline:
                    self.fail('terminated tracked worker retained RAM admission')
                time.sleep(.02)
        finally:
            if owner.poll() is None:
                owner.kill(); owner.communicate(timeout=5)
            if worker:
                try: os.kill(worker, signal.SIGKILL)
                except ProcessLookupError: pass

    def test_actual_compiler_chain_has_typed_results_and_execution_times(self):
        compiler = shutil.which('cc')
        self.assertIsNotNone(compiler, 'the package test environment must provide a C compiler')
        (self.directory / 'example.c').write_text('#include <stdio.h>\nint main(void){puts("42");return 0;}\n')
        obj = self.api.CompileJob('object', [compiler, '-c', 'example.c', '-o', 'example.o'], str(self.directory),
                                  expected_runtime_secs=60)
        link = self.api.CompileJob('link', [compiler, 'example.o', '-o', 'example'], str(self.directory),
                                   expected_runtime_secs=60, dependencies=['object'])
        declaration = self.api.CompileGroup('real-compiler', [obj, link])
        self.assertEqual(self.queue.submit(declaration), 'real-compiler')
        self.assertEqual(self.queue.submit(declaration), 'real-compiler')
        status = self.queue.wait('real-compiler', self.token())
        self.assertIsInstance(status, self.api.CompileStatus)
        self.assertTrue(status.successful)
        for result in status.jobs:
            self.assertIsInstance(result, self.api.CompileResult)
            self.assertEqual(result.returncode, 0)
            self.assertGreater(result.execution_seconds, 0)
            self.assertLessEqual(result.queued_at, result.started_at)
            self.assertLessEqual(result.started_at, result.finished_at)
            self.assertTrue(Path(result.log_path).exists())
        self.assertEqual(subprocess.check_output([str(self.directory / 'example')], text=True).strip(), '42')
        self.assertEqual(status.jobs[1].job_id, 'link')
        self.assertGreaterEqual(status.jobs[1].started_at, status.jobs[0].finished_at)
        self.assertFalse((BINS / 'compile-worker').exists(), 'obsolete worker binary must be removed from the release')

    def test_frozen_queue_accepts_concurrent_native_submissions(self):
        def submit(index):
            name = 'native-' + str(index)
            declaration = self.api.CompileGroup(name, [])
            self.queue.submit(declaration)
            return self.queue.wait(name, self.token()).successful
        with ThreadPoolExecutor(max_workers=32) as threads:
            self.assertTrue(all(threads.map(submit, range(128))))
        self.assertFalse(tuple(self.state.glob('*.lock')))

    def test_failure_keeps_exact_exit_code_and_failed_dependencies(self):
        failed = self.job('failed', 'raise SystemExit(7)')
        dependent = self.job('dependent', 'raise AssertionError("must not run")', dependencies=['failed'])
        self.queue.submit(self.api.CompileGroup('failures', [failed, dependent]))
        with self.assertRaises(self.api.CompilationFailed) as caught:
            self.queue.wait('failures', self.token())
        status = caught.exception.status
        self.assertTrue(status.complete)
        self.assertFalse(status.successful)
        self.assertEqual(status.jobs[0].returncode, 7)
        self.assertEqual(status.jobs[1].detail, 'compile prerequisite failed')
        self.assertIsNone(status.jobs[1].returncode)
        self.assertEqual(self.queue.wait('failures', self.token(), check=False).jobs[0].outcome, 'fail')

    def test_wait_releases_gil_and_stays_pending_until_completion(self):
        self.queue.submit(self.api.CompileGroup('waiting', [self.job('slow', 'import time;time.sleep(.6)')]))
        seen = threading.Event()
        worker = threading.Thread(target=lambda: (time.sleep(.05), seen.set()))
        worker.start()
        status = self.queue.wait('waiting', self.token())
        worker.join(timeout=2)
        self.assertTrue(seen.is_set())
        self.assertTrue(status.successful)
        self.assertEqual(status.jobs[0].job_id, 'slow')

    def test_signal_interrupts_a_parked_wait_and_keeps_the_job(self):
        self.queue.submit(self.api.CompileGroup('interrupted', [self.job('slow', 'import time;time.sleep(3)')]))
        owner = self.run_owner("""
from gpu_queue import CancelToken, CompilerQueue
print('waiting', flush=True)
try:
    CompilerQueue(state_dir=sys.argv[1]).wait('interrupted', CancelToken())
except KeyboardInterrupt:
    print('interrupted', flush=True)
""")
        try:
            self.assertEqual(owner.stdout.readline().strip(), 'waiting')
            time.sleep(.3)
            owner.send_signal(signal.SIGINT)
            output, _ = owner.communicate(timeout=5)
            self.assertEqual(output.strip(), 'interrupted')
        finally:
            if owner.poll() is None:
                owner.kill(); owner.communicate(timeout=5)
        self.assertTrue(self.queue.wait('interrupted', self.token()).successful)

    def eventually(self, predicate):
        deadline = time.monotonic() + 10
        while not predicate():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.02)

    def test_group_wait_is_parked_on_the_daemon_until_completion(self):
        release = self.directory / 'release'
        self.queue.submit(self.api.CompileGroup('parked', [self.job('held',
            f'import time\nfrom pathlib import Path\nwhile not Path({str(release)!r}).exists(): time.sleep(.01)')]))
        results = []
        waiter = threading.Thread(target=lambda: results.append(self.queue.wait('parked', self.token())))
        waiter.start()
        self.eventually(lambda: self.queue.health()['parked_group_waits'] == 1)
        release.touch()
        waiter.join(timeout=15)
        self.assertTrue(results[0].successful)
        self.assertEqual(self.queue.health()['parked_group_waits'], 0)

    def test_pending_host_memory_claim_stays_parked_until_its_owner_is_interrupted(self):
        owner = self.run_owner("""
from gpu_queue import HostMemoryClaim
try:
    with HostMemoryClaim(memory_bytes=int(sys.argv[3]), state_dir=sys.argv[1]):
        print('granted', flush=True)
except KeyboardInterrupt:
    print('interrupted', flush=True)
""", str(self.capacity()))
        try:
            self.eventually(lambda: self.queue.health()['host_memory']['parked_waits'] == 1)
            time.sleep(1)
            self.assertIsNone(owner.poll(), 'a pending claim must not expire')
            owner.send_signal(signal.SIGINT)
            output, _ = owner.communicate(timeout=10)
            self.assertEqual(output.strip(), 'interrupted')
        finally:
            if owner.poll() is None:
                owner.kill(); owner.communicate(timeout=5)
        self.eventually(lambda: self.queue.health()['host_memory']['parked_waits'] == 0)
        self.assertEqual(self.queue.health()['host_memory']['pending'], 0)
        self.assertEqual(self.queue.health()['host_memory']['granted'], 0)

    def test_explicit_running_cancellation_keeps_exact_reason(self):
        ready = self.directory / 'ready'
        self.queue.submit(self.api.CompileGroup('cancel', [self.job('running',
            f'import time\nfrom pathlib import Path\nPath({str(ready)!r}).touch()\ntime.sleep(60)')]))
        deadline = time.monotonic() + 10
        while not ready.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.02)
        self.queue.cancel('cancel', 'owner cancelled incomplete compilation', include_running=True)
        status = self.queue.wait('cancel', self.token(), check=False)
        self.assertEqual(status.jobs[0].outcome, 'cancelled')
        self.assertEqual(status.jobs[0].detail, 'owner cancelled incomplete compilation')
        self.assertNotEqual(status.jobs[0].returncode, 0)

    def test_native_declarations_reject_cycles_before_publication(self):
        first = self.job('first', 'pass', dependencies=['second'])
        second = self.job('second', 'pass', dependencies=['first'])
        with self.assertRaisesRegex(ValueError, 'cyclic'):
            self.api.CompileGroup('cycle', [first, second])
        with self.assertRaises(ValueError):
            self.api.CompileJob('bad', [], str(self.directory), expected_runtime_secs=60)

    def test_wait_keeps_the_same_ids_across_graceful_service_replacement(self):
        ready = self.directory / 'ready'
        first = self.job('first', f'import time\nfrom pathlib import Path\nPath({str(ready)!r}).touch()\ntime.sleep(.8)')
        second = self.job('second', 'pass')
        self.queue.submit(self.api.CompileGroup('replacement',[first,second],max_running=1))
        deadline = time.monotonic()+10
        while not ready.exists():
            self.assertLess(time.monotonic(),deadline)
            time.sleep(.02)
        results, errors = [], []
        def wait():
            try:
                results.append(self.queue.wait('replacement', self.token()))
            except BaseException as error:
                errors.append(error)
        waiter = threading.Thread(target=wait)
        waiter.start()
        arguments = self.daemon.args
        self.daemon.terminate();self.daemon.wait(timeout=5)
        time.sleep(.1)
        self.daemon = subprocess.Popen(arguments,stdout=self.log,stderr=self.log,start_new_session=True)
        waiter.join(timeout=15)
        self.assertFalse(waiter.is_alive())
        self.assertFalse(errors,errors)
        self.assertEqual([j.job_id for j in results[0].jobs],['first','second'])
        self.assertTrue(results[0].successful)


    def python_environment(self):
        return {**os.environ, 'PYTHONPATH': str(ROOT / 'python') + os.pathsep + os.environ.get('PYTHONPATH', '')}

    def front_end(self, purpose, script, *arguments):
        """`host-claim` for 8 MiB, executing a Python program that loads this checkout's extension."""
        return subprocess.Popen([str(BINS / 'host-claim'), '--bytes', str(8 << 20), purpose, '--label', 'front-end',
                                 '--state-dir', str(self.state), '--', sys.executable, '-c', BOOTSTRAP + script,
                                 str(self.state), str(EXTENSION), *arguments],
                                env=self.python_environment(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)

    def claims(self):
        return self.queue.health()['host_memory']['claims']

    def test_a_front_end_hands_its_claim_to_the_program_which_gives_it_to_its_child(self):
        child = """
import json, sys
from gpu_queue import HostMemoryClaim
sys.stdin.readline()
claim = HostMemoryClaim.inherited()
print(json.dumps(claim.release()), flush=True)
"""
        program = """
import json, os, subprocess, sys
from gpu_queue import HostMemoryClaim
claim = HostMemoryClaim.inherited()
child = subprocess.Popen([sys.executable, '-c', sys.argv[3], sys.argv[1], sys.argv[2]],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
claim.transfer(child.pid)
released, _ = child.communicate('go\\n', timeout=30)
print(json.dumps({'owner': os.getpid(), 'child': child.pid, 'code': child.returncode,
                  'released': json.loads(released)}), flush=True)
"""
        front_end = self.front_end('--host-only', program, BOOTSTRAP + child)
        output, errors = front_end.communicate(timeout=60)
        self.assertEqual(front_end.returncode, 0, errors)
        self.assertNotIn('identity', errors)
        report = json.loads(output)
        self.assertEqual(report['owner'], front_end.pid, 'the program keeps the front-end process')
        self.assertEqual(report['code'], 0)
        self.assertTrue(report['released']['released'])
        self.assertIn('observed_high_water_bytes', report['released'])
        self.assertEqual(self.claims(), [], 'no claim is left')

    def test_a_transferred_claim_refuses_its_previous_owner(self):
        program = """
import subprocess, sys
from gpu_queue import AuthorityUnavailable, HostMemoryClaim
claim = HostMemoryClaim.inherited()
child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE)
claim.transfer(child.pid)
try:
    HostMemoryClaim.inherited()
except AuthorityUnavailable:
    print('unavailable', flush=True)
except RuntimeError as error:
    print('refused:', error, flush=True)
child.communicate(timeout=10)
"""
        front_end = self.front_end('--host-only', program)
        output, errors = front_end.communicate(timeout=60)
        self.assertEqual(front_end.returncode, 0, errors)
        self.assertTrue(output.startswith('refused:'), output + errors)
        self.assertIn('transferred to', output)

    def test_front_end_claims_show_their_purpose_and_python_claims_show_none(self):
        for flag, purpose in [('--gpu-place', 'gpu_place'), ('--host-only', 'host_only')]:
            front_end = self.front_end(flag, 'import sys\nsys.stdin.readline()')
            try:
                self.eventually(lambda: [c['state'] for c in self.claims()] == ['granted'])
                claim, = self.claims()
                self.assertEqual(claim['purpose'], purpose)
                self.assertEqual(claim['owner_pid'], front_end.pid)
            finally:
                front_end.communicate('\n', timeout=30)
            self.assertEqual(front_end.returncode, 0)
            self.eventually(lambda: self.claims() == [])
        with self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state)):
            claim, = self.claims()
            self.assertEqual(claim['purpose'], None)
        self.assertEqual(self.api.LARGE_REQUEST, 10_000_000_000)

    def test_a_package_names_the_executables_of_its_own_build(self):
        named = {name: os.fspath(path) for name, path in self.api.EXECUTABLES.items()}
        built = {name: str(Path(PACKAGE_BIN) / name) for name in ('gpu-run', 'host-claim', 'gpu-switch')}
        self.assertEqual(named, built if PACKAGE_BIN else {})
        self.assertTrue(all(os.access(path, os.X_OK) for path in named.values()), named)

    def test_inheriting_without_a_front_end_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'HOST_MEMORY_CLAIM'):
            self.api.HostMemoryClaim.inherited()

    def test_a_group_result_sink_receives_its_results_before_the_wait_is_answered(self):
        sink = self.directory / 'compile-results'
        sink.mkdir()
        group = self.api.CompileGroup('sunk', [self.job('sunk', 'pass')], result_sink=str(sink))
        self.assertEqual(str(group.result_sink), str(sink))
        self.assertEqual(str(self.api.CompileGroup.from_json(group.to_json()).result_sink), str(sink))
        self.assertEqual(self.api.CompileGroup('plain', [self.job('plain', 'pass')]).result_sink, None)
        self.queue.submit(group)
        status = self.queue.wait('sunk', self.token())
        self.assertEqual(status.result_sink_error, None)
        published = json.loads((sink / 'sunk.results.json').read_text())
        self.assertEqual(published['jobs'][0]['outcome'], 'success')
        self.assertEqual(published['jobs'][0]['job_id'], 'sunk')
        with self.assertRaisesRegex(ValueError, 'absolute'):
            self.api.CompileGroup('relative', [self.job('relative', 'pass')], result_sink='compile-results')

    def owner(self, script, *arguments):
        """A Python program that loads this checkout's extension, with its standard streams piped."""
        return subprocess.Popen([sys.executable, '-c', BOOTSTRAP + script, str(self.state), str(EXTENSION), *arguments],
                                env=self.python_environment(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)

    def held_group(self, name):
        """A group whose job runs until the returned file exists."""
        release = self.directory / (name + '.release')
        self.queue.submit(self.api.CompileGroup(name, [self.job(name, (
            f'import time\nfrom pathlib import Path\nwhile not Path({str(release)!r}).exists(): time.sleep(.01)'))]))
        return release

    def parked_waits(self):
        health = self.queue.health()
        return health['parked_group_waits'], health['host_memory']['parked_waits']

    def assert_unavailable(self, error, operation):
        self.assertIs(type(error), self.api.AuthorityUnavailable)
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual((error.source, error.operation), ('compile service', operation))

    def test_constructors_without_a_state_directory_raise_authority_unavailable(self):
        missing = str(self.directory / 'never-made')
        for construct in (lambda: self.api.CompilerQueue(state_dir=missing),
                          lambda: self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=missing)):
            with self.assertRaises(self.api.AuthorityUnavailable) as caught:
                construct()
            self.assert_unavailable(caught.exception, 'state_dir')
        self.assertEqual((self.api.AuthorityUnavailable('x').source, self.api.AuthorityUnavailable('x').operation),
                         (None, None))

    def test_refusals_and_local_validation_keep_their_types(self):
        refusals = [lambda: self.queue.wait('unknown-group', self.token()),
                    lambda: self.queue.cancel('unknown-group', ' '),
                    lambda: self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state)).track_worker(1)]
        for refused in refusals:
            with self.assertRaises(RuntimeError) as caught:
                refused()
            self.assertNotIsInstance(caught.exception, self.api.AuthorityUnavailable)
        with self.assertRaises(ValueError):
            self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state), priority=11)
        with self.assertRaises(TypeError):
            self.queue.wait('unknown-group')

    def endpoint(self, state, respond):
        """A stand-in compile service at `state`. It answers each connection in a thread of its own with
        `respond(request, connection)`, and records every request it reads, in order, in the returned list. A
        client that leaves before its answer is no error. Close the returned socket to end it."""
        digest = hashlib.sha256(os.fsencode(os.path.realpath(state))).hexdigest()[:24]
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(f'\0rust-compile-{os.getuid()}-{digest}')
        listener.listen()
        received = []
        def answer(connection):
            with connection, contextlib.suppress(ConnectionError):
                line = connection.makefile('rb').readline()
                if line:
                    request = json.loads(line)
                    received.append(request)
                    respond(request, connection)
        def accept():
            while True:
                try:
                    connection, _ = listener.accept()
                except OSError:
                    return
                threading.Thread(target=answer, args=(connection,), daemon=True).start()
        threading.Thread(target=accept, daemon=True).start()
        return listener, received

    def lacking(self, state):
        """A compile service of an earlier generation at `state`: it answers every request with a health that
        names no capability. Close the returned socket to end it."""
        listener, _ = self.endpoint(state, lambda request, connection: answered(connection, EARLIER_HEALTH))
        return listener

    def stand_in(self, respond):
        """A stand-in compile service (`endpoint`) in a state directory of its own, ended with the test: that
        directory and the requests it received."""
        state = self.directory / 'stand-in'
        state.mkdir()
        listener, received = self.endpoint(state, respond)
        self.addCleanup(listener.close)
        return str(state), received

    def test_a_missing_capability_is_an_outage_as_before(self):
        claim = self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state))
        claim.acquire(self.token())
        self.daemon.kill()
        self.daemon.wait(timeout=10)
        lacking = self.lacking(self.state)
        child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE)
        try:
            with self.assertRaisesRegex(self.api.AuthorityUnavailable, 'lacks the transfer capability') as caught:
                claim.transfer(child.pid)
            self.assert_unavailable(caught.exception, 'health')
        finally:
            child.communicate(timeout=5)
            claim.release()
            lacking.close()

    def test_a_front_end_hands_its_outage_to_the_binding_reader(self):
        state = self.directory / 'lacking'
        state.mkdir()
        lacking = self.lacking(state)
        recorded = self.directory / 'attempt.outage.json'
        self.assertIsNone(self.api.FrontEndOutage.read(str(recorded)))
        environment = {name: value for name, value in os.environ.items() if name != 'GPU_QUEUE_PLACE'}
        try:
            front_end = subprocess.run([str(BINS / 'host-claim'), '--bytes', str(8 << 20), '--host-only',
                                        '--state-dir', str(state), '--outage-file', str(recorded), '--', 'true'],
                                       env=environment, capture_output=True, text=True, timeout=60)
        finally:
            lacking.close()
        self.assertEqual(front_end.returncode, 75, front_end.stderr)
        outage = self.api.FrontEndOutage.read(recorded)
        self.assertEqual((outage.source, outage.operation), ('compile service', 'health'))
        self.assertIn('lacks the transfer capability', outage.reason)
        self.assertIn(f'host-claim: {outage.reason}', front_end.stderr)
        recorded.write_text('{"source":')
        with self.assertRaises(ValueError):
            self.api.FrontEndOutage.read(recorded)

    def test_a_token_from_another_thread_ends_a_parked_wait_and_the_group_remains(self):
        release = self.held_group('kept')
        token = self.api.CancelToken()
        with ThreadPoolExecutor(max_workers=1) as threads:
            waiting = threads.submit(self.queue.wait, 'kept', token)
            self.eventually(lambda: self.parked_waits()[0] == 1)
            token.cancel()
            token.cancel()
            cancelled = time.monotonic()
            with self.assertRaises(asyncio.CancelledError):
                waiting.result(timeout=10)
            self.assertLess(time.monotonic() - cancelled, 1)
        self.assertTrue(token.cancelled)
        self.eventually(lambda: self.parked_waits()[0] == 0)
        self.assertFalse(self.queue.status('kept').complete, 'a local cancellation withdraws nothing')
        release.touch()
        self.assertTrue(self.queue.wait('kept', self.token()).successful, 'a later wait reattaches')

    def test_a_token_cancelled_first_parks_nothing_and_tokens_are_independent(self):
        release = self.held_group('independent')
        first = self.api.CancelToken()
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            self.queue.wait('independent', first)
        self.assertEqual(self.parked_waits(), (0, 0))
        cancelled, kept = self.api.CancelToken(), self.api.CancelToken()
        with ThreadPoolExecutor(max_workers=2) as threads:
            ended = threads.submit(self.queue.wait, 'independent', cancelled)
            waited = threads.submit(self.queue.wait, 'independent', kept)
            self.eventually(lambda: self.parked_waits()[0] == 2)
            cancelled.cancel()
            with self.assertRaises(asyncio.CancelledError):
                ended.result(timeout=10)
            self.assertFalse(kept.cancelled)
            release.touch()
            self.assertTrue(waited.result(timeout=30).successful)

    def test_a_token_racing_an_answer_gives_the_answer_or_a_local_cancellation(self):
        self.queue.submit(self.api.CompileGroup('answered', []))
        for _ in range(20):
            token = self.api.CancelToken()
            racer = threading.Thread(target=token.cancel)
            racer.start()
            try:
                self.assertTrue(self.queue.wait('answered', token).complete)
            except asyncio.CancelledError:
                pass
            racer.join()

    def test_a_cancelled_acquisition_raises_cancelled_error_and_releases_its_claim(self):
        claim = self.api.HostMemoryClaim(memory_bytes=self.capacity(), state_dir=str(self.state), label='cancelled')
        token = self.api.CancelToken()
        with ThreadPoolExecutor(max_workers=1) as threads:
            acquiring = threads.submit(claim.acquire, token)
            self.eventually(lambda: self.parked_waits()[1] == 1)
            token.cancel()
            with self.assertRaises(asyncio.CancelledError):
                acquiring.result(timeout=20)
        self.assertEqual(self.claims(), [], 'the failed acquisition released its claim')

    def test_a_token_cancelled_before_acquisition_starts_sends_nothing(self):
        state, received = self.stand_in(lambda request, connection: answered(connection, CAPABLE_HEALTH))
        token = self.api.CancelToken()
        token.cancel()
        claim = self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=state)
        started = time.monotonic()
        with self.assertRaises(asyncio.CancelledError):
            claim.acquire(token)
        del claim
        self.assertLess(time.monotonic() - started, 1)
        self.assertEqual(received, [], 'neither the acquisition nor its cleanup sent anything')

    def test_cancelling_an_acquisition_while_its_probe_is_pending_releases_its_claim_at_once(self):
        parked, probing, ended = threading.Event(), threading.Event(), threading.Event()
        self.addCleanup(ended.set)
        healths = []
        def health(connection):
            """The capability check is answered; the parked wait's probe stays pending."""
            healths.append(connection)
            if len(healths) == 1:
                return answered(connection, CAPABLE_HEALTH)
            probing.set()
            ended.wait()
        replies = {'health': health,
                   'host_memory_register': lambda connection: answered(connection, {'state': 'pending', 'warnings': []}),
                   'host_memory_wait': lambda connection: (parked.set(), ended.wait()),
                   'host_memory_release': lambda connection: answered(connection, {'released': True})}
        state, received = self.stand_in(lambda request, connection: replies[request['op']](connection))
        token = self.api.CancelToken()
        claim = self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=state)
        with ThreadPoolExecutor(max_workers=1) as threads:
            acquiring = threads.submit(claim.acquire, token)
            self.assertTrue(parked.wait(timeout=10) and probing.wait(timeout=10), received)
            token.cancel()
            cancelled = time.monotonic()
            with self.assertRaises(asyncio.CancelledError):
                acquiring.result(timeout=10)
            self.assertLess(time.monotonic() - cancelled, 1)
        del claim
        operations = [request['op'] for request in received]
        self.assertEqual(sorted(operations), sorted(['host_memory_register', 'health', 'host_memory_wait', 'health',
                                                     'host_memory_release']), 'no cancel, no abandon')
        self.assertEqual(operations[-1], 'host_memory_release', 'the failed acquisition released its claim last')

    def test_cancellation_racing_a_grant_ends_granted_or_cancelled_with_one_release(self):
        racing = []
        def grant(connection):
            """Grants the claim, its acquisition's token cancelled just before the grant in odd rounds and just
            after it in every round."""
            token = racing[-1]
            if len(racing) % 2:
                token.cancel()
            answered(connection, {'state': 'granted'})
            token.cancel()
        def respond(request, connection):
            if request['op'] == 'host_memory_wait':
                return grant(connection)
            answered(connection, CAPABLE_HEALTH)
        state, received = self.stand_in(respond)
        outcomes = []
        for _ in range(20):
            token = self.api.CancelToken()
            racing.append(token)
            claim = self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=state)
            started = time.monotonic()
            try:
                claim.acquire(token)
                claim.release()
                outcomes.append('granted')
            except asyncio.CancelledError:
                outcomes.append('cancelled')
            self.assertLess(time.monotonic() - started, 5, outcomes)
            claim_id = claim.claim_id
            del claim
            operations = [request['op'] for request in received if request.get('claim_id') == claim_id]
            self.assertEqual(operations, ['host_memory_register', 'host_memory_wait', 'host_memory_release'], outcomes)
        self.assertIn('cancelled', outcomes)
        self.assertNotIn('cancel', [request['op'] for request in received])

    @unittest.skipUnless(OUTAGE_GATES, 'outage gate 1: about 70 s')
    def test_every_rpc_of_both_bindings_raises_authority_unavailable_without_a_service(self):
        """Each RPC-bearing method with valid inputs; local getters and unattempted cleanup send nothing."""
        granted = """
import json, subprocess, sys
from gpu_queue import AuthorityUnavailable, CancelToken, HostMemoryClaim
claim = HostMemoryClaim(memory_bytes=8388608, state_dir=sys.argv[1])
claim.acquire(CancelToken())
child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.read()'], stdin=subprocess.PIPE)
print('ready', flush=True)
sys.stdin.readline()
calls = {'transfer': lambda: claim.transfer(child.pid), 'track_worker': lambda: claim.track_worker(child.pid),
         'release': claim.release, 'exit': lambda: claim.__exit__(None, None, None)}
try:
    calls[sys.argv[3]]()
    print(json.dumps(None), flush=True)
except AuthorityUnavailable as error:
    print(json.dumps([type(error).__name__, isinstance(error, RuntimeError), error.source, error.operation]), flush=True)
child.communicate()
"""
        delegated = {'transfer': 'health', 'track_worker': 'host_memory_track', 'release': 'host_memory_release',
                     'exit': 'host_memory_release'}
        owners = {method: self.owner(granted, method) for method in delegated}
        for owner in owners.values():
            self.assertEqual(owner.stdout.readline().strip(), 'ready')
        pending = self.api.HostMemoryClaim(memory_bytes=self.capacity(), state_dir=str(self.state))
        waiting = ThreadPoolExecutor(max_workers=1).submit(pending.acquire, self.token())
        self.eventually(lambda: self.parked_waits()[1] == 1)
        self.daemon.kill()
        self.daemon.wait(timeout=10)
        handed = {**self.python_environment(), 'HOST_MEMORY_CLAIM': json.dumps(
            {'claim_id': 'absent', 'memory_bytes': 8 << 20, 'state_dir': str(self.state)})}
        inherited = subprocess.Popen([sys.executable, '-c', BOOTSTRAP + """
import json
from gpu_queue import AuthorityUnavailable, HostMemoryClaim
try:
    HostMemoryClaim.inherited()
except AuthorityUnavailable as error:
    print(json.dumps([type(error).__name__, isinstance(error, RuntimeError), error.source, error.operation]))
""", str(self.state), str(EXTENSION)], env=handed, stdout=subprocess.PIPE, text=True)
        started = time.monotonic()
        for owner in owners.values():
            owner.stdin.write('\n')
            owner.stdin.flush()
        group = self.api.CompileGroup('absent', [self.job('absent', 'pass')])
        def entered():
            with self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state)):
                pass
        local = {'submit': (lambda: self.queue.submit(group), 'submit_group'),
                 'status': (lambda: self.queue.status('absent'), 'group_status'),
                 'wait': (lambda: self.queue.wait('absent', self.token()), 'group_wait'),
                 'cancel': (lambda: self.queue.cancel('absent', 'absent service'), 'cancel'),
                 'health': (self.queue.health, 'health'),
                 'acquire': (lambda: self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state))
                             .acquire(self.token()), 'host_memory_register'),
                 'enter': (entered, 'host_memory_register')}
        with ThreadPoolExecutor(max_workers=len(local)) as threads:
            calls = {name: threads.submit(call) for name, (call, _) in local.items()}
            for name, (_, operation) in local.items():
                with self.subTest(method=name), self.assertRaises(self.api.AuthorityUnavailable) as caught:
                    calls[name].result(timeout=AUTHORITY_PATIENCE + PROBE_INTERVAL + TOLERANCE + 30)
                self.assert_unavailable(caught.exception, operation)
        with self.assertRaises(self.api.AuthorityUnavailable) as caught:
            waiting.result(timeout=AUTHORITY_PATIENCE + PROBE_INTERVAL + CLEANUP_PATIENCE + TOLERANCE)
        self.assert_unavailable(caught.exception, 'host_memory_wait')
        for method, operation in delegated.items():
            with self.subTest(method=method):
                output, errors = owners[method].communicate(timeout=AUTHORITY_PATIENCE + TOLERANCE + 30)
                self.assertEqual(json.loads(output), ['AuthorityUnavailable', True, 'compile service', operation],
                                 errors)
        output, _ = inherited.communicate(timeout=AUTHORITY_PATIENCE + TOLERANCE + 30)
        self.assertEqual(json.loads(output), ['AuthorityUnavailable', True, 'compile service', 'host_memory_status'])
        self.assertLess(time.monotonic() - started, AUTHORITY_PATIENCE + PROBE_INTERVAL + TOLERANCE + 15)

    @unittest.skipUnless(OUTAGE_GATES, 'outage gate 7: about 10 s')
    def test_a_failed_release_then_destruction_keep_the_programs_status_within_the_cleanup_deadline(self):
        owner = self.owner("""
import time
from gpu_queue import AuthorityUnavailable, CancelToken, HostMemoryClaim
claim = HostMemoryClaim(memory_bytes=8388608, state_dir=sys.argv[1])
claim.acquire(CancelToken())
print('granted', flush=True)
sys.stdin.readline()
started = time.monotonic()
try:
    claim.release()
except AuthorityUnavailable as error:
    print('unavailable', time.monotonic() - started, flush=True)
del claim
print('destroyed', time.monotonic() - started, flush=True)
sys.exit(3)
""")
        try:
            self.assertEqual(owner.stdout.readline().strip(), 'granted')
            self.daemon.kill()
            self.daemon.wait(timeout=10)
            started = time.monotonic()
            output, errors = owner.communicate('\n', timeout=60)
            exited = time.monotonic() - started
        finally:
            if owner.poll() is None:
                owner.kill(); owner.communicate(timeout=5)
        self.assertEqual(owner.returncode, 3, errors)
        released, destroyed = (float(line.split()[1]) for line in output.splitlines())
        self.assertTrue(output.startswith('unavailable'), output)
        self.assertGreaterEqual(released, CLEANUP_PATIENCE - .5)
        self.assertLessEqual(destroyed, CLEANUP_PATIENCE + TOLERANCE)
        self.assertLessEqual(exited, CLEANUP_PATIENCE + TOLERANCE)
        self.assertEqual(errors.count('remains bound to its process lifetime'), 1, errors)

    @unittest.skipUnless(LEGACY_EXTENSION, 'requires a generation-16 client extension')
    def test_a_generation_16_client_acquires_and_releases_against_generation_17(self):
        script = """
claim = native.HostMemoryClaim(memory_bytes=8388608, state_dir=sys.argv[1], label='gen 16')
claim.acquire()
print('granted', flush=True)
sys.stdin.readline()
claim.release()
print('released', flush=True)
"""
        owner = subprocess.Popen([sys.executable, '-c', BOOTSTRAP + script, str(self.state), LEGACY_EXTENSION],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(owner.stdout.readline().strip(), 'granted')
            claim, = self.claims()
            self.assertEqual(claim['label'], 'gen 16')
            self.assertEqual(claim['purpose'], None, 'a gen-16 client declares no purpose')
            output, errors = owner.communicate('\n', timeout=30)
            self.assertEqual(output.strip(), 'released', errors)
            self.assertEqual(owner.returncode, 0, errors)
        finally:
            if owner.poll() == None:
                owner.kill(); owner.communicate(timeout=5)
        self.assertEqual(self.claims(), [])


if __name__ == '__main__':
    unittest.main()
