"""Real PyO3 calls into the disposable Rust compiler daemon, without a GPU."""
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
BINS = Path(os.environ.get('GPU_QUEUE_TEST_BIN_DIR', ROOT / 'target/debug'))
EXTENSION = Path(os.environ.get('GPU_QUEUE_TEST_EXTENSION', BINS / 'libgpu_arbiter.so'))


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

    def job(self, name, code, **kwargs):
        return self.api.CompileJob(name, [sys.executable, '-S', '-c', code], str(self.directory), **kwargs)

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

    def test_claims_and_submissions_carry_their_priority(self):
        with self.api.HostMemoryClaim(memory_bytes=8 << 20, state_dir=str(self.state), priority=10):
            claim, = self.queue.health()['host_memory']['claims']
            self.assertEqual(claim['priority'], 10)
        self.queue.submit(self.api.CompileGroup('urgent', [self.job('quick', 'pass')]), priority=-10)
        self.queue.wait('urgent')
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
from gpu_queue import HostMemoryClaim
claim = HostMemoryClaim(memory_bytes=8388608, state_dir=sys.argv[1])
claim.acquire()
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
from gpu_queue import HostMemoryClaim
claim = HostMemoryClaim(memory_bytes=8388608, state_dir=sys.argv[1])
claim.acquire()
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
        obj = self.api.CompileJob('object', [compiler, '-c', 'example.c', '-o', 'example.o'], str(self.directory))
        link = self.api.CompileJob('link', [compiler, 'example.o', '-o', 'example'], str(self.directory), dependencies=['object'])
        declaration = self.api.CompileGroup('real-compiler', [obj, link])
        self.assertEqual(self.queue.submit(declaration), 'real-compiler')
        self.assertEqual(self.queue.submit(declaration), 'real-compiler')
        status = self.queue.wait('real-compiler')
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
            return self.queue.wait(name).successful
        with ThreadPoolExecutor(max_workers=32) as threads:
            self.assertTrue(all(threads.map(submit, range(128))))
        self.assertFalse(tuple(self.state.glob('*.lock')))

    def test_failure_keeps_exact_exit_code_and_failed_dependencies(self):
        failed = self.job('failed', 'raise SystemExit(7)')
        dependent = self.job('dependent', 'raise AssertionError("must not run")', dependencies=['failed'])
        self.queue.submit(self.api.CompileGroup('failures', [failed, dependent]))
        with self.assertRaises(self.api.CompilationFailed) as caught:
            self.queue.wait('failures')
        status = caught.exception.status
        self.assertTrue(status.complete)
        self.assertFalse(status.successful)
        self.assertEqual(status.jobs[0].returncode, 7)
        self.assertEqual(status.jobs[1].detail, 'compile prerequisite failed')
        self.assertIsNone(status.jobs[1].returncode)
        self.assertEqual(self.queue.wait('failures', check=False).jobs[0].outcome, 'fail')

    def test_wait_releases_gil_and_stays_pending_until_completion(self):
        self.queue.submit(self.api.CompileGroup('waiting', [self.job('slow', 'import time;time.sleep(.6)')]))
        seen = threading.Event()
        worker = threading.Thread(target=lambda: (time.sleep(.05), seen.set()))
        worker.start()
        status = self.queue.wait('waiting')
        worker.join(timeout=2)
        self.assertTrue(seen.is_set())
        self.assertTrue(status.successful)
        self.assertEqual(status.jobs[0].job_id, 'slow')

    def test_signal_interrupts_a_parked_wait_and_keeps_the_job(self):
        self.queue.submit(self.api.CompileGroup('interrupted', [self.job('slow', 'import time;time.sleep(3)')]))
        owner = self.run_owner("""
from gpu_queue import CompilerQueue
print('waiting', flush=True)
try:
    CompilerQueue(state_dir=sys.argv[1]).wait('interrupted')
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
        self.assertTrue(self.queue.wait('interrupted').successful)

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
        waiter = threading.Thread(target=lambda: results.append(self.queue.wait('parked')))
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
        status = self.queue.wait('cancel', check=False)
        self.assertEqual(status.jobs[0].outcome, 'cancelled')
        self.assertEqual(status.jobs[0].detail, 'owner cancelled incomplete compilation')
        self.assertNotEqual(status.jobs[0].returncode, 0)

    def test_native_declarations_reject_cycles_before_publication(self):
        first = self.job('first', 'pass', dependencies=['second'])
        second = self.job('second', 'pass', dependencies=['first'])
        with self.assertRaisesRegex(ValueError, 'cyclic'):
            self.api.CompileGroup('cycle', [first, second])
        with self.assertRaises(ValueError):
            self.api.CompileJob('bad', [], str(self.directory))

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
                results.append(self.queue.wait('replacement'))
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



if __name__ == '__main__':
    unittest.main()
