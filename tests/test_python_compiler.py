"""Real PyO3 calls into the disposable Rust compiler daemon, without a GPU."""
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import os
from pathlib import Path
import shutil
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
        deadline = time.monotonic() + 10
        while True:
            try:
                self.queue.health()
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

    def test_actual_compiler_chain_has_typed_results_and_execution_times(self):
        compiler = shutil.which('cc')
        self.assertIsNotNone(compiler, 'the package test environment must provide a C compiler')
        (self.directory / 'example.c').write_text('#include <stdio.h>\nint main(void){puts("42");return 0;}\n')
        obj = self.api.CompileJob('object', [compiler, '-c', 'example.c', '-o', 'example.o'], str(self.directory))
        link = self.api.CompileJob('link', [compiler, 'example.o', '-o', 'example'], str(self.directory), dependencies=['object'])
        declaration = self.api.CompileGroup('real-compiler', [obj, link])
        self.assertEqual(self.queue.submit(declaration), 'real-compiler')
        self.assertEqual(self.queue.submit(declaration), 'real-compiler')
        status = self.queue.wait('real-compiler', timeout=15)
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
            return self.queue.wait(name, timeout=10).successful
        with ThreadPoolExecutor(max_workers=32) as threads:
            self.assertTrue(all(threads.map(submit, range(128))))
        self.assertFalse(tuple(self.state.glob('*.lock')))

    def test_failure_keeps_exact_exit_code_and_failed_dependencies(self):
        failed = self.job('failed', 'raise SystemExit(7)')
        dependent = self.job('dependent', 'raise AssertionError("must not run")', dependencies=['failed'])
        self.queue.submit(self.api.CompileGroup('failures', [failed, dependent]))
        with self.assertRaises(self.api.CompilationFailed) as caught:
            self.queue.wait('failures', timeout=10)
        status = caught.exception.status
        self.assertTrue(status.complete)
        self.assertFalse(status.successful)
        self.assertEqual(status.jobs[0].returncode, 7)
        self.assertEqual(status.jobs[1].detail, 'compile prerequisite failed')
        self.assertIsNone(status.jobs[1].returncode)
        self.assertEqual(self.queue.wait('failures', check=False).jobs[0].outcome, 'fail')

    def test_wait_releases_gil_and_timeout_leaves_the_same_job_in_custody(self):
        self.queue.submit(self.api.CompileGroup('waiting', [self.job('slow', 'import time;time.sleep(.6)')]))
        seen = threading.Event()
        worker = threading.Thread(target=lambda: (time.sleep(.05), seen.set()))
        worker.start()
        with self.assertRaises(TimeoutError):
            self.queue.wait('waiting', timeout=.2)
        worker.join(timeout=2)
        self.assertTrue(seen.is_set())
        status = self.queue.wait('waiting', timeout=10)
        self.assertTrue(status.successful)
        self.assertEqual(status.jobs[0].job_id, 'slow')

    def test_host_context_restores_affinity_when_body_raises(self):
        original = os.sched_getaffinity(0)
        reservation = self.queue.host(memory_bytes=1, cpus=1, timeout=10)
        with self.assertRaisesRegex(ValueError, 'body failed'):
            with reservation as active:
                self.assertEqual(set(active.cpus), os.sched_getaffinity(0))
                self.assertEqual(len(active.cpus), 1)
                raise ValueError('body failed')
        self.assertEqual(os.sched_getaffinity(0), original)
        self.assertFalse(tuple((self.state / 'host-reservations').glob('*.json')))
        with self.assertRaisesRegex(RuntimeError, 'already been released'):
            with reservation:
                self.fail('single-use reservation was reused')

    def test_explicit_running_cancellation_keeps_exact_reason(self):
        ready = self.directory / 'ready'
        self.queue.submit(self.api.CompileGroup('cancel', [self.job('running',
            f'import time\nfrom pathlib import Path\nPath({str(ready)!r}).touch()\ntime.sleep(60)')]))
        deadline = time.monotonic() + 10
        while not ready.exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.02)
        self.queue.cancel('cancel', 'owner cancelled incomplete compilation', include_running=True)
        status = self.queue.wait('cancel', timeout=10, check=False)
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

    def test_native_pipeline_runs_direct_continuations_after_own_prerequisites(self):
        marker = self.directory / 'compiled'
        compile_job = self.job('compile', f'from pathlib import Path;Path({str(marker)!r}).touch()')
        refused = self.job('refused', 'raise SystemExit(7)')
        group = self.api.CompileGroup('pipeline', [compile_job, refused])
        ready = self.job('native-ready', f'from pathlib import Path;assert Path({str(marker)!r}).exists()', dependencies=['compile'])
        blocked = self.job('native-blocked', 'raise AssertionError("must not run")', dependencies=['refused'])
        plan = self.api.Pipeline(group, [ready, blocked], max_running=1)
        self.assertEqual(self.queue.submit_pipeline(plan), 'pipeline')
        self.assertEqual(self.queue.submit_pipeline(plan), 'pipeline')
        result = self.queue.wait_pipeline('pipeline', timeout=15, check=False)
        self.assertEqual([j.outcome for j in result.jobs], ['success', 'fail'])
        self.assertIn('prerequisite failed', result.jobs[1].detail)
        roundtrip = self.api.Pipeline.from_json(plan.to_json())
        self.assertEqual(roundtrip.to_json(), plan.to_json())
        from copy import deepcopy
        self.assertEqual(deepcopy(result.jobs[0]).to_json(), result.jobs[0].to_json())
        self.assertEqual(self.api.CompileResult.from_json(result.jobs[0].to_json()).returncode, 0)


if __name__ == '__main__':
    unittest.main()
