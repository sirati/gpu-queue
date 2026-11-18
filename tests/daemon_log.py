"""Where a test's daemon log is kept after the test: one file per test, named by the test's id."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Each test's compile-service log, its admission records among it, is kept here after the test.
LOGS = Path(os.environ.get('GPU_QUEUE_TEST_LOG_DIR', ROOT / 'target/daemon-logs'))


def path(test_id):
    """The kept log of the test `test_id`, its directory made."""
    LOGS.mkdir(parents=True, exist_ok=True)
    return LOGS / f'{test_id}.log'
