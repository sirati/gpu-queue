"""Waiting for an observable condition, with no deadline: the one waiter of the Python suites.

A test waits on observable state or events and never bounds a wait: its one hang guard is the containment its run
executes in, which ends every process of the run at its limit. A condition that never holds hangs the run, and the
``waiting for <what>`` line this module writes names the wait that hung."""
import sys
import time
from typing import Callable, TypeVar

T = TypeVar('T')

#: Seconds between two polls of a condition: pacing, which bounds nothing.
_POLL = .02


def until(what: str, condition: Callable[[], T]) -> T:
    """``condition``'s first truthy value. It calls ``condition`` once; if the value is falsy, it writes
    ``waiting for <what>`` to standard error once and then calls ``condition`` again every 20 ms."""
    value = condition()
    if value:
        return value
    print(f'waiting for {what}', file=sys.stderr, flush=True)
    while not (value := condition()):
        time.sleep(_POLL)
    return value
