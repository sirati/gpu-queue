"""suites.py MODULE...: runs the named unittest modules, verbosely, with a stack dump of every thread on SIGTERM.

A suite's one hang guard is the containment its run executes in: at its limit every process of the run receives
SIGTERM. `-v` writes each test's name before the test runs, so a hung test is the last name with no outcome. Before it
loads the modules, this runner registers a dump of every thread's stack on SIGTERM (`faulthandler.register`) to the
run's standard error, which unittest does not capture without `-b`; the dump chains to the signal's default action,
which then ends the process. It bounds nothing and changes no outcome. The modules are named explicitly, so an absent
one fails with an ImportError, and a module that runs no test exits 5 ("NO TESTS RAN")."""
import faulthandler
import signal
import sys
import unittest

faulthandler.register(signal.SIGTERM, all_threads=True, chain=True)
unittest.main(module=None, argv=[sys.argv[0], '-v', *sys.argv[1:]])
