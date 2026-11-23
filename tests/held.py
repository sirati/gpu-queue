"""A command that runs until its test releases it: the one held-command owner of the Python suites.

A held command has no lifetime of its own. It publishes its pid, waits for its release file, and only then runs what
follows its pause, so a check that ran after a timed pause still decides the command's result."""
from pathlib import Path
import sys

#: This module's directory, which a held program puts first on its path to import `published`.
HERE = Path(__file__).resolve().parent


class Held:
    """The command `name` in `directory`: it publishes its pid as `<name>.started` and runs until `<name>.release`
    exists."""

    def __init__(self, directory, name):
        self.started = Path(directory) / f'{name}.started'
        self.released = Path(directory) / f'{name}.release'

    def program(self, prologue='', epilogue=''):
        """A `sys.executable -S -c` program: `prologue`; its pid published whole as the started marker; a wait,
        checked every 20 ms, until the release file exists; then `epilogue`."""
        return [sys.executable, '-S', '-c', self._code(prologue, epilogue)]

    def executable(self, path, prologue='', epilogue=''):
        """The same program as an executable file at `path`, for a caller that runs a program by its path; returns
        the path as a string."""
        path = Path(path)
        path.write_text(f'#!{sys.executable} -S\n' + self._code(prologue, epilogue))
        path.chmod(0o700)
        return str(path)

    def _code(self, prologue, epilogue):
        return (f'import sys\nsys.path.insert(0, {str(HERE)!r})\nfrom published import publish\n'
                'import os, time\nfrom pathlib import Path\n'
                f'{prologue}\n'
                f'publish(Path({str(self.started)!r}), str(os.getpid()))\n'
                f'while not Path({str(self.released)!r}).exists():\n'
                '    time.sleep(0.02)\n'
                f'{epilogue}\n')

    def pid(self):
        """The pid the command published, read after the test waited for its started marker."""
        return int(self.started.read_text())

    def release(self):
        """Creates the release file: its existence is the event."""
        self.released.touch()
