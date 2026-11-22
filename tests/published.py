"""Publishing a file that another process reads: the one owner of atomic publication in the Python tests.

It imports only `os` and `pathlib`, so a program run with `python -S` can import it."""
import os
from pathlib import Path


def publish(path, text):
    """Writes `text` to `<file name>.tmp` in `path`'s directory and renames it onto `path` with `os.replace`. A rename
    within one directory is atomic, so a published file that exists is complete."""
    path = Path(path)
    staged = path.with_name(path.name + '.tmp')
    staged.write_text(text)
    os.replace(staged, path)
