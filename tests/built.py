"""Where the build under test lives: its directories and files, each read from the environment once, here."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# The binaries under test; a package names its own bin directory.
BINS = Path(os.environ.get('GPU_QUEUE_TEST_BIN_DIR', ROOT / 'target/debug'))
# The compiled native Python extension.
EXTENSION = Path(os.environ.get('GPU_QUEUE_TEST_EXTENSION', BINS / 'libgpu_arbiter.so'))
# The mock NVML library `cargo test` builds beside the binaries; a package names its installed copy.
NVML = Path(os.environ.get('GPU_QUEUE_TEST_NVML', BINS / 'examples' / 'libnvml_mock.so'))
# The bin directory a package build compiled into the extension (python-package.nix); a development build has none.
PACKAGE_BIN = os.environ.get('GPU_QUEUE_TEST_PACKAGE_BIN', '')
# An installed older package's bin directory, for the backward-compatibility suite; empty when there is none.
LEGACY = os.environ.get('GPU_QUEUE_LEGACY_BIN_DIR', '')
