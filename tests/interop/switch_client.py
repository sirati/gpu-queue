"""One lease through ml-project's tools/_gpu_switch, unchanged.

usage: switch_client.py TREE MODE VRAM_BYTES LABEL PRIORITY

usable() is asked about ml-project's default lock: it only compares that path,
it never opens it. Prints `none` when gpu-switch is not the queue here,
`granted` once granted; releases on a stdin line.
"""
import sys
import os
from pathlib import Path

tree, mode, vram, label, priority = sys.argv[1:6]
sys.path.insert(0, tree)

from tools import _gpu_switch  # noqa: E402

if not _gpu_switch.usable(_gpu_switch.DEFAULT_LOCK):
    print('none', flush=True)
    sys.exit(0)
grant = _gpu_switch.lease(_gpu_switch.Ask(mode, int(vram), label, int(priority), label, False, False))
# Optional fake allocation for daemon-enforcement tests. The imported client
# stays unchanged; the driver represents an application actually using its grant.
claim = Path(os.environ['NINFER_MOCK_GPU_DIR']) / str(os.getpid()) if 'NINFER_MOCK_GPU_DIR' in os.environ else None
if claim:
    claim.write_text('mem=1 sm=50 type=C')
print('granted', flush=True)
try:
    sys.stdin.readline()
finally:
    if claim:
        claim.unlink(missing_ok=True)
    grant.release()
print('released', flush=True)
