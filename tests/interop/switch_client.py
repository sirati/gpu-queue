"""One lease through ml-project's tools/_gpu_switch, unchanged.

usage: switch_client.py TREE MODE VRAM_BYTES LABEL PRIORITY

usable() is asked about ml-project's default lock: it only compares that path,
it never opens it. Prints `none` when gpu-switch is not the queue here,
`granted` once granted; releases on a stdin line.
"""
import sys

tree, mode, vram, label, priority = sys.argv[1:6]
sys.path.insert(0, tree)

from tools import _gpu_switch  # noqa: E402

if not _gpu_switch.usable(_gpu_switch.DEFAULT_LOCK):
    print('none', flush=True)
    sys.exit(0)
grant = _gpu_switch.lease(_gpu_switch.Ask(mode, int(vram), label, int(priority), label, False, False))
print('granted', flush=True)
sys.stdin.readline()
grant.release()
print('released', flush=True)
