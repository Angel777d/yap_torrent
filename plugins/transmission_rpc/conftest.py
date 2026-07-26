"""Make the test suite runnable straight from the checkout.

The plugin and the core `yap_torrent` package are the same working tree, so the
tests import both directly from source — no `pip install` required. This adds the
core `src/`, the plugin `src/`, and the sibling `../py_core` (angelovich.core) to
`sys.path`. Each insert is skipped if the directory is absent (e.g. when the
packages are already installed), so it never shadows an installed copy harmfully.
"""
import sys
from pathlib import Path

_here = Path(__file__).resolve().parent      # plugins/transmission_rpc
_repo = _here.parents[1]                      # repo root

for _path in (_here / "src", _repo / "src", _repo.parent / "py_core"):
	_str = str(_path)
	if _path.is_dir() and _str not in sys.path:
		sys.path.insert(0, _str)
