"""Make the core test suite runnable straight from the checkout (no install).

Adds the core `src/` and the sibling `../py_core` (angelovich.core) to sys.path.
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent  # repo root

for _path in (_root / "src", _root.parent / "py_core"):
	_str = str(_path)
	if _path.is_dir() and _str not in sys.path:
		sys.path.insert(0, _str)
