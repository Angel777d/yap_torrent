import logging
from enum import IntEnum
from typing import Dict, Tuple

from angelovich.core.DataStorage import EntityComponent

from yap_torrent.protocol import InfoHash

logger = logging.getLogger(__name__)


class FilePriority(IntEnum):
	Low = -1
	Normal = 0
	High = 1


class TorrentFileEC(EntityComponent):
	"""Runtime, metadata-derived description of a single file in a torrent.

	One per file. Recreated every session from TorrentInfo and never persisted.
	Linked to its torrent by the shared info_hash (see iterate_files).
	"""

	def __init__(self, info_hash: InfoHash, index: int, path: str, first_piece: int, pieces_length: int) -> None:
		super().__init__()
		self.info_hash: InfoHash = info_hash
		self.index: int = index
		self.path: str = path
		self.first_piece: int = first_piece
		self.pieces_length: int = pieces_length


class TorrentFileStateEC(EntityComponent):
	"""Persisted per-file user selection. The only per-file state saved to disk."""

	def __init__(self, wanted: bool = True, priority: FilePriority = FilePriority.Normal) -> None:
		super().__init__()
		self.wanted: bool = wanted
		self.priority: FilePriority = priority


class RestoreFileSelectionEC(EntityComponent):
	def __init__(self, selection: Dict[int, Tuple[bool, int]]) -> None:
		super().__init__()
		self.selection: Dict[int, Tuple[bool, int]] = selection
