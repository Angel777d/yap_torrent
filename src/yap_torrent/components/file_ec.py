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
	"""Where one file lives: piece range plus its byte range (start/length) in the torrent."""

	def __init__(self, info_hash: InfoHash, index: int, path: str, first_piece: int, pieces_length: int,
	             start: int = 0, length: int = 0) -> None:
		super().__init__()
		self.info_hash: InfoHash = info_hash
		self.index: int = index
		self.path: str = path
		self.first_piece: int = first_piece
		self.pieces_length: int = pieces_length
		self.start: int = start
		self.length: int = length


class TorrentFileStateEC(EntityComponent):
	def __init__(self, wanted: bool = True, priority: int = 0) -> None:
		super().__init__()
		self.wanted: bool = wanted
		self.priority: FilePriority = FilePriority(priority)

	def serialize(self) -> Tuple[bool, int]:
		return self.wanted, self.priority.value


class RestoreFileSelectionEC(EntityComponent):
	def __init__(self, selection: Dict[int, Tuple[bool, int]]) -> None:
		super().__init__()
		self.selection: Dict[int, Tuple[bool, int]] = selection
