import logging
from enum import IntEnum
from typing import Dict, Tuple, Set

from angelovich.core.DataStorage import EntityComponent

from yap_torrent.protocol import InfoHash
from yap_torrent.protocol.structures import Bitfield

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
		self.start: int = start
		self.length: int = length
		self.wanted_pieces: set = set(range(first_piece, first_piece + pieces_length))


class TorrentFileProgressEC(EntityComponent):
	"""Bytes of this file we hold. Derived from the torrent bitfield, never persisted."""

	def __init__(self, piece_size: int, total_bytes: int, bytes_completed: int = 0) -> None:
		super().__init__()
		self._bytes_completed: int = bytes_completed
		self._total_bytes: int = total_bytes
		self._piece_size: int = piece_size

	@property
	def bytes_completed(self):
		return min(self._bytes_completed, self._total_bytes)

	def increment_piece(self):
		self._bytes_completed += self._piece_size

	def update_progress(self, bitfield: Bitfield, wanted: Set[int]) -> "TorrentFileProgressEC":
		self._bytes_completed = len(bitfield.have.intersection(wanted)) * self._piece_size
		return self

	def reset(self):
		self._bytes_completed = 0

class TorrentFileStateEC(EntityComponent):
	def __init__(self, is_wanted: bool = True, priority: int = 0) -> None:
		super().__init__()
		self.is_wanted: bool = is_wanted
		self.priority: FilePriority = FilePriority(priority)

	def serialize(self) -> Tuple[bool, int]:
		return self.is_wanted, self.priority.value


class RestoreFileSelectionEC(EntityComponent):
	def __init__(self, selection: Dict[int, Tuple[bool, int]]) -> None:
		super().__init__()
		self.selection: Dict[int, Tuple[bool, int]] = selection
