import logging
import time
from typing import Hashable, Optional, Set

from angelovich.core.DataStorage import EntityComponent, EntityHashComponent

from yap_torrent.protocol.structures import PieceBlockInfo, PieceInfo

logger = logging.getLogger(__name__)


class PieceEC(EntityHashComponent):
	def __init__(self, info_hash: bytes, info: PieceInfo):
		super().__init__()
		self.info_hash: bytes = info_hash
		self.info: PieceInfo = info

	@staticmethod
	def make_hash(info_hash: bytes, index: int) -> Hashable:
		return info_hash, index

	def __hash__(self):
		return hash(self.make_hash(self.info_hash, self.info.index))


class PieceDownloadProgressEC(EntityComponent):
	def __init__(self, info: PieceInfo):
		super().__init__()
		self.info: PieceInfo = info
		self.data: bytearray = bytearray(info.size)
		self._blocks: Set[PieceBlockInfo] = info.create_blocks()
		self._requested: Set[PieceBlockInfo] = set()
		self._received: Set[int] = set()  # begin offsets
		self.downloading_by: set = set()  # peer entities requesting this piece

	def next_block(self) -> Optional[PieceBlockInfo]:
		for block in self._blocks.difference(self._requested):
			return block
		return None

	def mark_requested(self, block: PieceBlockInfo) -> None:
		self._requested.add(block)

	def release(self, block: PieceBlockInfo) -> None:
		"""Return a still-unreceived block to the pool (e.g. its peer disconnected)."""
		if block.begin not in self._received:
			self._requested.discard(block)

	def all_requested(self) -> bool:
		return len(self._requested) >= len(self._blocks)

	def missing_blocks(self):
		"""Blocks not yet received — endgame candidates for redundant requests."""
		return [block for block in self._blocks if block.begin not in self._received]

	def add_block(self, begin: int, data: bytes) -> bool:
		if begin in self._received:
			return self.is_full()
		self._received.add(begin)
		self.data[begin:begin + len(data)] = data
		return self.is_full()

	def is_full(self) -> bool:
		return len(self._received) == len(self._blocks)


class CompletePieceDataEC(EntityComponent):
	def __init__(self, data: bytes):
		super().__init__()
		self.data: bytes = data

	def get_block(self, begin: int, length: int) -> bytes:
		return self.data[begin:begin + length]


class UploadRequestedEC(EntityComponent):
	"""Peers with pending REQUESTs for this piece (residency + CANCEL tracking)."""

	def __init__(self):
		super().__init__()
		self.peers: set = set()


class PieceTtlEC(EntityComponent):
	"""Eviction TTL for an idle cached piece."""

	def __init__(self) -> None:
		super().__init__()
		self.__last_update: float = time.monotonic()

	def touch(self):
		self.__last_update = time.monotonic()

	def can_remove(self, ttl: float) -> bool:
		return time.monotonic() - self.__last_update > ttl

	@property
	def last_update(self) -> float:
		return self.__last_update
