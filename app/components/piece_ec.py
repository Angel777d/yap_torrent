import hashlib
import logging
import time
from typing import Hashable, List, Tuple

from core.DataStorage import EntityComponent
from torrent import TorrentInfo

logger = logging.getLogger(__name__)


class PieceEC(EntityComponent):
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info: TorrentInfo, index: int, data: bytes = bytes()):
		super().__init__()
		self.data: bytearray = bytearray(data)
		self.__downloaded: int = len(data)

		self.info_hash: bytes = info.info_hash
		self.index: int = index

		self.__hash: bytes = info.pieces.get_piece_hash(index)
		self.__begin = 0  # block_index * block_size

		self.__size = info.calculate_piece_size(index)

		self.__in_progress: List[Tuple[bytes, float, int, int, int]] = []
		self.__canceled: List[Tuple[bytes, float, int, int, int]] = []

	def has_next(self) -> bool:
		return bool(self.__canceled or self.__begin < self.__size)

	def _calculate_block_size(self, begin: int) -> int:
		if begin + self.__block_size > self.__size:
			return self.__size % self.__block_size
		return self.__block_size

	def get_next(self, peer_id: bytes) -> Tuple[int, int, int]:
		if self.__canceled:
			_, _, index, begin, block_size = self.__canceled.pop()
			self.__in_progress.append((peer_id, time.time(), self.index, begin, block_size))
			return index, begin, block_size

		begin = self.__begin
		block_size = self._calculate_block_size(begin)
		self.__begin += block_size
		self.__in_progress.append((peer_id, time.time(), self.index, begin, block_size))
		return self.index, begin, block_size

	def cancel(self, peer_id: bytes):
		self.__canceled.extend(p for p in self.__in_progress if p[0] == peer_id)
		self.__in_progress = [p for p in self.__in_progress if p[0] != peer_id]

	def append(self, begin: int, block: bytes):
		self.data[begin:begin + len(block)] = block
		new_progress = [p for p in self.__in_progress if p[3] != begin]
		if len(self.__in_progress) == len(new_progress):
			logger.debug(f"Did not wait for block {begin}")
			return

		self.__in_progress = new_progress
		self.__downloaded += len(block)

		# check piece is corrupted and reset piece
		if self.completed and not self.__hash == hashlib.sha1(self.data).digest():
			logger.warning(f"piece {self.index} is corrupted. reset")

			self.__downloaded = 0
			self.__begin = 0
			self.__canceled = []

	def check_hash(self) -> bool:
		return self.__hash == hashlib.sha1(self.data).digest()

	@property
	def completed(self):
		return self.__downloaded >= self.__size

	@property
	def __block_size(self):
		return self.__BLOCK_SIZE

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.make_hash(self.info_hash, self.index)

	@staticmethod
	def make_hash(info_hash: bytes, index: int) -> Hashable:
		return info_hash, index

	def get_block(self, begin, length) -> bytes:
		return self.data[begin:begin + length]


class PieceToSaveEC(EntityComponent):
	pass


class PiecePendingRemoveEC(EntityComponent):
	REMOVE_TIMEOUT = 15  # TODO: move to config

	def __init__(self) -> None:
		super().__init__()
		self.__last_update: float = 0

	def update(self):
		self.__last_update = time.time()

	def can_remove(self) -> bool:
		return time.time() - self.__last_update > self.REMOVE_TIMEOUT

	@property
	def last_update(self) -> float:
		return self.__last_update
