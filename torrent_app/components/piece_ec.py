import logging
import time
from typing import Hashable, Tuple, Set

from core.DataStorage import EntityComponent
from torrent_app.protocol import TorrentInfo
from torrent_app.utils import check_hash

logger = logging.getLogger(__name__)


class PieceEC(EntityComponent):
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info: TorrentInfo, index: int, data: bytes = bytes()):
		super().__init__()

		self.__hash: bytes = info.pieces.get_piece_hash(index)
		self.__size = info.calculate_piece_size(index)

		self.info_hash: bytes = info.info_hash
		self.index: int = index
		self.data: bytes = data
		self.__downloaded = bytearray(self.__size)

		begin = 0
		self.__all_blocks: Set[int] = set()
		while begin < self.__size:
			self.__all_blocks.add(begin)
			begin += self.__block_size

		self.__loaded: Set[int] = set()
		self.__requested: Set[int] = set()

	def has_next(self) -> bool:
		return bool(len(self.__all_blocks.difference(self.__requested, self.__loaded)))

	def _calculate_block_size(self, begin: int) -> int:
		if begin + self.__block_size > self.__size:
			return self.__size % self.__block_size
		return self.__block_size

	def get_next(self, peer_id: bytes) -> Tuple[int, int, int]:
		begin = self.__all_blocks.difference(self.__requested, self.__loaded).pop()
		self.__requested.add(begin)

		block_size = self._calculate_block_size(begin)
		return self.index, begin, block_size

	def cancel(self, peer_id: bytes):
		pass

	def append(self, begin: int, block: bytes):
		self.__downloaded[begin:begin + len(block)] = block

		self.__loaded.add(begin)
		self.__requested.remove(begin)
		if self.completed:
			self.data = bytes(self.__downloaded)
			self.__downloaded.clear()

			# check piece is corrupted and reset piece
			if not check_hash(self.data, self.__hash):
				logger.warning(f"piece {self.index} is corrupted. reset")

				self.__requested.clear()
				self.__loaded.clear()

	@property
	def completed(self):
		return len(self.__all_blocks) == len(self.__loaded)

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
