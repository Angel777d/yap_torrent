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
		self.__downloaded: bytearray = bytearray(self.__size)

		self.__blocks: Set[int] = self.__create_blocks()

	def __create_blocks(self) -> Set[int]:
		if self.completed:
			return set()
		begin = 0
		result: Set[int] = set()
		while begin < self.__size:
			result.add(begin)
			begin += self.__block_size
		return result

	def has_next(self, in_progress: set) -> bool:
		return bool(len(self.__blocks.difference(in_progress)))

	def _calculate_block_size(self, begin: int) -> int:
		if begin + self.__block_size > self.__size:
			return self.__size % self.__block_size
		return self.__block_size

	def get_next(self, in_progress: set) -> Tuple[int, int, int]:
		begin = self.__blocks.difference(in_progress).pop()
		block_size = self._calculate_block_size(begin)
		return self.index, begin, block_size

	def append(self, begin: int, block: bytes):
		# already completed
		if self.completed:
			return
		# block already downloaded. just skip
		if begin not in self.__blocks:
			return

		self.__downloaded[begin:begin + len(block)] = block
		self.__blocks.remove(begin)

		if not self.__blocks:
			self.data = bytes(self.__downloaded)
			self.__downloaded.clear()

			# check piece is corrupted and reset piece
			if not check_hash(self.data, self.__hash):
				logger.warning(f"piece {self.index} is corrupted. reset")
				self.__blocks = self.__create_blocks()
				self.data = bytes()

			# mark to save
			self.add_marker(PieceToSaveEC)

	@property
	def completed(self):
		return len(self.data) == self.__size

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
		self.__last_update = time.monotonic()

	def can_remove(self) -> bool:
		return time.monotonic() - self.__last_update > self.REMOVE_TIMEOUT

	@property
	def last_update(self) -> float:
		return self.__last_update
