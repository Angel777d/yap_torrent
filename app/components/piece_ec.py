import hashlib
import logging
from typing import Hashable, List

from core.DataStorage import EntityComponent
from torrent.structures import PieceInfo

logger = logging.getLogger(__name__)


class PieceEC(EntityComponent):
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info_hash: bytes, piece_info: PieceInfo):
		super().__init__()
		self.data: bytearray = bytearray()

		self.info_hash: bytes = info_hash
		self.__piece_info: PieceInfo = piece_info
		self.__begin = 0  # block_index * block_size

		self.__canceled: List[int] = []
		self.__downloaded: int = 0

	@property
	def index(self) -> int:
		return self.__piece_info.index

	def has_next(self) -> bool:
		return bool(self.__canceled or self.__begin < self.__piece_info.piece_length)

	def get_next(self):
		if self.__canceled:
			return self.index, self.__canceled.pop(), self.__block_size
		begin = self.__begin
		self.__begin += self.__block_size
		return self.index, begin, self.__block_size

	def cancel(self, begin: int):
		self.__canceled.append(begin)

	def append(self, begin: int, block: bytes):
		self.data[begin:begin + self.__block_size] = block
		self.__downloaded += self.__block_size

		# check piece is corrupted and reset piece
		if self.completed and not self.__piece_info.piece_hash == hashlib.sha1(self.data).digest():
			logger.warning(f"piece {self.index} is corrupted. reset")

			self.__downloaded = 0
			self.__begin = 0
			self.__canceled = []

	@property
	def completed(self):
		return self.__downloaded >= self.__piece_info.piece_length

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


class PieceToSaveEC(EntityComponent):
	pass
