import math
from typing import Hashable, Dict, List

from core.DataStorage import EntityComponent


class PieceEC(EntityComponent):
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info_hash: bytes, index: int, length: int):
		super().__init__()
		self.__info_hash: bytes = info_hash
		self.__index: int = index
		self.__length = length
		self.__data: Dict[int, bytes] = {}
		self.__begin = 0  # block_index * block_size

		self.__canceled: List[int] = []

	def has_next(self) -> bool:
		return self.__canceled or self.__begin < self.__length

	def get_next(self):
		if self.__canceled:
			return self.__index, self.__canceled.pop(), self.__block_size
		result = self.__begin
		self.__begin += self.__block_size
		return result

	def cancel(self, begin: int):
		self.__canceled.append(begin)

	def append(self, begin: int, block: bytes):
		block_index = begin // self.__block_size
		self.__data[block_index] = block

	@property
	def completed(self):
		blocks_num = math.ceil(self.__length / self.__block_size)
		return len(self.__data) == blocks_num

	@property
	def __block_size(self):
		return self.__BLOCK_SIZE

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.make_hash(self.__info_hash, self.__index)

	@staticmethod
	def make_hash(info_hash: bytes, index: int) -> Hashable:
		return info_hash, index


class CompletedPieceEC(EntityComponent):
	pass
