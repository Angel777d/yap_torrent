import math
from typing import Set

from angelovichcore.DataStorage import EntityComponent


class BitfieldEC(EntityComponent):
	def __init__(self):
		super().__init__()
		self._have: Set[int] = set()

	@staticmethod
	def index_to_position(index: int) -> tuple[int, int]:
		return index // 8, 7 - index % 8

	@staticmethod
	def __position_to_index(i, offset) -> int:
		return i * 8 + 7 - offset

	def update(self, bitfield: bytes):
		self._have = set(
			self.__position_to_index(i, offset) for i, byte in enumerate(bitfield) for offset in range(8) if
			byte & (1 << offset))
		return self

	def set_index(self, index: int):
		self._have.add(index)

	def have_index(self, index: int) -> bool:
		return index in self._have

	def interested_in(self, remote: "BitfieldEC", exclude: Set[int]) -> Set[int]:
		return remote._have.difference(self._have).difference(exclude)

	@property
	def have_num(self) -> int:
		return len(self._have)

	def dump(self, length) -> bytes:
		return bytes(
			int(sum((1 if self.__position_to_index(i, offset) in self._have else 0) << offset for offset in range(8)))
			for i in
			range(math.ceil(length / 8)))
