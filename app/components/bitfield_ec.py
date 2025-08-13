import math
from typing import Set

from core.DataStorage import EntityComponent
from torrent import TorrentInfo


class BitfieldEC(EntityComponent):
	def __init__(self, length: int, bitfield: bytes):
		super().__init__()
		self._length: int = length
		self._bitfield: bytearray = bytearray(bitfield)
		self._complete = False
		self._have: Set[int] = set()

	@staticmethod
	def create_empty(info: TorrentInfo):
		return bytearray(math.ceil(info.pieces.num / 8))

	def update(self, bitfield: bytes):
		self._bitfield = bytearray(bitfield)

	def set(self, index: int):
		byte_index, byte_shift = self.__index_to_position(index)
		byte_value = self._bitfield[byte_index]
		self._bitfield[byte_index] = byte_value | 1 << byte_shift
		self._have.add(index)

	def have(self, index: int) -> bool:
		return index in self._have

	@staticmethod
	def __index_to_position(index: int) -> tuple[int, int]:
		return index // 8, 7 - index % 8

	@property
	def have_num(self) -> int:
		return len(self._have)

	def is_interested_in(self, remote_bitfield: "BitfieldEC") -> bool:
		return bool(remote_bitfield._have.difference(self._have))
