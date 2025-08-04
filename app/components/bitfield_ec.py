import math

from core.DataStorage import EntityComponent
from torrent import TorrentInfo


class BitfieldEC(EntityComponent):
    def __init__(self, bitfield: bytes):
        super().__init__()
        self._bitfield: bytearray = bytearray(bitfield)

    @staticmethod
    def create_empty(info: TorrentInfo):
        return bytearray(math.ceil(info.pieces.num / 8))

    def update(self, bitfield: bytes):
        self._bitfield = bytearray(bitfield)

    def set(self, index: int):
        byte_index, byte_shift = self.__index_to_position(index)
        byte_value = self._bitfield[byte_index]
        self._bitfield[byte_index] = byte_value | 1 << byte_shift

    @staticmethod
    def __index_to_position(index: int) -> tuple[int, int]:
        return index // 8, 7 - index % 8

    @property
    def have_num(self) -> int:
        return sum((byte_value & 1 << byte_shift > 0) for byte_value in self._bitfield for byte_shift in range(8))
