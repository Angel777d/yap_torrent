from typing import Hashable

from core.DataStorage import EntityComponent
from torrent.structures import PieceData


class PieceEC(EntityComponent):
    def __init__(self, info_hash: bytes, index: int, length: int):
        super().__init__()
        self.info_hash: bytes = info_hash
        self.data: PieceData = PieceData(index, length)

    @classmethod
    def is_hashable(cls) -> bool:
        return True

    def get_hash(self) -> Hashable:
        return self.make_hash(self.info_hash, self.data.index)

    @staticmethod
    def make_hash(info_hash: bytes, index: int) -> Hashable:
        return info_hash, index


class CompletedPieceEC(EntityComponent):
    pass
