import math
from importlib.metadata import files
from typing import List

from torrent.parser import decode


class PeerInfo:
    def __init__(self, data):
        self.host = f"{data[0]}.{data[1]}.{data[2]}.{data[3]}"
        self.port = int.from_bytes(data[4:], "big")
        self.hash = f'{self.host}:{self.port}'

    @staticmethod
    def from_binary_peers(peers):
        return [PeerInfo(peers[i: i + 6]) for i in range(0, len(peers), 6)]

    def __repr__(self):
        return self.hash

    def __hash__(self):
        return self.hash


class TrackerAnnounceResponse:
    def __init__(self, response, compact: int = 1):
        self.__compact = compact
        self.__data = decode(response)

    @property
    def interval(self):
        return self.__data.get('interval', -1)

    @property
    def min_interval(self):
        return self.__data.get('min interval', -1)

    @property
    def complete(self):
        return self.__data.get('complete', 0)

    @property
    def incomplete(self):
        return self.__data.get('incomplete', 0)

    @property
    def peers(self) -> List[PeerInfo]:
        return PeerInfo.from_binary_peers(self.__data.get("peers", b''))

    @property
    def tracker_id(self) -> str:
        return self.__data.get("tracker id", "")

    @property
    def failure_reason(self) -> str:
        return self.__data.get("failure reason", "")

    @property
    def warning_message(self) -> str:
        return self.__data.get("warning message", "")


class PieceInfo:
    def __init__(self, index: int, piece_length: int, piece_hash: bytes):
        self.index: int = index
        self.piece_length: int = piece_length
        self.piece_hash: bytes = piece_hash


class Pieces:
    def __init__(self, piece_length: int, pieces: bytes):
        self.__piece_length: int = piece_length
        self.__pieces: bytes = pieces

    def get_piece(self, index: int) -> PieceInfo:
        return PieceInfo(index, self.__piece_length, self.__pieces[index * 20:(index + 1) * 20])

    @property
    def num(self) -> int:
        # pieces: string consisting of the concatenation of all 20-byte SHA1 hash values, one per piece (byte string, i.e. not urlencoded)
        return int(len(self.__pieces) / 20)

    @property
    def piece_length(self) -> int:
        return self.__piece_length


class FileInfo:
    def __init__(self, path: List[str], length: int, md5sum: str):
        self.path: List[str] = path
        self.length: int = length
        self.md5sum: str = md5sum

    @classmethod
    def from_dict(cls, data: dict):
        return FileInfo(data.get("path", []), data.get("length", 0), data.get("md5sum", ''))


class TorrentInfo:
    def __init__(self, info_hash: bytes, data: dict):
        self.info_hash: bytes = info_hash
        self.__data = data

    def is_valid(self) -> bool:
        return len(self.info_hash) > 0

    @property
    def info(self) -> dict:
        return self.__data.get("info", {})

    @property
    def name(self) -> str:
        info = self.info
        return info.get('name.utf-8', info.get("name", ""))

    @property
    def files(self) -> tuple[FileInfo]:
        files_field = self.info.get('files', [])
        if files_field:
            return *(FileInfo.from_dict(file_dict) for file_dict in files_field),
        else:
            return (FileInfo([self.name], self.info.get("length", 0), self.info.get("md5sum", '')),)

    @property
    def announce(self) -> str:
        return self.__data.get("announce", "WTF")

    @property
    def announce_list(self) -> List[str]:
        return self.__data.get('announce-list', [self.__data.get("announce", "WTF")])

    @property
    def size(self) -> int:
        if self.files:
            size = sum(f.get('length') for f in self.files)
        else:
            size = self.info.get('length', 0)
        return size

    @property
    def pieces(self) -> Pieces:
        return Pieces(self.info.get('piece length', 1), self.info.get('pieces', b""))


class BitField:
    def __init__(self, length: int):
        self._length: int = length
        self._bitfield = bytearray(math.ceil(length / 8))

    @staticmethod
    def __index_to_position(index: int) -> tuple[int, int]:
        byte_index = index // 8
        byte_shift = 7 - index % 8
        return byte_index, byte_shift

    def update(self, bitfield: bytearray):
        self._bitfield = bitfield

    def set_at(self, index: int):
        byte_index, byte_shift = self.__index_to_position(index)
        byte_value = self._bitfield[byte_index]
        self._bitfield[byte_index] = byte_value | 1 << byte_shift

    # def reset_at(self, index: int):
    #     byte_index, byte_shift = self.__index_to_position(index)
    #     byte_value = self._bitfield[byte_index]
    #     self._bitfield[byte_index] = byte_value & ~(1 << byte_shift)

    def get_at(self, index) -> bool:
        byte_index, byte_shift = self.__index_to_position(index)
        byte_value = self._bitfield[byte_index]
        return byte_value & 1 << byte_shift != 0

    def get_next_index(self, owned: 'BitField') -> int:
        for i in range(0, len(self._bitfield)):
            byte_value = self._bitfield[i] & ~owned._bitfield[i]
            if byte_value != 0:
                for byte_shift in range(0, 8):
                    if byte_value & 1 << byte_shift:
                        return i * 8 + byte_shift
        return -1
