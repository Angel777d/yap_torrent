import math
from typing import List, Dict
from warnings import deprecated

from torrent.parser import decode


class PeerInfo:
	def __init__(self, data: bytes):
		self.host = f"{data[0]}.{data[1]}.{data[2]}.{data[3]}"
		self.port = int.from_bytes(data[4:], "big")

	def __repr__(self):
		return f'PeerInfo: {self.host}:{self.port}'


class TrackerAnnounceResponse:
	def __init__(self, response, compact: int = 1):
		self.__compact: int = compact
		self.__data: dict = decode(response)

	@property
	def interval(self) -> int:
		return self.__data.get('interval', -1)

	@property
	def min_interval(self) -> int:
		return self.__data.get('min interval', -1)

	@property
	def complete(self) -> int:
		return self.__data.get('complete', 0)

	@property
	def incomplete(self) -> int:
		return self.__data.get('incomplete', 0)

	@property
	def peers(self) -> tuple[PeerInfo, ...]:
		peers: bytes = self.__data.get("peers", b'')
		if self.__compact:
			return tuple(PeerInfo(peers[i: i + 6]) for i in range(0, len(peers), 6))
		raise NotImplementedError()

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
	@deprecated("use announce_list instead")
	def announce(self) -> str:
		return self.__data.get("announce", "WTF")

	@property
	def announce_list(self) -> List[List[str]]:
		return self.__data.get('announce-list', [[self.__data.get("announce", "WTF")]])

	@property
	def size(self) -> int:
		if self.files:
			size = sum(f.length for f in self.files)
		else:
			size = self.info.get('length', 0)
		return size

	@property
	def pieces(self) -> Pieces:
		return Pieces(self.info.get('piece length', 1), self.info.get('pieces', b""))


class PieceData:
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, index: int, length: int):
		self.index = index
		self.__length = length
		self.__data: Dict[int, bytes] = {}
		self.__begin = 0  # block_index * block_size

	def append(self, begin: int, block: bytes):
		block_index = begin // self.block_size
		self.__data[block_index] = block

	def get_next_begin(self):
		result = self.__begin
		self.__begin += self.block_size
		return result

	@property
	def completed(self):
		blocks_num = math.ceil(self.__length / self.block_size)
		return len(self.__data) == blocks_num

	@property
	def block_size(self):
		return self.__BLOCK_SIZE
