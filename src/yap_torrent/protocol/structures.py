import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import List, Generator, Tuple, Dict, Any, Iterable, Set

from yap_torrent.protocol import encode


def _block_size():
	return 2 ** 14  # (16kb)


def _calculate_block_size(size: int, begin: int) -> int:
	block_size = _block_size()
	if begin + block_size > size:
		return size % block_size
	return block_size


@dataclass(frozen=True, slots=True)
class PeerInfo:
	host: str
	port: int

	@classmethod
	def from_bytes(cls, data: bytes) -> "PeerInfo":
		return PeerInfo(f"{data[0]}.{data[1]}.{data[2]}.{data[3]}", int.from_bytes(data[4:], "big"))


@dataclass(frozen=True, slots=True)
class PieceBlockInfo:
	index: int
	begin: int
	length: int


@dataclass(frozen=True, slots=True)
class PieceInfo:
	size: int
	index: int
	piece_hash: bytes

	def create_blocks(self) -> Set[PieceBlockInfo]:
		begin = 0
		block_size = _block_size()
		result: Set[PieceBlockInfo] = set()
		while begin < self.size:
			result.add(PieceBlockInfo(self.index, begin, _calculate_block_size(self.size, begin)))
			begin += block_size
		return result


@dataclass(frozen=True, slots=True)
class FileInfo:
	path: List[bytes]
	length: int
	md5sum: bytes
	start: int = 0

	@classmethod
	def from_dict(cls, data: dict, start: int):
		# path.utf-8 is not in BEP-03. But uses widely
		path = data.get("path.utf-8", data.get("path", []))
		return FileInfo(path, data.get("length", 0), data.get("md5sum", b''), start)


@dataclass(frozen=True, slots=True)
class TorrentInfo:
	_data: Dict[str, Any]

	def get_metadata(self) -> bytes:
		return encode(self._data)

	@property
	def name(self) -> str:
		return self.raw_name.decode("utf-8")

	@property
	def raw_name(self) -> bytes:
		# name.utf-8 is not in BEP-03. But uses widely
		return self._data.get('name.utf-8', self._data.get("name", b''))

	@staticmethod
	def __files_generator(files_field: List[dict]):
		start = 0
		for file_dict in files_field:
			info = FileInfo.from_dict(file_dict, start)
			yield info
			start += info.length

	@property
	def files(self) -> Iterable[FileInfo]:
		if 'files' in self._data:
			return tuple(self.__files_generator(self._data["files"]))
		else:
			return (FileInfo([self.raw_name], self._data.get("length", 0), self._data.get("md5sum", b'')),)

	def get_file_path(self, root: Path, file: FileInfo) -> Path:
		# add folder for multifile protocol
		path = root.joinpath(self.name) if 'files' in self._data else root
		for file_path in file.path:
			path = path.joinpath(file_path.decode("utf-8"))
		return path

	@property
	def size(self) -> int:
		return sum(f.length for f in self.files)

	def calculate_downloaded(self, pieces_num: int):
		downloaded = pieces_num * self.piece_length
		return min(downloaded, self.size) / self.size

	def is_complete(self, pieces_num: int) -> bool:
		downloaded = pieces_num * self.piece_length
		return downloaded >= self.size

	@property
	def _pieces(self) -> bytes:
		return self._data.get('pieces', b"")

	@property
	def piece_length(self) -> int:
		return self._data.get('piece length', 0)

	@property
	def pieces_num(self) -> int:
		# pieces: string consisting of the concatenation of all 20-byte SHA1 hash values, one per piece (byte string, i.e., not urlencoded)
		return int(len(self._pieces) / 20)

	def get_piece_hash(self, index: int) -> bytes:
		return self._pieces[index * 20:(index + 1) * 20]

	def get_piece_info(self, index: int) -> PieceInfo:
		return PieceInfo(self.calculate_piece_size(index), index, self.get_piece_hash(index))

	def calculate_piece_size(self, index: int) -> int:
		piece_length = self.piece_length
		torrent_full_size = self.size
		if (index + 1) * piece_length > torrent_full_size:
			size = torrent_full_size % piece_length
		else:
			size = piece_length
		return size

	def piece_to_files(self, index: int) -> Generator[Tuple[FileInfo, int, int]]:
		piece_length = self.piece_length
		piece_start = index * piece_length
		piece_end = piece_start + self.calculate_piece_size(index)
		for file in self.files:
			file_end = file.start + file.length
			if piece_start >= file_end:
				continue
			if file.start >= piece_end:
				continue

			start_pos = max(piece_start, file.start)
			file_end = file.start + file.length
			end_pos = min(piece_end, file_end)

			yield file, start_pos, end_pos


@dataclass(frozen=True, slots=True)
class TorrentFileInfo:
	_data: Dict[str, Any]

	@property
	def info(self):
		return TorrentInfo(self._data.get("info", {}))

	def make_info_hash(self) -> bytes:
		return hashlib.sha1(self.info.get_metadata()).digest()

	# announce: The announcement URL of the tracker (string)
	# announce-list: (optional) this is an extension to the official specification, offering backwards-compatibility. (list of lists of strings).
	@property
	def announce_list(self) -> List[List[str]]:
		if 'announce-list' in self._data:
			result: List[List[str]] = []
			for tier in self._data['announce-list']:
				result.append([announce.decode("utf-8") for announce in tier])
			return result
		elif 'announce' in self._data:
			return [[self._data["announce"].decode("utf-8")]]
		return []

	# creation date: (optional) the creation time of the torrent, in standard UNIX epoch format (integer, seconds since 1-Jan-1970 00:00:00 UTC)
	@property
	def creation_date(self):
		return self._data.get("creation date")

	# comment: (optional) free-form textual comments of the author (string)
	@property
	def comment(self):
		return self._data.get("comment")

	# created by: (optional) name and version of the program used to create the .torrent (string)
	@property
	def created_by(self):
		return self._data.get("created by")

	# encoding: (optional) the string encoding format used to generate the pieces part of the info dictionary in the .torrent metafile (string)
	@property
	def encoding(self):
		return self._data.get("encoding")


@dataclass(eq=False, frozen=True, slots=True)
class TrackerAnnounceResponse:
	_tracker_response: dict
	_compact: int

	@property
	def interval(self) -> int:
		return self._tracker_response.get('interval', -1)

	@property
	def min_interval(self) -> int:
		return self._tracker_response.get('min interval', 60 * 30)

	@property
	def complete(self) -> int:
		return self._tracker_response.get('complete', 0)

	@property
	def incomplete(self) -> int:
		return self._tracker_response.get('incomplete', 0)

	@property
	def peers(self) -> tuple[PeerInfo, ...]:
		peers: bytes = self._tracker_response.get("peers", b'')
		if self._compact:
			return tuple(PeerInfo.from_bytes(peers[i: i + 6]) for i in range(0, len(peers), 6))
		raise NotImplementedError()

	@property
	def tracker_id(self) -> bytes:
		return self._tracker_response.get("tracker id", b'')

	@property
	def failure_reason(self) -> str:
		return self._tracker_response.get("failure reason", b'').decode("utf-8")

	@property
	def warning_message(self) -> str:
		return self._tracker_response.get("warning message", b'').decode("utf-8")


class Bitfield:
	def __init__(self):
		self._have: Set[int] = set()

	@staticmethod
	def __position_to_index(i, offset) -> int:
		return i * 8 + 7 - offset

	def reset(self, value: Set[int]):
		self._have = value

	def update(self, bitfield: bytes):
		self._have = set(
			self.__position_to_index(i, offset) for i, byte in enumerate(bitfield) for offset in range(8) if
			byte & (1 << offset))
		return self

	def set_index(self, index: int):
		self._have.add(index)

	def have_index(self, index: int) -> bool:
		return index in self._have

	def interested_in(self, remote: "Bitfield") -> Set[int]:
		return remote._have.difference(self._have)

	def intersection(self, other: Set[int]) -> Set[int]:
		return self._have.intersection(other)

	@property
	def have_num(self) -> int:
		return len(self._have)

	def dump(self, length) -> bytes:
		return bytes(
			int(sum((1 if self.__position_to_index(i, offset) in self._have else 0) << offset for offset in range(8)))
			for i in range(math.ceil(length / 8)))
