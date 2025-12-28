import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import List, Generator, Tuple

from torrent_app.protocol import encode
from torrent_app.protocol.parser import decode


@dataclass(unsafe_hash=True)
class PeerInfo:
	host: str
	port: int

	@classmethod
	def from_bytes(cls, data: bytes) -> "PeerInfo":
		return PeerInfo(f"{data[0]}.{data[1]}.{data[2]}.{data[3]}", int.from_bytes(data[4:], "big"))


class TrackerAnnounceResponse:
	def __init__(self, response, compact: int = 1):
		self.__compact: int = compact
		self.__tracker_response: dict = decode(response)

	@property
	def interval(self) -> int:
		return self.__tracker_response.get('interval', -1)

	@property
	def min_interval(self) -> int:
		return self.__tracker_response.get('min interval', 60 * 30)

	@property
	def complete(self) -> int:
		return self.__tracker_response.get('complete', 0)

	@property
	def incomplete(self) -> int:
		return self.__tracker_response.get('incomplete', 0)

	@property
	def peers(self) -> tuple[PeerInfo, ...]:
		peers: bytes = self.__tracker_response.get("peers", b'')
		if self.__compact:
			return tuple(PeerInfo.from_bytes(peers[i: i + 6]) for i in range(0, len(peers), 6))
		raise NotImplementedError()

	@property
	def tracker_id(self) -> bytes:
		return self.__tracker_response.get("tracker id", b'')

	@property
	def failure_reason(self) -> str:
		return self.__tracker_response.get("failure reason", b'').decode("utf-8")

	@property
	def warning_message(self) -> str:
		return self.__tracker_response.get("warning message", b'').decode("utf-8")


class Pieces:
	def __init__(self, piece_length: int, pieces: bytes):
		self.__piece_length: int = piece_length
		self.__pieces: bytes = pieces

	def get_piece_hash(self, index: int) -> bytes:
		return self.__pieces[index * 20:(index + 1) * 20]

	@property
	def num(self) -> int:
		# pieces: string consisting of the concatenation of all 20-byte SHA1 hash values, one per piece (byte string, i.e., not urlencoded)
		return int(len(self.__pieces) / 20)

	@property
	def piece_length(self) -> int:
		return self.__piece_length


class FileInfo:
	def __init__(self, path: List[bytes], length: int, md5sum: bytes, start: int = 0):
		self.path: List[bytes] = path
		self.length: int = length
		self.md5sum: bytes = md5sum
		self.start: int = start

	@classmethod
	def from_dict(cls, data: dict, start: int):
		# path.utf-8 is not in BEP-03. But uses widely
		path = data.get("path.utf-8", data.get("path", []))
		return FileInfo(path, data.get("length", 0), data.get("md5sum", b''), start)


class TorrentInfo:
	def __init__(self, info: dict) -> None:
		self.__info: dict = info

	def get_metadata(self) -> bytes:
		return encode(self.__info)

	@property
	def name(self) -> str:
		return self.raw_name.decode("utf-8")

	@property
	def raw_name(self) -> bytes:
		return self.__info.get('name.utf-8', self.__info.get("name", b''))

	@staticmethod
	def __files_generator(files_field: List[dict]):
		start = 0
		for file_dict in files_field:
			info = FileInfo.from_dict(file_dict, start)
			yield info
			start += info.length

	@property
	def files(self) -> tuple[FileInfo]:
		files_field: List[dict] = self.__info.get('files', [])
		if files_field:
			return *(self.__files_generator(files_field)),
		else:
			return (FileInfo([self.raw_name], self.__info.get("length", 0), self.__info.get("md5sum", b'')),)

	@property
	def size(self) -> int:
		return sum(f.length for f in self.files)

	@property
	def pieces(self) -> Pieces:
		return Pieces(self.__info.get('piece length', 1), self.__info.get('pieces', b""))

	def get_file_path(self, root: Path, file: FileInfo) -> Path:
		# add folder for multifile protocol
		path = root.joinpath(self.name) if 'files' in self.__info else root
		for file_path in file.path:
			path = path.joinpath(file_path.decode("utf-8"))
		return path

	def calculate_piece_size(self, index: int) -> int:
		piece_length = self.pieces.piece_length
		torrent_full_size = self.size
		if (index + 1) * piece_length > torrent_full_size:
			size = torrent_full_size % piece_length
		else:
			size = piece_length
		return size

	def piece_to_files(self, index: int) -> Generator[Tuple[FileInfo, int, int]]:
		piece_length = self.pieces.piece_length
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


class TorrentFileInfo:
	def __init__(self, data: dict):
		self.__data = data
		self.info = TorrentInfo(self.__data.get("info", {}))
		self.__info_hash = hashlib.sha1(self.info.get_metadata()).digest()

	def is_valid(self) -> bool:
		return len(self.__info_hash) > 0

	@property
	def info_hash(self) -> bytes:
		return self.__info_hash

	# announce: The announcement URL of the tracker (string)
	# announce-list: (optional) this is an extension to the official specification, offering backwards-compatibility. (list of lists of strings).
	@property
	def announce_list(self) -> List[List[str]]:
		if 'announce-list' in self.__data:
			result: List[List[str]] = []
			for tier in self.__data['announce-list']:
				result.append([announce.decode("utf-8") for announce in tier])
			return result
		elif 'announce' in self.__data:
			return [[self.__data["announce"].decode("utf-8")]]
		return []

	# creation date: (optional) the creation time of the torrent, in standard UNIX epoch format (integer, seconds since 1-Jan-1970 00:00:00 UTC)
	@property
	def creation_date(self):
		return self.__data.get("creation date")

	# comment: (optional) free-form textual comments of the author (string)
	@property
	def comment(self):
		return self.__data.get("comment")

	# created by: (optional) name and version of the program used to create the .torrent (string)
	@property
	def created_by(self):
		return self.__data.get("created by")

	# encoding: (optional) the string encoding format used to generate the pieces part of the info dictionary in the .torrent metafile (string)
	@property
	def encoding(self):
		return self.__data.get("encoding")


@dataclass(unsafe_hash=True)
class PieceBlock:
	index: int
	begin: int
	length: int


@dataclass
class PieceInfo:
	size: int
	index: int
	piece_hash: bytes

	@staticmethod
	def from_torrent(info: TorrentInfo, index: int) -> "PieceInfo":
		return PieceInfo(info.calculate_piece_size(index), index, info.pieces.get_piece_hash(index))
