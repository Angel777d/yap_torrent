from pathlib import Path
from typing import List, Generator, Tuple
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
		return self.__data.get('min interval', 60 * 30)

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


class Pieces:
	def __init__(self, piece_length: int, pieces: bytes):
		self.__piece_length: int = piece_length
		self.__pieces: bytes = pieces

	def get_piece_hash(self, index: int) -> bytes:
		return self.__pieces[index * 20:(index + 1) * 20]

	@property
	def num(self) -> int:
		# pieces: string consisting of the concatenation of all 20-byte SHA1 hash values, one per piece (byte string, i.e. not urlencoded)
		return int(len(self.__pieces) / 20)

	@property
	def piece_length(self) -> int:
		return self.__piece_length


class FileInfo:
	def __init__(self, path: List[str], length: int, md5sum: str, start: int = 0):
		self.path: List[str] = path
		self.length: int = length
		self.md5sum: str = md5sum
		self.start: int = start

	@classmethod
	def from_dict(cls, data: dict, start: int):
		return FileInfo(data.get("path", []), data.get("length", 0), data.get("md5sum", ''), start)


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

	@staticmethod
	def __files_generator(files_field: List[dict]):
		start = 0
		for file_dict in files_field:
			info = FileInfo.from_dict(file_dict, start)
			yield info
			start += info.length

	@property
	def files(self) -> tuple[FileInfo]:
		files_field: List[dict] = self.info.get('files', [])
		if files_field:
			return *(self.__files_generator(files_field)),
		else:
			return (FileInfo([self.name], self.info.get("length", 0), self.info.get("md5sum", '')),)

	def _get_file_path(self, root: Path, file: FileInfo) -> Path:
		# add folder for multifile torrent
		path = root.joinpath(self.name) if 'files' in self.info else root
		for file_path in file.path:
			path = path.joinpath(file_path)
		return path

	@property
	@deprecated("use announce_list instead")
	def announce(self) -> str:
		return self.__data.get("announce", "WTF")

	@property
	def announce_list(self) -> List[List[str]]:
		return self.__data.get('announce-list', [[self.__data.get("announce", "WTF")]])

	@property
	def size(self) -> int:
		return sum(f.length for f in self.files)

	@property
	def pieces(self) -> Pieces:
		return Pieces(self.info.get('piece length', 1), self.info.get('pieces', b""))

	def calculate_piece_size(self, index: int) -> int:
		piece_length = self.pieces.piece_length
		torrent_full_size = self.size
		if (index + 1) * piece_length > torrent_full_size:
			size = torrent_full_size % piece_length
		else:
			size = piece_length
		return size

	def piece_to_files(self, index: int, root: Path) -> Generator[Tuple[FileInfo, Path, int, int]]:
		piece_length = self.pieces.piece_length
		piece_start = index * piece_length
		piece_end = piece_start + self.calculate_piece_size(index)
		for file in self.files:
			file_end = file.start + file.length
			if piece_start >= file_end:
				continue
			if file.start >= piece_end:
				continue

			path = self._get_file_path(root, file)
			start_pos = max(piece_start, file.start)
			file_end = file.start + file.length
			end_pos = min(piece_end, file_end)

			yield file, path, start_pos, end_pos
