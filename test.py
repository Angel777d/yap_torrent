import math
from os.path import getsize
from pathlib import Path
from typing import Generator, Tuple, List

from torrent_app.utils import load_piece, save_piece

_TEST_FILES = [Path("test1.rar"), Path("test2.rar")]
_PIECE_SIZE = 2 ** 16


class TorrentInfo:
	class FileInfo:
		def __init__(self, path: Path, start: int, length: int) -> None:
			self.path = path
			self.start = start
			self.length = length

	class Pieces:
		def __init__(self):
			self.piece_length = _PIECE_SIZE

	def __init__(self):
		self.pieces = TorrentInfo.Pieces()

	def _get_file_path(self, root: Path, file: FileInfo) -> Path:
		return root.joinpath(file.path)

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
		files, size = self.files

		for file in files:
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

	@property
	def files(self) -> Tuple[List[FileInfo], int]:
		result = []
		start = 0
		for p in _TEST_FILES:
			length = getsize(p)
			result.append(TorrentInfo.FileInfo(p, start, length))
			start += getsize(p)
		return result, start

	@property
	def size(self) -> int:
		files, size = self.files
		return size


info = TorrentInfo()

num_pieces = math.ceil(info.size / _PIECE_SIZE)

for index in range(num_pieces):
	print(index)
	data = load_piece(Path("."), info, index)
	save_piece(Path("test"), info, index, data)
