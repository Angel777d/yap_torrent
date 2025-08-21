from pathlib import Path

from torrent import TorrentInfo


def load_piece(root: Path, info: TorrentInfo, index: int) -> bytes:
	piece_length = info.pieces.piece_length
	data = bytearray()
	for file, path, start_pos, end_pos in info.piece_to_files(index, root):
		with open(path, "rb") as f:
			offset = start_pos - file.start
			length = end_pos - start_pos
			f.seek(offset)
			buffer = f.read(length)
			# print(f"read {length} bytes from {path}, offset: {offset}")
			data[start_pos % piece_length:end_pos % piece_length] = buffer
	return bytes(data)


def save_piece(root: Path, info: TorrentInfo, index: int, data: bytes) -> None:
	piece_length = info.pieces.piece_length
	for file, path, start_pos, end_pos in info.piece_to_files(index, root):
		path.parent.mkdir(parents=True, exist_ok=True)
		read_from = start_pos % piece_length
		read_to = read_from + end_pos - start_pos
		buffer = data[read_from:read_to]
		offset = start_pos - file.start

		with open(path, "r+b" if path.exists() else "wb") as f:
			f.seek(offset)
			f.write(buffer)
			# print(f"write {len(buffer)} bytes to {path}, offset: {offset}")
