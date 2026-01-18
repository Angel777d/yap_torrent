import asyncio
import concurrent.futures
import hashlib
from pathlib import Path
from typing import Callable, TypeVar, TypeVarTuple

from yap_torrent.protocol import TorrentInfo

_T = TypeVar("_T")
_Ts = TypeVarTuple("_Ts")
_pool = concurrent.futures.ProcessPoolExecutor()


async def execute_in_pool(func: Callable[[*_Ts], _T], *args: *_Ts) -> _T:
	loop = asyncio.get_running_loop()
	return await loop.run_in_executor(_pool, func, *args)


def load_piece(root: Path, info: TorrentInfo, index: int) -> bytes:
	piece_length = info.piece_length
	data = bytearray(info.get_piece_info(index).size)
	for file, start_pos, end_pos in info.piece_to_files(index):
		path = info.get_file_path(root, file)
		with open(path, "rb") as f:
			offset = start_pos - file.start
			length = end_pos - start_pos
			read_from = start_pos % piece_length

			f.seek(offset)
			buffer = f.read(length)

			data[read_from:read_from + length] = buffer
	return bytes(data)


def save_piece(root: Path, info: TorrentInfo, index: int, data: bytes) -> None:
	piece_length = info.piece_length
	for file, start_pos, end_pos in info.piece_to_files(index):
		path = info.get_file_path(root, file)

		# TODO: move to protocol init
		# crate parent path
		path.parent.mkdir(parents=True, exist_ok=True)
		# reserve file size
		if not path.exists():
			with open(path, "wb") as out:
				out.truncate(file.length)

		offset = start_pos - file.start
		length = end_pos - start_pos
		read_from = start_pos % piece_length

		buffer = data[read_from:read_from + length]

		with open(path, "r+b") as f:
			f.seek(offset)
			f.write(buffer)


def check_hash(data: bytes, data_hash: bytes) -> bool:
	return data_hash == hashlib.sha1(data).digest()
