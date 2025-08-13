import hashlib

from torrent.parser import decode, encode
from torrent.structures import TorrentInfo


def load_torrent_file(path) -> TorrentInfo:
	try:
		with open(path, "rb") as f:
			data = decode(f.read())
	except Exception as ex:
		print(f"wrong file format. exception: {ex}")
		return TorrentInfo(bytes(), dict())

	encoded_info = encode(data.get("info"))
	info_hash = hashlib.sha1(encoded_info).digest()
	return TorrentInfo(info_hash, data)
