import hashlib
import logging

from torrent_app.protocol.parser import decode, encode
from torrent_app.protocol.structures import TorrentInfo

logger = logging.getLogger(__name__)


def load_torrent_file(path) -> TorrentInfo:
	try:
		with open(path, "rb") as f:
			data = decode(f.read())
	except Exception as ex:
		logger.warning(f"wrong torrent file format. exception: {ex}")
		return TorrentInfo(bytes(), dict())

	encoded_info = encode(data.get("info"))
	info_hash = hashlib.sha1(encoded_info).digest()
	return TorrentInfo(info_hash, data)
