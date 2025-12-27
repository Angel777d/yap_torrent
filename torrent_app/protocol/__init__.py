import logging
from typing import Optional

from torrent_app.protocol.parser import decode, encode
from torrent_app.protocol.structures import TorrentInfo, TorrentFileInfo

logger = logging.getLogger(__name__)


def load_torrent_file(path) -> Optional[TorrentFileInfo]:
	try:
		with open(path, "rb") as f:
			data = decode(f.read())
	except Exception as ex:
		logger.error(f"wrong torrent '{path}' file format. exception: {ex}")
		return None

	return TorrentFileInfo(data)
