import logging
from typing import Optional

from yap_torrent.protocol.parser import decode, encode
from yap_torrent.protocol.structures import TorrentInfo, TorrentFileInfo

logger = logging.getLogger(__name__)


def load_torrent_file(path) -> Optional[TorrentFileInfo]:
	try:
		with open(path, "rb") as f:
			data = decode(f.read())
	except Exception as ex:
		logger.error(f"wrong torrent '{path}' file format. exception: {ex}")
		return None

	return TorrentFileInfo(data)
