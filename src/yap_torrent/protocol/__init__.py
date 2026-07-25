import logging
from pathlib import Path
from typing import Optional

from yap_torrent.protocol.parser import decode, encode
from yap_torrent.protocol.structures import TorrentInfo, Metainfo

logger = logging.getLogger(__name__)

# Type alias for better readability
InfoHash = bytes


def load_torrent_file(path: Path) -> Optional[Metainfo]:
	try:
		with open(path, "rb") as f:
			data = decode(f.read())
	except Exception as ex:
		logger.error(f"wrong torrent '{path}' file format. exception: {ex}")
		return None

	return Metainfo(data)
