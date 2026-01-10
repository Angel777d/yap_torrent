import base64
import urllib.parse
from typing import List

from yap_torrent.protocol import logger


class MagnetInfo:
	def __init__(self, magnet_link):
		self.info_hash: bytes = bytes()
		self.name: str = ""
		self.trackers: List[str] = []

		if not magnet_link.startswith("magnet:?"):
			return

		parsed_url = urllib.parse.urlparse(magnet_link)
		query_params = urllib.parse.parse_qs(parsed_url.query)
		if not query_params:
			return

		for k, v in query_params.items():
			if k == "dn":
				for item in v:
					self.name = item
			if k == "tr":
				self.trackers = v
			if k == "xt":
				try:
					for item in v:
						_, protocol, value = item.split(":")
						if len(value) == 40:
							self.info_hash = bytes.fromhex(value.lower())
						elif len(value) == 32:
							self.info_hash = base64.b32decode(value)
						else:
							logger.warning(f"wrong magnet info_hash length for {magnet_link}")
				except Exception as e:
					logger.error(f"wrong magnet info_hash format: {magnet_link}. Error: {e}")

	def is_valid(self):
		return len(self.info_hash) == 20

	def __repr__(self):
		return f"Magnet link: {self.name if self.name else self.info_hash}"
