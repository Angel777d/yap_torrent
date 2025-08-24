from typing import Hashable

from core.DataStorage import EntityComponent
from torrent_app.protocol import TorrentInfo


class TorrentInfoEC(EntityComponent):
	def __init__(self, torrent_info: TorrentInfo) -> None:
		super().__init__()
		self.info: TorrentInfo = torrent_info

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.info.info_hash


class TorrentSaveEC(EntityComponent):
	pass
