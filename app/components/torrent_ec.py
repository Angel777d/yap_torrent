from typing import Hashable

from core.DataStorage import EntityComponent
from torrent import TorrentInfo


class TorrentInfoEC(EntityComponent):
	def __init__(self, torrent_info: TorrentInfo, created_at: float) -> None:
		super().__init__()
		self.info: TorrentInfo = torrent_info
		self.created_at: float = created_at

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.info.info_hash
