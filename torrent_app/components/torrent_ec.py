from pathlib import Path
from typing import Hashable

from angelovichcore.DataStorage import EntityComponent
from torrent_app.protocol import TorrentInfo


class TorrentHashEC(EntityComponent):
	def __init__(self, info_hash: bytes) -> None:
		super().__init__()
		self.info_hash: bytes = info_hash

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.info_hash


class TorrentInfoEC(EntityComponent):
	def __init__(self, torrent_info: TorrentInfo) -> None:
		super().__init__()
		self.info: TorrentInfo = torrent_info


class TorrentPathEC(EntityComponent):
	def __init__(self, path: Path) -> None:
		super().__init__()
		self.root_path: Path = path


class SaveTorrentEC(EntityComponent):
	pass


class ValidateTorrentEC(EntityComponent):
	pass
