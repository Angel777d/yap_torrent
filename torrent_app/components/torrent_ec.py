from pathlib import Path
from typing import Hashable, Dict

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


class TorrentStatsEC(EntityComponent):
	def __init__(self, **kwargs) -> None:
		super().__init__()

		self.uploaded = kwargs.get("uploaded", 0)
		self.downloaded = kwargs.get("downloaded", 0)

	def export(self) -> Dict[str, int]:
		return {
			"uploaded": self.uploaded,
			"downloaded": self.downloaded
		}

	def update_uploaded(self, length: int) -> None:
		self.uploaded += length

	def update_downloaded(self, length: int) -> None:
		self.downloaded += length


class SaveTorrentEC(EntityComponent):
	pass


class ValidateTorrentEC(EntityComponent):
	pass
