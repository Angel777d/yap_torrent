import time
from typing import List

from angelovichcore.DataStorage import EntityComponent
from torrent_app.protocol.structures import TrackerAnnounceResponse, PeerInfo


class SaveData:
	def __init__(self) -> None:
		self.tracker_id: str = ""
		self.last_update_time: float = 0
		self.interval: float = 0
		self.min_interval: float = 0
		self.peers: tuple[PeerInfo, ...] = tuple()
		self.uploaded: int = 0
		self.announce_list: List[List[str]] = []


class TorrentTrackerDataEC(EntityComponent):
	def __init__(self, announce_list: List[List[str]]):
		super().__init__()

		self.announce_list: List = announce_list

		self.last_update_time: float = 0
		self.interval: float = 0
		self.min_interval: float = 0
		self.tracker_id: str = ""

		self.uploaded = 0

	def save_announce(self, response: TrackerAnnounceResponse):
		self.last_update_time = time.monotonic()
		self.interval = response.interval
		self.min_interval = response.min_interval
		self.tracker_id = response.tracker_id

	def update_uploaded(self, length: int) -> None:
		self.uploaded += length

	def import_save(self, data: SaveData):
		self.tracker_id = data.tracker_id
		self.last_update_time = data.last_update_time
		self.interval = data.interval
		self.min_interval = data.min_interval
		self.uploaded = data.uploaded
		return self

	def export_save(self) -> SaveData:
		result = SaveData()
		result.tracker_id = self.tracker_id
		result.last_update_time = self.last_update_time
		result.interval = self.interval
		result.min_interval = self.min_interval
		result.uploaded = self.uploaded
		result.announce_list = self.announce_list
		return result
