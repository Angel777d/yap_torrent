import time
from typing import List, Tuple

from core.DataStorage import EntityComponent
from torrent import TorrentInfo
from torrent.structures import TrackerAnnounceResponse, PeerInfo


class TorrentTrackerDataEC(EntityComponent):
	def __init__(self, torrent_info: TorrentInfo):
		super().__init__()

		self.info_hash: bytes = torrent_info.info_hash
		self.announce_list: List[List[str]] = torrent_info.announce_list

		self.last_update_time: float = 0
		self.interval: float = 0
		self.min_interval: float = 0
		self.tracker_id: str = ""
		self.peers: tuple[PeerInfo] = tuple()

	def save_announce(self, response: TrackerAnnounceResponse):
		self.last_update_time = time.time()
		self.interval = response.interval
		self.min_interval = response.min_interval
		self.tracker_id = response.tracker_id

		self.peers = response.peers

	def load(self, tracker_data: Tuple[float, float, float, tuple[PeerInfo]]):
		self.min_interval, self.interval, self.last_update_time, self.peers = tracker_data
		return self

	def save(self) -> Tuple[float, float, float, tuple[PeerInfo]]:
		return self.min_interval, self.interval, self.last_update_time, self.peers


# marker to process update data
class TorrentTrackerUpdatedEC(EntityComponent):
	pass
