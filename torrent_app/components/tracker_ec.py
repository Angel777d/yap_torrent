import random
import time
from typing import List, Dict, Any

from angelovichcore.DataStorage import EntityComponent
from torrent_app.protocol.structures import TrackerAnnounceResponse


class TorrentTrackerEC(EntityComponent):
	def __init__(self, announce_list: List[List[str]]):
		super().__init__()

		self.announce_list: List[List[str]] = announce_list

		# according to https://bittorrent.org/beps/bep_0012.html
		for announce_tier in self.announce_list:
			random.shuffle(announce_tier)


class TorrentTrackerDataEC(EntityComponent):
	def __init__(self, **kwargs):
		super().__init__()

		self.last_update_time: float = kwargs.get("last_update_time", 0)
		self.interval: float = kwargs.get("interval", 0)
		self.min_interval: float = kwargs.get("min_interval", 0)
		self.tracker_id: bytes = kwargs.get("tracker_id", b'')

		self.failure_reason: str = ""
		self.warning_message: str = ""

	def save_announce(self, response: TrackerAnnounceResponse):
		self.last_update_time = time.monotonic()
		self.interval = response.interval
		self.min_interval = response.min_interval
		self.tracker_id = response.tracker_id

		self.failure_reason = response.failure_reason
		self.warning_message = response.warning_message

	def export(self) -> Dict[str, Any]:
		return {
			"tracker_id": self.tracker_id,
			"last_update_time": self.last_update_time,
			"interval": self.interval,
			"min_interval": self.min_interval,
		}
