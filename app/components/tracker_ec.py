import time
from typing import List

from core.DataStorage import EntityComponent
from torrent.structures import TrackerAnnounceResponse, PeerInfo


class TorrentTrackerDataEC(EntityComponent):
    def __init__(self, info_hash: bytes, announce_list: List[List[str]]):
        super().__init__()

        self.info_hash: bytes = info_hash
        self.announce_list: List[List[str]] = announce_list

        self.last_update_time: float = 0
        self.interval: float = 0
        self.tracker_id: str = ""
        self.peers: tuple[PeerInfo] = tuple()

    def save_announce(self, response: TrackerAnnounceResponse):
        self.last_update_time = time.time()
        self.interval = response.interval
        self.tracker_id = response.tracker_id

        self.peers = response.peers


# marker to process update data
class TorrentTrackerUpdatedEC(EntityComponent):
    pass
