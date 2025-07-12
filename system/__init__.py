import json
import math
import time
from typing import Dict, List, Set

from torrent.structures import TorrentInfo, PeerInfo, TrackerAnnounceResponse, BitField, Pieces


class Config:
    DEFAULT_CONFIG = "config.json"

    def __init__(self):
        with open(self.DEFAULT_CONFIG, "r") as f:
            data = json.load(f)
            self.data_folder = data.get("data_folder", "data")

            self.active_folder = data.get("active_folder", f"{self.data_folder}/active")
            self.watch_folder = data.get("watch_folder", f"{self.data_folder}/watch")
            self.download_folder = data.get("download_folder", f"{self.data_folder}/download")
            self.trash_folder = data.get("trash_folder", f"{self.data_folder}/trash")

            self.port: int = int(data.get("port", 6889))

            self.max_connections = int(data.get("max_connections", 30))

class ActiveTorrent:
    def __init__(self, info: TorrentInfo):
        self.info: TorrentInfo = info
        self.downloaded: int = 0
        self.uploaded: int = 0
        self.bitfield = BitField(info.pieces.num)

        self.last_update_time: float = 0
        self.interval: float = 0
        self.tracker_id: str = ""

    def save_announce(self, response: TrackerAnnounceResponse):
        self.tracker_id = response.tracker_id
        self.interval = response.interval
        self.last_update_time = time.time()

    @staticmethod
    def load(info: TorrentInfo):
        result = ActiveTorrent(info)
        # TODO: load info
        return result


class PieceData:
    __BLOCK_SIZE = 2 ** 14  # (16kb)

    def __init__(self, index: int, length: int):
        self.index = index
        self.__length = length
        self.__data: Dict[int, bytes] = {}
        self.__begin = 0  # block_index * block_size

    def append(self, begin: int, block: bytes):
        block_index = begin // self.block_size
        self.__data[block_index] = block

    def get_next_begin(self):
        result = self.__begin
        self.__begin += self.block_size
        return result

    @property
    def completed(self):
        blocks_num = math.ceil(self.__length / self.block_size)
        return len(self.__data) == blocks_num

    @property
    def block_size(self):
        return self.__BLOCK_SIZE


class Storage:
    def __init__(self, peer_id: bytes, external_ip: str):
        # common
        self.peer_id: bytes = peer_id
        self.external_ip: str = external_ip

        # torrents
        self.new_torrents: Dict[bytes, TorrentInfo] = {}
        self.active_torrents: Dict[bytes, ActiveTorrent] = {}

        # peers
        self.peers: Dict[bytes, Set[PeerInfo]] = {}

        # pieces
        self.loaded_pieces: Dict[bytes, List[PieceData]] = {}


class System:
    def __init__(self, config: Config, storage: Storage):
        self.config: Config = config
        self.storage: Storage = storage

    async def start(self) -> 'System':
        return self

    async def update(self, delta_time: float):
        pass
