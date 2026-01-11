import asyncio
from typing import Optional

from angelovich.core.DataStorage import DataStorage
from angelovich.core.Dispatcher import Dispatcher

from yap_torrent.config import Config


class Env:
	def __init__(self, peer_id: bytes, ip: str, external_ip: str, cfg: Config):
		self.peer_id: bytes = peer_id
		self.ip: str = ip
		self.external_ip: str = external_ip
		self.config: Config = cfg
		self.data_storage: DataStorage = DataStorage()
		self.event_bus = Dispatcher()
		self.close_event: Optional[asyncio.Event] = None
