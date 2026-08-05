import asyncio
from typing import Optional

from angelovich.core.Environment import Environment

from yap_torrent.config import Config


class Env(Environment):
	def __init__(self, peer_id: bytes, ip: str, external_ip: str, cfg: Config):
		super().__init__()
		self.peer_id: bytes = peer_id
		self.ip: str = ip
		self.external_ip: str = external_ip
		self.config: Config = cfg
		self.close_event: Optional[asyncio.Event] = None
