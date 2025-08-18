import logging

from app.config import Config
from core.DataStorage import DataStorage

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class Env:
	def __init__(self, peer_id: bytes, external_ip: str, cfg: Config):
		self.peer_id: bytes = peer_id
		self.external_ip: str = external_ip
		self.config: Config = cfg
		self.data_storage = DataStorage()


class System:
	def __init__(self, env: Env):
		self.env: Env = env

	async def start(self) -> 'System':
		return self

	async def update(self, delta_time: float):
		pass

	def close(self):
		pass
