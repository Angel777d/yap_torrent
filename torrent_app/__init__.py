import logging

from core.DataStorage import DataStorage
from torrent_app.config import Config

logger = logging.getLogger(__name__)


class Env:
	def __init__(self, peer_id: bytes, ip: str, external_ip: str, cfg: Config):
		self.peer_id: bytes = peer_id
		self.ip: str = ip
		self.external_ip: str = external_ip
		self.config: Config = cfg
		self.data_storage = DataStorage()


class System:
	def __init__(self, env: Env):
		self.env: Env = env

	async def start(self) -> 'System':
		return self

	async def update(self, delta_time: float):
		await self._update(delta_time)

	async def _update(self, delta_time: float):
		pass

	def close(self):
		pass


class TimeSystem(System):
	def __init__(self, env: Env, min_update_time: float = 1):
		super().__init__(env)
		self.__min_update_time = min_update_time
		self.__cumulative_update_time = 0

	async def update(self, delta_time: float):
		self.__cumulative_update_time += delta_time
		if self.__cumulative_update_time >= self.__min_update_time:
			await self._update(self.__cumulative_update_time)
			self.__cumulative_update_time = 0
