import asyncio
import logging
from asyncio import Task
from typing import Coroutine, Any

from angelovichcore.DataStorage import DataStorage
from angelovichcore.Dispatcher import Dispatcher
from yap_torrent.config import Config

logger = logging.getLogger(__name__)


class Env:
	def __init__(self, peer_id: bytes, ip: str, external_ip: str, cfg: Config):
		self.peer_id: bytes = peer_id
		self.ip: str = ip
		self.external_ip: str = external_ip
		self.config: Config = cfg
		self.data_storage: DataStorage = DataStorage()
		self.event_bus = Dispatcher()


class System:
	def __init__(self, env: Env):
		self.__env: Env = env
		self.__tasks: set[asyncio.Task] = set()

	async def start(self) -> 'System':
		pass

	async def update(self, delta_time: float):
		await self._update(delta_time)

	async def _update(self, delta_time: float):
		pass

	def add_task(self, coro: Coroutine[Any, Any, Any]) -> Task:
		task = asyncio.create_task(coro)
		task.add_done_callback(lambda _: self.__tasks.remove(task))
		self.__tasks.add(task)
		return task

	def close(self) -> None:
		for task in self.__tasks:
			task.cancel()

	@property
	def env(self):
		return self.__env

	def __repr__(self):
		return f"System: {self.__class__.__name__}"


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
