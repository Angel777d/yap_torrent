import asyncio
from typing import Coroutine, Any

from _asyncio import Task

from yap_torrent.env import Env


class System:
	def __init__(self, env: Env):
		self.__env: Env = env
		self.__tasks: set[asyncio.Task] = set()

	async def start(self):
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
