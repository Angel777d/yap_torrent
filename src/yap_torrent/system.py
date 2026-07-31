import asyncio
from typing import Coroutine, Any, TypeVar, Callable, Optional

from _asyncio import Task
from angelovich.core.Dispatcher import CallbackType as DispatcherCallbackType

from yap_torrent.env import Env

_T = TypeVar("_T")


class System:
	def __init__(self, env: Env):
		self.__env: Env = env
		self.__tasks: set[asyncio.Task] = set()

	async def start(self):
		pass

	async def stop(self):
		self.env.event_bus.remove_all_listeners(scope=self)

	async def update(self, delta_time: float):
		await self._update(delta_time)

	async def _update(self, delta_time: float):
		pass

	def add_task(self,
	             coro: Coroutine[Any, Any, _T],
	             callback: Optional[Callable[[_T], None]] = None,
	             name: Optional[str] = None) -> Task:

		def done(_task: Task[_T]):
			if callback:
				callback(_task)
			self.__tasks.remove(_task)

		task = asyncio.create_task(coro, name=name)
		task.add_done_callback(done)
		self.__tasks.add(task)
		return task

	def add_listener(self, event: str, callback: DispatcherCallbackType):
		self.env.event_bus.add_listener(event, callback, scope=self)

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
