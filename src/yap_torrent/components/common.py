import time

from angelovich.core.DataStorage import EntityComponent


class IdleEC(EntityComponent):
	def __init__(self) -> None:
		super().__init__()
		self.__last_update: float = time.monotonic()

	def touch(self):
		self.__last_update = time.monotonic()

	def overlives_period(self, ttl: float) -> bool:
		return time.monotonic() - self.__last_update > ttl

	@property
	def last_update(self) -> float:
		return self.__last_update
