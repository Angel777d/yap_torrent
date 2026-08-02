from typing import Set

from yap_torrent.env import Env


class TorrentPlugin:
	def __init__(self):
		self.name = ""

	def set_name(self, name: str):
		self.name = name

	async def start(self, env: Env):
		raise NotImplementedError

	async def update(self, delta_time: float):
		pass

	async def stop(self):
		pass

	def close(self):
		pass

	@staticmethod
	def get_purpose() -> Set[str]:
		return set()
