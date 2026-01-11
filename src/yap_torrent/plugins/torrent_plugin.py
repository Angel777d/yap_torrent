from typing import Set

from yap_torrent.env import Env


class TorrentPlugin:
	async def start(self, env: Env):
		raise NotImplementedError

	async def update(self, delta_time: float):
		pass

	def close(self):
		pass

	@staticmethod
	def get_purpose() -> Set[str]:
		return set()
