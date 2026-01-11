import asyncio
import logging
from typing import Set

from yap_torrent.env import Env
from yap_torrent.plugins import TorrentPlugin
from .screens import root


# yap_torrent.plugins.simple_controls
class SimpleControlsPlugin(TorrentPlugin):
	async def start(self, env: Env):
		loop = asyncio.get_running_loop()
		root(env, loop)

	def close(self):
		pass

	@staticmethod
	def get_purpose() -> Set[str]:
		return set("ui")


logger = logging.getLogger(__name__)
plugin = SimpleControlsPlugin()
logger.info(f"YAP SimpleControls plugin imported")
