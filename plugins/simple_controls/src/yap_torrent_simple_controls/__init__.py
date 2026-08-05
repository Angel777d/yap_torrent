import asyncio
import logging
from typing import Set

from angelovich.core.Plugin import Plugin

from yap_torrent.env import Env
from .screens import root


# yap_torrent.plugins.simple_controls
class SimpleControlsPlugin(Plugin):
	async def start(self, env: Env):
		loop = asyncio.get_running_loop()
		root(env, loop)

	def close(self):
		pass

	@staticmethod
	def get_purpose() -> Set[str]:
		return {"ui", }


logger = logging.getLogger(__name__)
plugin = SimpleControlsPlugin()
logger.info(f"YAP SimpleControls plugin imported")
