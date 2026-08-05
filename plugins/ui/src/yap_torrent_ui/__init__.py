import asyncio
import logging
from typing import Set

from angelovich.core.Plugin import Plugin

from yap_torrent.env import Env

logger = logging.getLogger(__name__)


# yap_torrent.plugins.ui
class UIPlugin(Plugin):
	def __init__(self):
		super().__init__()
		self._app = None

	async def start(self, env: Env):
		from .ui_app import TorrentUIApp
		self._app = TorrentUIApp(env)
		asyncio.create_task(self._app.run_async(
			headless=False,
			inline=False,
			inline_no_clear=False,
			mouse=True,
			size=None,
			auto_pilot=None
		))

	def close(self):
		self._app.stop()

	@staticmethod
	def get_purpose() -> Set[str]:
		return {"ui", }


plugin = UIPlugin()

logger.info(f"Torrent App UI plugin imported")
