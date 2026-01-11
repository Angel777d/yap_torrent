import asyncio
import logging
from typing import Set

from yap_torrent.env import Env
from yap_torrent.plugins import TorrentPlugin

logger = logging.getLogger(__name__)

# yap_torrent.plugins.ui
class UIPlugin(TorrentPlugin):
	def __init__(self):
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
		return set("ui")

plugin = UIPlugin()

logger.info(f"Torrent App UI plugin imported")
