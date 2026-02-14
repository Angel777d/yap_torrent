import logging
from typing import Set

from yap_torrent.env import Env
from yap_torrent.plugins import TorrentPlugin
from .server import WebServer


class WebInterfacePlugin(TorrentPlugin):
	def __init__(self):
		self.server: WebServer | None = None

	async def start(self, env: Env):
		self.server = WebServer(env)
		await self.server.start()

	async def stop(self):
		if self.server:
			await self.server.stop()

	def close(self):
		if self.server:
			self.server.close()

	@staticmethod
	def get_purpose() -> Set[str]:
		return {"web", }


logger = logging.getLogger(__name__)
plugin = WebInterfacePlugin()
logger.info("YAP Web Interface plugin imported")
