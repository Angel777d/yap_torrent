import logging
from typing import Optional, Set

from yap_torrent.env import Env
from yap_torrent.plugins import TorrentPlugin

from .server import RpcServer


class TransmissionRpcPlugin(TorrentPlugin):
	def __init__(self):
		self.server: Optional[RpcServer] = None

	async def start(self, env: Env):
		self.server = RpcServer(env)
		await self.server.start()

	async def stop(self):
		if self.server:
			await self.server.stop()

	def close(self):
		if self.server:
			self.server.close()

	@staticmethod
	def get_purpose() -> Set[str]:
		# Unique purpose so it never conflicts with the web/ui plugins and can
		# run alongside them.
		return {"transmission-rpc"}


logger = logging.getLogger(__name__)
plugin = TransmissionRpcPlugin()
logger.info("YAP Transmission RPC plugin imported")
