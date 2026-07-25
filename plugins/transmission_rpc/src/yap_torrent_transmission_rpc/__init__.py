import logging
from typing import Set, List

from yap_torrent.env import Env
from yap_torrent.plugins import TorrentPlugin
from .server import RpcServer


class TransmissionRpcPlugin(TorrentPlugin):
	def __init__(self):
		self.servers: List[RpcServer] = []

	async def start(self, env: Env):
		self.servers.append(await RpcServer(env).start())

	async def stop(self):
		for s in self.servers:
			await s.stop()

	def close(self):
		self.servers.clear()

	@staticmethod
	def get_purpose() -> Set[str]:
		# Unique purpose so it never conflicts with the web/ui plugins and can
		# run alongside them.
		return {"transmission-rpc"}


logger = logging.getLogger(__name__)
plugin = TransmissionRpcPlugin()
logger.info("YAP Transmission RPC plugin imported")
