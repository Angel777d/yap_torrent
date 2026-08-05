import logging
from typing import Set, List

from angelovich.core.Plugin import Plugin

from yap_torrent.env import Env
from .server import RpcServer


class TransmissionRpcPlugin(Plugin):
	def __init__(self):
		super().__init__()
		self.servers: List[RpcServer] = []

	async def start(self, env: Env):
		self.servers.append(await RpcServer(self.name, env).start())

	async def stop(self):
		for s in self.servers:
			await s.stop()

	def close(self):
		self.servers.clear()

	@staticmethod
	def get_purpose() -> Set[str]:
		# "web" as well as its own purpose: this plugin now serves the browser UI beside
		# the RPC, so a still-installed yap_torrent_web is skipped rather than racing it
		# for a port. It remains compatible with the TUI/console plugins.
		return {"transmission-rpc", "web"}


logger = logging.getLogger(__name__)
plugin = TransmissionRpcPlugin()
logger.info("YAP Transmission RPC plugin imported")
