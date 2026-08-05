import logging
from typing import List, Set

from angelovich.core.Plugin import Plugin
from angelovich.core.System import System

from yap_torrent.env import Env
from .system import TransmissionRpcSystem


class TransmissionRpcPlugin(Plugin):
	def get_systems(self, env: Env) -> List[System]:
		return [TransmissionRpcSystem(env, self.name)]

	@staticmethod
	def get_purpose() -> Set[str]:
		# "web" as well as its own purpose: this plugin now serves the browser UI beside
		# the RPC, so a still-installed yap_torrent_web is skipped rather than racing it
		# for a port. It remains compatible with the TUI/console plugins.
		return {"transmission-rpc", "web"}


logger = logging.getLogger(__name__)
plugin = TransmissionRpcPlugin()
logger.info("YAP Transmission RPC plugin imported")
