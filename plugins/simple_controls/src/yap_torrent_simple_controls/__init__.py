import logging
from typing import List, Set

from angelovich.core.Plugin import Plugin
from angelovich.core.System import System

from yap_torrent.env import Env
from .system import SimpleControlsSystem


# yap_torrent.plugins.simple_controls
class SimpleControlsPlugin(Plugin):
	def get_systems(self, env: Env) -> List[System]:
		return [SimpleControlsSystem(env)]

	@staticmethod
	def get_purpose() -> Set[str]:
		return {"ui", }


logger = logging.getLogger(__name__)
plugin = SimpleControlsPlugin()
logger.info(f"YAP SimpleControls plugin imported")
