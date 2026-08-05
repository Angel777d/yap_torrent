import logging
from typing import List, Set

from angelovich.core.Plugin import Plugin
from angelovich.core.System import System

from yap_torrent.env import Env
from .system import UISystem

logger = logging.getLogger(__name__)


# yap_torrent.plugins.ui
class UIPlugin(Plugin):
	def get_systems(self, env: Env) -> List[System]:
		return [UISystem(env)]

	@staticmethod
	def get_purpose() -> Set[str]:
		return {"ui", }


plugin = UIPlugin()

logger.info(f"Torrent App UI plugin imported")
