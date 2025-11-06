import logging
from asyncio import AbstractEventLoop
from typing import List

from torrent_app import Env, System

logger = logging.getLogger(__name__)


# torrent_app.plugins.torrent_ui
def init_plugin(loop: AbstractEventLoop, env: Env) -> List[System]:
	return []


logger.info(f"Torrent App UI plugin imported")
