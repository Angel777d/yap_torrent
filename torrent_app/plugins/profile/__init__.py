import logging
from asyncio import AbstractEventLoop
from typing import List

from torrent_app import Env, System
from .profile_system import ProfileSystem

logger = logging.getLogger(__name__)


# torrent_app.plugins.profile
def init_plugin(loop: AbstractEventLoop, env: Env) -> List[System]:
	return [ProfileSystem(env)]


logger.info(f"Profile plugin imported")
