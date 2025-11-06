import logging
from typing import List

from torrent_app import Env, System
from .profile_system import ProfileSystem

logger = logging.getLogger(__name__)

_systems: List[System] = []


# torrent_app.plugins.profile
async def start(env: Env):
	_systems.append(await ProfileSystem(env).start())


async def update(delta_time: float):
	for system in _systems:
		await system.update(delta_time)


def close():
	for system in _systems:
		system.close()
	_systems.clear()


logger.info(f"Profile plugin imported")
