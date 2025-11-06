from typing import List

from torrent_app import Env, System
from torrent_app.plugins.input.input_system import InputSystem

_systems: List[System] = []


# torrent_app.plugins.input
async def start(env: Env):
	_systems.append(await InputSystem(env).start())


async def update(delta_time: float):
	for system in _systems:
		await system.update(delta_time)


def close():
	for system in _systems:
		system.close()
	_systems.clear()
