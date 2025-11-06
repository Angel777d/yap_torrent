from asyncio import AbstractEventLoop
from typing import List

from torrent_app import Env, System
from torrent_app.plugins.input.input_system import InputSystem


# torrent_app.plugins.input
def init_plugin(loop: AbstractEventLoop, env: Env) -> List[System]:
	return [InputSystem(env)]
