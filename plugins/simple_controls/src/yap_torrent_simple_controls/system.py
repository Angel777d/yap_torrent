import asyncio

from angelovich.core.System import System

from .screens import root


class SimpleControlsSystem(System):
	async def start(self):
		loop = asyncio.get_running_loop()
		root(self.env, loop)
