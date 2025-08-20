import asyncio
import time
from typing import List

from app import Env, System
from app.announce_system import AnnounceSystem
from app.peer_system import PeerSystem
from app.piece_system import PieceSystem
from app.profile_system import ProfileSystem
from app.watch_system import WatcherSystem

GLOBAL_TICK_TIME = 1


class Application:
	def __init__(self, env: Env):
		self.env = env
		self.systems: List[System] = []

	async def run(self):
		env = self.env
		self.systems = [
			await WatcherSystem(env).start(),
			await AnnounceSystem(env).start(),
			await PeerSystem(env).start(),
			await PieceSystem(env).start(),
			await ProfileSystem(env).start(),
		]

		last_time = time.time()

		while True:
			await asyncio.sleep(GLOBAL_TICK_TIME)

			current_time = time.time()
			dt = current_time - last_time
			last_time = current_time

			for system in self.systems:
				await system.update(dt)

	def close(self):
		for system in self.systems:
			system.close()
