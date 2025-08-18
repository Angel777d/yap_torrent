import asyncio
import time
from typing import List

import upnp
from app import Env, System
from app.announce_system import AnnounceSystem
from app.config import Config
from app.peer_system import PeerSystem
from app.piece_system import PieceSystem
from app.watch_system import WatcherSystem

GLOBAL_TICK_TIME = 1


def network_setup(port: int) -> tuple[str, str]:
	ip: str = upnp.get_my_ip()
	service = upnp.discover(ip)
	if service:
		open_res = upnp.open_port(service, port, ip)
		print(f"open port: {open_res}")
	return ip, upnp.get_my_ext_ip()


def create_peer_id():
	return b'-PY0001-111111111111'


async def main():
	# loop = asyncio.get_event_loop()
	# loop.set_debug(True)

	print("start")

	config = Config()
	_, external_ip = network_setup(config.port)
	env = Env(create_peer_id(), external_ip, config)

	systems: List[System] = [
		await WatcherSystem(env).start(),
		await AnnounceSystem(env).start(),
		await PeerSystem(env).start(),
		await PieceSystem(env).start(),
	]

	last_time = time.time()

	while True:

		await asyncio.sleep(GLOBAL_TICK_TIME)
		current_time = time.time()
		dt = current_time - last_time
		last_time = current_time

		for system in systems:
			await system.update(dt)


if __name__ == '__main__':
	asyncio.run(main())
