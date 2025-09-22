import asyncio
import logging
import time
from typing import List

from torrent_app import Env, System, Config, upnp
from torrent_app.systems.announce_system import AnnounceSystem
from torrent_app.systems.bt_main_system import BTMainSystem
from torrent_app.systems.bt_dht_system import BTDHTSystem
from torrent_app.systems.bt_extension_system import BTExtensionSystem
from torrent_app.systems.input_system import InputSystem
from torrent_app.systems.bt_ext_metadata_system import BTExtMetadataSystem
from torrent_app.systems.peer_system import PeerSystem
from torrent_app.systems.piece_system import PieceSystem
from torrent_app.systems.watch_system import WatcherSystem

GLOBAL_TICK_TIME = 1


def network_setup() -> tuple[str, str]:
	return upnp.get_my_ip(), upnp.get_my_ext_ip()


def open_port(ip: str, port: int, dht_port: int):
	service = upnp.discover(ip)
	if service:
		open_res = upnp.open_port(service, port, ip, protocol="TCP")
		print(f"open TCP port: {open_res}")

		open_res = upnp.open_port(service, dht_port, ip, protocol="UDP")
		print(f"open UDP port: {open_res}")


def create_peer_id():
	# TODO: generate and/or save peer id
	return b'-PY0001-111111111111'


class Application:
	def __init__(self):
		config = Config()
		ip, external_ip = network_setup()
		open_port(ip, config.port, config.dht_port)

		self.env = Env(create_peer_id(), ip, external_ip, config)
		self.systems: List[System] = []

	async def run(self, close_event: asyncio.Event):
		env = self.env
		logging.info("Start torrent app")

		self.systems = [
			await WatcherSystem(env).start(),
			await AnnounceSystem(env).start(),
			await PeerSystem(env).start(),
			await PieceSystem(env).start(),
			await BTMainSystem(env).start(),
			await BTExtensionSystem(env).start(),
			await BTExtMetadataSystem(env).start(),
			await BTDHTSystem(env).start(),
			await InputSystem(env).start(),
			# await ProfileSystem(env).start(),
		]

		last_time = time.monotonic()

		while not close_event.is_set():
			await asyncio.sleep(GLOBAL_TICK_TIME)

			current_time = time.monotonic()
			dt = current_time - last_time
			last_time = current_time

			for system in self.systems:
				await system.update(dt)

	def stop(self):
		logging.info("Close torrent app")
		for system in self.systems:
			system.close()
