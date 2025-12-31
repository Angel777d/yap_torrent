import asyncio
import logging
import time
from typing import List

import torrent_app.plugins as plugins
from torrent_app import Env, System, Config, upnp
from torrent_app.plugins import TorrentPlugin
from torrent_app.systems.announce_system import AnnounceSystem
from torrent_app.systems.bt_choke_system import BTChokeSystem
from torrent_app.systems.bt_dht_system import BTDHTSystem
from torrent_app.systems.bt_download_system import BTDownloadSystem
from torrent_app.systems.bt_ext_metadata_system import BTExtMetadataSystem
from torrent_app.systems.bt_extension_system import BTExtensionSystem
from torrent_app.systems.bt_intrest_system import BTInterestedSystem
from torrent_app.systems.bt_local_data_system import LocalDataSystem
from torrent_app.systems.bt_magnet_system import MagnetSystem
from torrent_app.systems.bt_upload_system import BTUploadSystem
from torrent_app.systems.bt_validation_system import ValidationSystem
from torrent_app.systems.peer_system import PeerSystem
from torrent_app.systems.piece_system import PieceSystem
from torrent_app.systems.torrents_system import TorrentSystem
from torrent_app.systems.watch_system import WatcherSystem

logger = logging.getLogger(__name__)

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

		env = Env(create_peer_id(), ip, external_ip, config)
		self.systems: List[System] = [
			PeerSystem(env),
			BTChokeSystem(env),
			BTInterestedSystem(env),
			BTDownloadSystem(env),
			BTUploadSystem(env),
			PieceSystem(env),
			ValidationSystem(env),
			BTExtensionSystem(env),
			BTExtMetadataSystem(env),
			BTDHTSystem(env),
			MagnetSystem(env),
			AnnounceSystem(env),
			TorrentSystem(env),
			LocalDataSystem(env),
			WatcherSystem(env),
		]

		self.plugins: List[TorrentPlugin] = plugins.discover_plugins(env.config)

		self.env = env

	async def run(self, close_event: asyncio.Event):
		env = self.env
		env.close_event = close_event

		logger.info("Torrent application start")

		for system in self.systems:
			logger.debug(f"start system {system}")
			await system.start()

		for plugin in self.plugins:
			logger.debug(f"start plugin {plugin}")
			await plugin.start(env)

		logger.info("Torrent application initialized")

		last_time = time.monotonic()
		while not close_event.is_set():
			current_time = time.monotonic()
			dt = current_time - last_time
			last_time = current_time

			try:
				for system in self.systems:
					await system.update(dt)
			except Exception as e:
				logger.error("unexpected exception on systems update")
				logger.error(e)

			try:
				for plugin in self.plugins:
					await plugin.update(dt)
			except Exception as e:
				logger.error("unexpected exception on plugins update")
				logger.error(e)

			await asyncio.sleep(GLOBAL_TICK_TIME)

		logger.info("Torrent application stop")
		self.stop()

		logger.info("Torrent application closed")

		await asyncio.sleep(0)

	# leftovers = asyncio.all_tasks()
	# print(leftovers)

	def stop(self):
		for system in self.systems:
			system.close()

		for plugin in self.plugins:
			plugin.close()
