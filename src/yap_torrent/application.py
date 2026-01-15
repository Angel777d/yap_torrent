import asyncio
import logging
import time
from typing import List

import yap_torrent.plugins as plugins
from yap_torrent import upnp
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.plugins import TorrentPlugin
from yap_torrent.system import System
from yap_torrent.systems.announce_system import AnnounceSystem
from yap_torrent.systems.bt_choke_system import BTChokeSystem
from yap_torrent.systems.bt_dht_system import BTDHTSystem
from yap_torrent.systems.bt_download_system import BTDownloadSystem
from yap_torrent.systems.bt_ext_metadata_system import BTExtMetadataSystem
from yap_torrent.systems.bt_extension_system import BTExtensionSystem
from yap_torrent.systems.bt_intrest_system import BTInterestedSystem
from yap_torrent.systems.bt_local_data_system import LocalDataSystem
from yap_torrent.systems.bt_magnet_system import MagnetSystem
from yap_torrent.systems.bt_upload_system import BTUploadSystem
from yap_torrent.systems.bt_validation_system import ValidationSystem
from yap_torrent.systems.peer_system import PeerSystem
from yap_torrent.systems.piece_system import PieceSystem
from yap_torrent.systems.torrents_system import TorrentSystem
from yap_torrent.systems.watch_system import WatcherSystem

logger = logging.getLogger(__name__)

GLOBAL_TICK_TIME = 1


def network_setup() -> tuple[str, str]:
	return upnp.get_my_ip(), upnp.get_my_ext_ip()


def open_port(ip: str, port: int, dht_port: int):
	service = upnp.discover(ip)
	if service:
		open_res = upnp.open_port(service, port, ip, protocol="TCP")
		logger.info(f"open TCP port: {open_res}")

		open_res = upnp.open_port(service, dht_port, ip, protocol="UDP")
		logger.info(f"open UDP port: {open_res}")


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
			except Exception as ex:
				logger.error("unexpected exception on systems update: %s", ex, exc_info=True)

			try:
				for plugin in self.plugins:
					await plugin.update(dt)
			except Exception as ex:
				logger.error("unexpected exception on plugins update: %s", ex, exc_info=True)

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
