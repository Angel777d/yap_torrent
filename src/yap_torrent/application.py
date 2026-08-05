import asyncio
import logging
import signal
import time
from typing import List

from angelovich.core.Plugin import discover_plugins
from angelovich.core.System import System

from yap_torrent import upnp
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.systems.announce_system import AnnounceSystem
from yap_torrent.systems.choke_system import ChokeSystem
from yap_torrent.systems.dht_system import DHTSystem
from yap_torrent.systems.download_system import DownloadSystem
from yap_torrent.systems.ext_metadata_system import ExtMetadataSystem
from yap_torrent.systems.extension_system import ExtensionSystem
from yap_torrent.systems.file_system import FileSystem
from yap_torrent.systems.intrest_system import InterestedSystem
from yap_torrent.systems.local_data_system import LocalDataSystem
from yap_torrent.systems.magnet_system import MagnetSystem
from yap_torrent.systems.metainfo_system import MetainfoSystem
from yap_torrent.systems.peer_data_system import PeerDataSystem
from yap_torrent.systems.peer_system import PeerSystem
from yap_torrent.systems.piece_system import PieceSystem
from yap_torrent.systems.settings_system import SettingsSystem
from yap_torrent.systems.stats_system import StatsSystem
from yap_torrent.systems.torrents_system import TorrentSystem
from yap_torrent.systems.upload_system import UploadSystem
from yap_torrent.systems.validation_system import ValidationSystem
from yap_torrent.systems.watch_system import WatcherSystem

logger = logging.getLogger(__name__)

GLOBAL_TICK_TIME = 1
PLUGINS_GROUP = "yap_torrent.plugins"


def network_setup() -> tuple[str, str]:
	return upnp.get_my_ip(), upnp.get_my_ext_ip()


def open_port(ip: str, port: int, dht_port: int):
	service = upnp.discover(ip)
	if service:
		open_res = upnp.open_port(service, port, ip, protocol="TCP")
		logger.info(f"open TCP port: {open_res}")

		open_res = upnp.open_port(service, dht_port, ip, protocol="UDP")
		logger.info(f"open UDP port: {open_res}")


class Application:
	def __init__(self, config: Config):
		ip, external_ip = network_setup()
		open_port(ip, config.port, config.dht_port)

		env = Env(config.peer_id, ip, external_ip, config)
		print(
			f"peer_id:{env.peer_id}, ip: {env.ip}, ext: {env.external_ip}, port: {env.config.port}, dht_port: {env.config.dht_port}")
		self.systems: List[System] = [
			# first: plugins register against it at start-up, after every system is up
			SettingsSystem(env),
			MetainfoSystem(env),
			FileSystem(env),
			PeerSystem(env),
			ChokeSystem(env),
			InterestedSystem(env),
			DownloadSystem(env),
			UploadSystem(env),
			PieceSystem(env),
			# after the transfer systems, so a sample reads the bytes this tick moved
			StatsSystem(env),
			ValidationSystem(env),
			ExtensionSystem(env),
			ExtMetadataSystem(env),
			DHTSystem(env),
			MagnetSystem(env),
			TorrentSystem(env),
			LocalDataSystem(env),
			PeerDataSystem(env),
			WatcherSystem(env),
			AnnounceSystem(env),
		]

		for plugin in discover_plugins(PLUGINS_GROUP, config.disabled_plugins):
			self.systems.extend(plugin.get_systems(env))

		self.env = env

	def shutdown(self, sig: signal.Signals) -> None:
		logger.info("Shutting down on %s", getattr(sig, "name", sig))
		if self.env.close_event:
			self.env.close_event.set()

	def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
		# SIGBREAK is Windows' Ctrl+Break — the other way a console app is asked to stop
		# there, and the only one a caller can deliver to a process it did not share a
		# console with. SIGHUP is POSIX-only. Each is skipped where it does not exist.
		for name in ("SIGHUP", "SIGBREAK", "SIGTERM", "SIGINT"):
			sig = getattr(signal, name, None)
			if sig is None:
				continue
			try:
				loop.add_signal_handler(sig, self.shutdown, sig)
			except NotImplementedError:
				try:
					signal.signal(sig, lambda signum, frame, _sig=sig: self.shutdown(_sig))
				except (ValueError, OSError) as ex:
					logger.debug("Cannot install a handler for %s: %s", name, ex)

	async def run(self, close_event: asyncio.Event):
		self._install_signal_handlers(asyncio.get_running_loop())

		env = self.env
		env.close_event = close_event

		logger.info("Torrent application start")

		for system in self.systems:
			logger.debug(f"start system {system}")
			await system.start()

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

			await asyncio.sleep(GLOBAL_TICK_TIME)

		logger.info("Torrent application stop")

		# async stop
		for system in self.systems:
			await system.stop()

		self.close()

		logger.info("Torrent application closed")

		await asyncio.sleep(0)

		# leftovers = asyncio.all_tasks()
		# print(leftovers)
		pass

	def close(self):
		# lock close
		for system in self.systems:
			system.close()
