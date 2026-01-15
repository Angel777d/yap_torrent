import asyncio
import logging
import os
import pickle
from pathlib import Path
from typing import Any, Dict, Set

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import KnownPeersEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, SaveTorrentEC, ValidateTorrentEC, \
	TorrentPathEC, TorrentStatsEC
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.env import Env
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.system import System
from yap_torrent.systems import create_torrent_entity

logger = logging.getLogger(__name__)


class LocalDataSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)
		self.collection = self.env.data_storage.get_collection(SaveTorrentEC)

	async def start(self):
		self.env.event_bus.add_listener("action.torrent.remove", self._on_torrent_remove, scope=self)

		active_path = Path(self.env.config.active_folder)
		self.add_task(_load_local(self.env, active_path))

	def close(self):
		to_save = self.env.data_storage.get_collection(TorrentEC).entities
		for torrent_entity in to_save:
			path = _path_from_entity(self.env, torrent_entity)
			save_data = _export_torrent_data(torrent_entity)
			_save(path, save_data)
		super().close()

	async def _update(self, delta_time: float):
		# save local protocol data
		to_save = self.collection.entities
		for entity in to_save:
			entity.remove_component(SaveTorrentEC)
			self.add_task(_save_local(self.env, entity))

	async def _on_torrent_remove(self, info_hash: bytes):
		path = _path_from_info_hash(self.env, info_hash)
		if path.exists():
			os.remove(path)


async def _load_local(env: Env, active_path: Path):
	for root, dirs, files in os.walk(active_path):
		for file_name in files:
			file_path = Path(root).joinpath(file_name)
			with open(file_path, 'rb') as f:
				logger.debug(f"Loading save from {file_path}")
				save_data = pickle.load(f)
				_import_torrent_data(env, save_data)


async def _save_local(env: Env, torrent_entity: Entity):
	loop = asyncio.get_running_loop()
	path = _path_from_entity(env, torrent_entity)
	save_data = _export_torrent_data(torrent_entity)
	await loop.run_in_executor(None, _save, path, save_data)


def _path_from_info_hash(env, info_hash: bytes) -> Path:
	active_path = Path(env.config.active_folder)
	return active_path.joinpath(info_hash.hex())


def _path_from_entity(env, torrent_entity: Entity) -> Path:
	info_hash: bytes = torrent_entity.get_component(TorrentEC).info_hash
	return _path_from_info_hash(env, info_hash)


def _save(path: Path, save_data: dict[str, Any]):
	logger.debug(f"Save torrent data: {path}")
	path.parent.mkdir(parents=True, exist_ok=True)
	with open(path, 'wb') as f:
		pickle.dump(save_data, f, pickle.DEFAULT_PROTOCOL)


def _export_torrent_data(torrent_entity: Entity) -> dict[str, Any]:
	result: dict[str, Any] = {
		"info_hash": torrent_entity.get_component(TorrentEC).info_hash,
		"peers": torrent_entity.get_component(KnownPeersEC).peers,
		"path": torrent_entity.get_component(TorrentPathEC).root_path,
		"stats": torrent_entity.get_component(TorrentStatsEC).export(),
	}

	if torrent_entity.has_component(TorrentInfoEC):
		torrent_info = torrent_entity.get_component(TorrentInfoEC).info
		result['torrent_info'] = torrent_info
		result['bitfield'] = torrent_entity.get_component(TorrentEC).bitfield.dump(torrent_info.pieces_num)

	if torrent_entity.has_component(TorrentTrackerEC):
		result['announce_list'] = torrent_entity.get_component(TorrentTrackerEC).announce_list
		result['tracker_data'] = torrent_entity.get_component(TorrentTrackerDataEC).export()
	result['validate'] = torrent_entity.has_component(ValidateTorrentEC)
	return result


def _import_torrent_data(env, save_data: dict[str, Any]):
	# create the basic torrent entity
	info_hash = save_data.get('info_hash')
	path = save_data.get('path', Path(env.config.download_folder))
	torrent_info = save_data.get('torrent_info', None)
	stats = save_data.get('stats', {})
	torrent_entity = create_torrent_entity(env, info_hash, path, stats, torrent_info)

	# update bitfield
	bitfield = save_data.get('bitfield', bytes())
	torrent_entity.get_component(TorrentEC).bitfield.update(bitfield)

	# update peers
	peers: Set[PeerInfo] = save_data.get('peers', {})
	torrent_entity.get_component(KnownPeersEC).update_peers(peers)

	# update tracker data if any
	if 'announce_list' in save_data:
		announce_list = save_data.get('announce_list', [])
		torrent_entity.add_component(TorrentTrackerEC(announce_list))
		tracker_data: Dict[str, Any] = save_data.get('tracker_data', {})
		torrent_entity.add_component(TorrentTrackerDataEC(**tracker_data))

	# update validate option
	validate = save_data.get('validate', False)
	if validate:
		env.event_bus.dispatch("request.torrent.invalidate", info_hash)
