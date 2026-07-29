import logging
import os
import pickle
from pathlib import Path
from typing import Any, Dict, Set, Tuple

from angelovich.core.DataStorage import Entity

from yap_torrent.components.file_ec import TorrentFileEC, TorrentFileStateEC, RestoreFileSelectionEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, SaveTorrentEC, ValidateTorrentEC, TorrentPathEC, \
	TorrentLabelsEC, TorrentLimitsEC, TorrentPriorityEC, TorrentStatsEC
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.env import Env
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.system import System
from yap_torrent.systems import iterate_files
from yap_torrent.systems import create_torrent_entity
from yap_torrent.utils import execute_in_pool, write_atomic

logger = logging.getLogger(__name__)


class LocalDataSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)
		self.collection = self.env.data_storage.get_collection(SaveTorrentEC)

	async def start(self):
		self.add_listener("action.torrent.remove", self._on_torrent_remove)

		active_path = Path(self.env.config.active_folder)
		await _load_local(self.env, active_path)

	def close(self):
		to_save = self.env.data_storage.get_collection(TorrentEC).entities
		for torrent_entity in to_save:
			path = _path_from_entity(self.env, torrent_entity)
			save_data = _export_torrent_data(self.env, torrent_entity)
			_save(path, save_data)
		super().close()

	async def _update(self, delta_time: float):
		# save local protocol data
		to_save = self.collection.entities
		for torrent_entity in to_save:
			torrent_entity.remove_component(SaveTorrentEC)
			path = _path_from_entity(self.env, torrent_entity)
			save_data = _export_torrent_data(self.env, torrent_entity)
			self.add_task(execute_in_pool(_save, path, save_data))

	async def _on_torrent_remove(self, info_hash: bytes):
		path = _path_from_info_hash(self.env, info_hash)
		if path.exists():
			os.remove(path)


async def _load_local(env: Env, active_path: Path):
	"""Restore every saved torrent, skipping the ones that cannot be read.

	One unreadable file used to take the whole start with it — and every torrent after it
	in the walk — so a single bad save meant a client that would not come up.
	"""
	for root, dirs, files in os.walk(active_path):
		for file_name in files:
			file_path = Path(root).joinpath(file_name)
			if file_path.suffix == ".tmp":
				continue  # a save that was interrupted; the previous one is still there
			try:
				with open(file_path, 'rb') as f:
					logger.debug(f"Loading save from {file_path}")
					save_data = pickle.load(f)
			except Exception as ex:  # noqa: BLE001
				logger.error("Skipping unreadable torrent save %s: %s", file_path, ex)
				continue
			try:
				_import_torrent_data(env, save_data)
			except Exception as ex:  # noqa: BLE001
				logger.error("Skipping malformed torrent save %s: %s", file_path, ex)


def _path_from_info_hash(env, info_hash: bytes) -> Path:
	active_path = Path(env.config.active_folder)
	return active_path.joinpath(info_hash.hex())


def _path_from_entity(env, torrent_entity: Entity) -> Path:
	info_hash: bytes = torrent_entity.get_component(TorrentEC).info_hash
	return _path_from_info_hash(env, info_hash)


def _save(path: Path, save_data: dict[str, Any]):
	logger.debug(f"Save torrent data: {path}")
	write_atomic(path, pickle.dumps(save_data, pickle.DEFAULT_PROTOCOL))


def _export_torrent_data(env: Env, torrent_entity: Entity) -> dict[str, Any]:
	result: dict[str, Any] = {
		"info_hash": torrent_entity.get_component(TorrentEC).info_hash,
		"path": torrent_entity.get_component(TorrentPathEC).root_path,
		"stats": torrent_entity.get_component(TorrentStatsEC).export(),
	}

	if torrent_entity.has_component(TorrentInfoEC):
		torrent_info = torrent_entity.get_component(TorrentInfoEC).info
		result['torrent_info'] = torrent_info
		result['bitfield'] = torrent_entity.get_component(TorrentEC).bitfield.dump(torrent_info.pieces_num)

	if torrent_entity.has_component(TorrentPriorityEC):
		result['priority'] = torrent_entity.get_component(TorrentPriorityEC).priority

	if torrent_entity.has_component(TorrentLabelsEC):
		result['labels'] = torrent_entity.get_component(TorrentLabelsEC).labels

	if torrent_entity.has_component(TorrentLimitsEC):
		result['limits'] = torrent_entity.get_component(TorrentLimitsEC).export()

	if torrent_entity.has_component(TorrentTrackerEC):
		result['announce_list'] = torrent_entity.get_component(TorrentTrackerEC).announce_list
		result['tracker_data'] = torrent_entity.get_component(TorrentTrackerDataEC).export()

	result['files'] = _export_file_selection(env, torrent_entity)
	result['validate'] = torrent_entity.has_component(ValidateTorrentEC)
	return result


def _export_file_selection(env: Env, torrent_entity: Entity) -> Dict[int, Tuple[bool, int]]:
	if torrent_entity.has_component(RestoreFileSelectionEC):
		return torrent_entity.get_component(RestoreFileSelectionEC).selection

	info_hash = torrent_entity.get_component(TorrentEC).info_hash
	selection: Dict[int, Tuple[bool, int]] = {}
	for file_entity in iterate_files(env, info_hash):
		file_ec = file_entity.get_component(TorrentFileEC)
		state = file_entity.get_component(TorrentFileStateEC)
		selection[file_ec.index] = (state.wanted, state.priority.value)

	return selection


def _import_torrent_data(env, save_data: dict[str, Any]):
	# create the basic torrent entity
	info_hash: bytes = save_data.get('info_hash')
	path: Path = Path(save_data.get('path'))
	torrent_info = save_data.get('torrent_info', None)
	stats = save_data.get('stats', {})
	torrent_entity = create_torrent_entity(env, info_hash, path, stats, torrent_info)

	# update bitfield
	bitfield = save_data.get('bitfield', bytes())
	torrent_entity.get_component(TorrentEC).bitfield.update(bitfield)

	priority = save_data.get('priority')
	if priority is not None:
		torrent_entity.add_component(TorrentPriorityEC(int(priority)))

	labels = save_data.get('labels')
	if labels:
		torrent_entity.add_component(TorrentLabelsEC(labels))

	limits = save_data.get('limits')
	if limits:
		torrent_entity.add_component(TorrentLimitsEC(**limits))

	# peers are persisted separately by PeerDataSystem (global store)

	# update tracker data if any
	if 'announce_list' in save_data:
		announce_list = save_data.get('announce_list', [])
		torrent_entity.add_component(TorrentTrackerEC(announce_list))
		tracker_data: Dict[str, Any] = save_data.get('tracker_data', {})
		torrent_entity.add_component(TorrentTrackerDataEC(**tracker_data))

	# restore per-file selection; FileSystem applies it when file entities materialize
	torrent_entity.add_component(RestoreFileSelectionEC(save_data.get('files', {})))

	# update validate option
	validate = save_data.get('validate', False)
	if validate:
		env.event_bus.dispatch("request.torrent.invalidate", info_hash)
