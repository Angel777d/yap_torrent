"""Tests for torrent labels: setting them, cleaning them, and keeping them.

Labels are the one piece of purely user-authored state on a torrent, so losing them to a
restart loses something nothing else can reconstruct.
"""
import asyncio
from pathlib import Path

from yap_torrent.components.torrent_ec import SaveTorrentEC, TorrentLabelsEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.systems import create_torrent_entity, get_torrent_entity
from yap_torrent.systems.local_data_system import LocalDataSystem, _export_torrent_data, _save
from yap_torrent.systems.torrents_system import TorrentSystem

PIECES = 4


def _env(tmp_path: Path) -> Env:
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	env.config.data_folder = tmp_path
	env.config.active_folder = tmp_path / "active"
	env.config.download_folder = tmp_path / "download"
	env.config.peers_file = str(tmp_path / "peers.dat")
	return env


def _meta() -> Metainfo:
	info = {
		"name": b"t", "piece length": 16384,
		"pieces": b"\x00" * 20 * PIECES, "length": 16384 * PIECES,
	}
	return Metainfo(decode(encode({"info": info})))


def _torrent(env: Env, meta: Metainfo):
	return create_torrent_entity(env, meta.make_info_hash(), env.config.download_folder, {}, meta.info)


async def _system(env: Env):
	system = TorrentSystem(env)
	await system.start()
	return system


def _labels(torrent):
	return torrent.get_component(TorrentLabelsEC).labels


def test_labels_are_set_and_marked_for_saving(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		await _system(env)

		await asyncio.gather(*env.event_bus.dispatch(
			"request.torrent.set_labels", meta.make_info_hash(), ["linux", "iso"]))

		assert _labels(torrent) == ["linux", "iso"]
		assert torrent.has_component(SaveTorrentEC)

	asyncio.run(run())


def test_setting_labels_replaces_the_previous_set(tmp_path):
	# Transmission's torrent-set labels is a replace, not a merge
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		await _system(env)
		info_hash = meta.make_info_hash()

		await asyncio.gather(*env.event_bus.dispatch("request.torrent.set_labels", info_hash, ["a", "b"]))
		await asyncio.gather(*env.event_bus.dispatch("request.torrent.set_labels", info_hash, ["c"]))

		assert _labels(torrent) == ["c"]

	asyncio.run(run())


def test_blanks_and_duplicates_are_dropped_in_order(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		await _system(env)

		await asyncio.gather(*env.event_bus.dispatch(
			"request.torrent.set_labels", meta.make_info_hash(), [" iso ", "", "linux", "iso", "   "]))

		assert _labels(torrent) == ["iso", "linux"]

	asyncio.run(run())


def test_setting_the_same_labels_again_does_not_ask_for_a_save(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		await _system(env)
		info_hash = meta.make_info_hash()

		await asyncio.gather(*env.event_bus.dispatch("request.torrent.set_labels", info_hash, ["a"]))
		torrent.remove_component(SaveTorrentEC)
		await asyncio.gather(*env.event_bus.dispatch("request.torrent.set_labels", info_hash, ["a"]))

		assert torrent.has_component(SaveTorrentEC) is False

	asyncio.run(run())


def test_an_unlabelled_torrent_carries_no_component(tmp_path):
	# absence is the empty case, so nothing has to hold an empty list around
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		await _system(env)

		await asyncio.gather(*env.event_bus.dispatch("request.torrent.set_labels", meta.make_info_hash(), []))

		assert torrent.has_component(TorrentLabelsEC) is False

	asyncio.run(run())


def test_labels_come_back_after_a_restart(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		torrent.add_component(TorrentLabelsEC(["linux", "iso"]))
		_save(tmp_path / "active" / meta.make_info_hash().hex(), _export_torrent_data(env, torrent))

		fresh = _env(tmp_path)
		await LocalDataSystem(fresh).start()

		assert _labels(get_torrent_entity(fresh, meta.make_info_hash())) == ["linux", "iso"]

	asyncio.run(run())
