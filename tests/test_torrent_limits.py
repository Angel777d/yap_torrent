"""Tests for per-torrent limits: stored, persisted, enforced by nothing.

Nothing in the transfer path reads these. They are kept so a client's choice round-trips
instead of silently reading back as zero, which is why these tests are about storage and
survival rather than about behaviour.
"""
import asyncio
from pathlib import Path

from yap_torrent.components.torrent_ec import SaveTorrentEC, TorrentLimitsEC
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


def test_limits_are_stored_and_survive_a_restart(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		await _system(env)

		await asyncio.gather(*env.event_bus.dispatch(
			"request.torrent.set_limits", meta.make_info_hash(),
			{"upload_limit": 50, "upload_limited": True, "seed_ratio_limit": 1.5}))

		limits = torrent.get_component(TorrentLimitsEC)
		assert (limits.upload_limit, limits.upload_limited, limits.seed_ratio_limit) == (50, True, 1.5)
		assert torrent.has_component(SaveTorrentEC)

		_save(tmp_path / "active" / meta.make_info_hash().hex(), _export_torrent_data(env, torrent))
		fresh = _env(tmp_path)
		await LocalDataSystem(fresh).start()

		restored = get_torrent_entity(fresh, meta.make_info_hash()).get_component(TorrentLimitsEC)
		assert (restored.upload_limit, restored.upload_limited, restored.seed_ratio_limit) == (50, True, 1.5)

	asyncio.run(run())


def test_unknown_limit_keys_are_ignored(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		await _system(env)

		await asyncio.gather(*env.event_bus.dispatch(
			"request.torrent.set_limits", meta.make_info_hash(), {"not_a_limit": 1}))

		assert torrent.has_component(TorrentLimitsEC) is False

	asyncio.run(run())
