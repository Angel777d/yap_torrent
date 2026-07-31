"""Tests for the torrent dates: when it was added, started, finished, last active.

They are wall-clock epoch seconds because they are shown to users and written to disk —
a monotonic stamp would be meaningless in the next process — and they have to come back
through the save file or a restart resets every torrent's history.
"""
import asyncio
import time
from pathlib import Path

from yap_torrent.components.torrent_ec import SaveTorrentEC, TorrentStatsEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.systems import create_torrent_entity, get_torrent_entity
from yap_torrent.systems.local_data_system import LocalDataSystem, _export_torrent_data, _save
from yap_torrent.systems.stats_system import StatsSystem

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


def test_a_new_torrent_is_dated_when_it_is_added(tmp_path):
	before = time.time()
	torrent = _torrent(_env(tmp_path), _meta())
	stats = torrent.get_component(TorrentStatsEC)

	assert before <= stats.added_date <= time.time()
	assert stats.started_date == 0.0
	assert stats.done_date == 0.0


def test_transfer_moves_the_activity_date(tmp_path):
	torrent = _torrent(_env(tmp_path), _meta())
	stats = torrent.get_component(TorrentStatsEC)
	assert stats.activity_date == 0.0

	stats.update_downloaded(1000)
	downloaded_at = stats.activity_date
	assert downloaded_at > 0

	stats.update_uploaded(1000)
	assert stats.activity_date >= downloaded_at


def test_start_and_completion_are_dated_and_marked_for_saving(tmp_path):
	async def run():
		env = _env(tmp_path)
		torrent = _torrent(env, _meta())
		system = StatsSystem(env)
		await system.start()

		await asyncio.gather(*env.event_bus.dispatch("action.torrent.start", torrent))
		stats = torrent.get_component(TorrentStatsEC)
		assert stats.started_date > 0
		assert torrent.has_component(SaveTorrentEC)  # a date only in memory is lost to a crash

		torrent.remove_component(SaveTorrentEC)
		await asyncio.gather(*env.event_bus.dispatch("action.torrent.complete", torrent))
		finished_at = stats.done_date
		assert finished_at > 0

		# re-checking a finished torrent raises the event again; the date must not move
		torrent.remove_component(SaveTorrentEC)
		await asyncio.gather(*env.event_bus.dispatch("action.torrent.complete", torrent))
		assert stats.done_date == finished_at
		assert torrent.has_component(SaveTorrentEC) is False

	asyncio.run(run())


def test_the_dates_come_back_through_the_save_file(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		stats = torrent.get_component(TorrentStatsEC)
		stats.started_date = 111.0
		stats.done_date = 222.0
		stats.activity_date = 333.0
		added = stats.added_date
		_save(tmp_path / "active" / meta.make_info_hash().hex(), _export_torrent_data(env, torrent))

		fresh = _env(tmp_path)
		await LocalDataSystem(fresh).start()

		restored = get_torrent_entity(fresh, meta.make_info_hash()).get_component(TorrentStatsEC)
		assert (restored.added_date, restored.started_date) == (added, 111.0)
		assert (restored.done_date, restored.activity_date) == (222.0, 333.0)

	asyncio.run(run())
