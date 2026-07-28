"""Tests for the download-queue active window (Part A, event-driven).

TorrentSystem attaches TorrentDownloadProgressEC + TorrentPriorityEC to every
torrent that gains metadata and keeps the top max_active_downloads (by ascending
priority) marked with ActiveTorrentEC, reacting to add / start / stop / complete.
The async flow is driven with asyncio.run() inside plain sync tests.
"""
import asyncio
from pathlib import Path

from yap_torrent.components.torrent_ec import (
	ActiveTorrentEC,
	TorrentDownloadProgressEC,
	TorrentEC,
	TorrentInfoEC,
	TorrentPriorityEC,
	TorrentState,
	TorrentStatsEC,
	ValidateTorrentEC,
)
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.systems import compute_wanted_bitfield, create_torrent_entity, get_info_hash
from yap_torrent.systems.torrents_system import TorrentSystem, iterate_torrents_to_download


def _env(limit: int = 2) -> Env:
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	env.config.max_active_downloads = limit
	return env


def _make(env: Env, name: str, npieces: int = 2, complete: bool = False):
	info = {"name": name.encode(), "piece length": 16384, "pieces": b"\x00" * 20 * npieces, "length": 16384 * npieces}
	meta = Metainfo(decode(encode({"info": info})))
	entity = create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)
	if complete:
		bitfield = entity.get_component(TorrentEC).bitfield
		for i in range(npieces):
			bitfield.set_index(i)
	# FileSystem attaches this on metadata-add; mirror it so is_torrent_complete works.
	entity.add_component(TorrentDownloadProgressEC(compute_wanted_bitfield(env, get_info_hash(entity), meta.info)))
	return entity


def _active(*torrents):
	return [t for t in torrents if t.has_component(ActiveTorrentEC)]


# --- iterate_torrents_to_download (the eligibility filter) ------------------
def test_iterate_filters_validating_inactive_complete_and_no_priority():
	env = _env()
	good = _make(env, "good")
	validating = _make(env, "validating")
	inactive = _make(env, "inactive")
	complete = _make(env, "complete", complete=True)
	_make(env, "no_priority")  # never gets TorrentPriorityEC -> not in the collection

	for t in (good, validating, inactive, complete):
		t.add_component(TorrentPriorityEC(0))
	validating.add_component(ValidateTorrentEC())
	inactive.get_component(TorrentStatsEC).state = TorrentState.Inactive

	assert set(iterate_torrents_to_download(env.data_storage)) == {good}


# --- active window via TorrentSystem (event-driven) ------------------------
def test_active_window_marks_top_n_by_priority():
	async def run():
		env = _env(limit=2)
		t1, t2, t3, t4 = (_make(env, f"t{i}") for i in range(1, 5))
		system = TorrentSystem(env)
		await system.start()  # processes existing torrents in creation order
		assert _active(t1, t2, t3, t4) == [t1, t2]
		assert [t.get_component(TorrentPriorityEC).priority for t in (t1, t2, t3, t4)] == [0, 1, 2, 3]

	asyncio.run(run())


def test_window_shifts_when_a_torrent_is_paused():
	async def run():
		env = _env(limit=2)
		t1, t2, t3 = (_make(env, f"t{i}") for i in range(1, 4))
		system = TorrentSystem(env)
		await system.start()
		assert _active(t1, t2, t3) == [t1, t2]

		await system._on_torrent_stop(get_info_hash(t1))  # pause t1 -> window shifts to t2, t3
		assert _active(t1, t2, t3) == [t2, t3]

	asyncio.run(run())


def test_window_shifts_when_a_torrent_completes():
	async def run():
		env = _env(limit=2)
		t1, t2, t3 = (_make(env, f"t{i}") for i in range(1, 4))
		system = TorrentSystem(env)
		await system.start()
		assert _active(t1, t2, t3) == [t1, t2]

		bitfield = t1.get_component(TorrentEC).bitfield
		for i in range(t1.get_component(TorrentInfoEC).info.pieces_num):
			bitfield.set_index(i)
		await env.event_bus.dispatch_async("action.torrent.complete", t1)

		assert not t1.has_component(ActiveTorrentEC)
		assert _active(t2, t3) == [t2, t3]

	asyncio.run(run())


def test_remove_renormalizes_priorities():
	async def run():
		env = _env(limit=5)
		t1, t2, t3 = (_make(env, f"t{i}") for i in range(1, 4))
		system = TorrentSystem(env)
		await system.start()
		assert [t.get_component(TorrentPriorityEC).priority for t in (t1, t2, t3)] == [0, 1, 2]

		await system._on_torrent_remove(get_info_hash(t1))  # must not crash + must compact
		assert t2.get_component(TorrentPriorityEC).priority == 0
		assert t3.get_component(TorrentPriorityEC).priority == 1

	asyncio.run(run())
