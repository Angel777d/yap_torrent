"""Tests for the download-queue active window (Part A).

Run with:  pytest tests
(only pytest itself needs installing; sources come from conftest.py)
"""
from pathlib import Path

from yap_torrent.components.torrent_ec import (
	ActiveTorrentEC,
	TorrentEC,
	TorrentPriorityEC,
	TorrentState,
	TorrentStatsEC,
)
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.systems import create_torrent_entity
from yap_torrent.systems.torrents_system import TorrentSystem, select_active


# --- pure helper -----------------------------------------------------------
def test_select_active_picks_lowest_priority():
	items = [(b"a", 3), (b"b", 1), (b"c", 2), (b"d", 5)]
	assert select_active(items, 2) == {b"b", b"c"}  # priorities 1, 2


def test_select_active_edge_cases():
	assert select_active([(b"a", 1)], 0) == set()
	assert select_active([], 3) == set()
	assert select_active([(b"a", 1), (b"b", 2)], 5) == {b"a", b"b"}  # limit > n


def test_select_active_is_deterministic_on_ties():
	items = [(b"z", 1), (b"a", 1), (b"m", 1)]
	# ties break on key ascending -> a, m
	assert select_active(items, 2) == {b"a", b"m"}


# --- recompute integration -------------------------------------------------
def _make_torrent(env, name: str, npieces: int = 2, complete: bool = False):
	piece_len = 16384
	info = {
		"name": name.encode(),
		"piece length": piece_len,
		"pieces": b"\x00" * 20 * npieces,
		"length": piece_len * npieces,
	}
	metainfo = Metainfo(decode(encode({"info": info})))
	info_hash = metainfo.make_info_hash()
	entity = create_torrent_entity(env, info_hash, Path("D:/dl"), {}, metainfo.info)
	if complete:
		bitfield = entity.get_component(TorrentEC).bitfield
		for i in range(npieces):
			bitfield.set_index(i)
	return entity


def _active_names(entities):
	return {
		e.get_component(TorrentEC).info_hash: e.has_component(ActiveTorrentEC)
		for e in entities
	}


def test_recompute_marks_top_n_and_shifts_window():
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	env.config.max_active_downloads = 2
	system = TorrentSystem(env)

	t1 = _make_torrent(env, "t1")
	t2 = _make_torrent(env, "t2")
	t3 = _make_torrent(env, "t3")
	t4 = _make_torrent(env, "t4")

	system._recompute_queue()

	# exactly the first two (lowest priority = earliest eligible) are active
	active = [t for t in (t1, t2, t3, t4) if t.has_component(ActiveTorrentEC)]
	assert active == [t1, t2]
	# all eligible got a priority, ascending in creation order
	prios = [t.get_component(TorrentPriorityEC).priority for t in (t1, t2, t3, t4)]
	assert prios == sorted(prios) and len(set(prios)) == 4

	# pause t1 -> it leaves the queue, window shifts to t2, t3
	t1.get_component(TorrentStatsEC).state = TorrentState.Inactive
	system._recompute_queue()
	assert not t1.has_component(ActiveTorrentEC)
	assert not t1.has_component(TorrentPriorityEC)
	assert [t for t in (t2, t3, t4) if t.has_component(ActiveTorrentEC)] == [t2, t3]

	# complete t2 -> leaves the queue, window shifts to t3, t4
	bf = t2.get_component(TorrentEC).bitfield
	for i in range(2):
		bf.set_index(i)
	system._recompute_queue()
	assert not t2.has_component(ActiveTorrentEC)
	assert not t2.has_component(TorrentPriorityEC)
	assert [t for t in (t3, t4) if t.has_component(ActiveTorrentEC)] == [t3, t4]


def test_incomplete_metadata_and_verifying_are_excluded():
	from yap_torrent.components.torrent_ec import ValidateTorrentEC

	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	env.config.max_active_downloads = 5
	system = TorrentSystem(env)

	# magnet without metadata: no TorrentInfoEC
	magnet = create_torrent_entity(env, b"\x11" * 20, Path("D:/dl"), {}, None)
	# verifying torrent
	verifying = _make_torrent(env, "verifying")
	verifying.add_component(ValidateTorrentEC())
	# normal torrent
	normal = _make_torrent(env, "normal")

	system._recompute_queue()

	assert not magnet.has_component(ActiveTorrentEC)
	assert not verifying.has_component(ActiveTorrentEC)
	assert normal.has_component(ActiveTorrentEC)
