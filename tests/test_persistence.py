"""Tests for state that has to outlive a session: the torrent saves and the peer store.

Both stores are read back at startup, which is what makes their failure modes expensive —
a file that cannot be parsed, or an address that can never be dialled, costs every later
session, not just the one that wrote it.
"""
import asyncio
import pickle
from pathlib import Path

from yap_torrent.components.peer_ec import PeerEC, PeerState
from yap_torrent.components.torrent_ec import (
	SaveTorrentEC,
	TorrentCustomDataEC,
	TorrentEC,
	TorrentInfoEC,
	TorrentQueuePositionEC,
	TorrentState,
	TorrentStatsEC,
)
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo, PeerInfo
from yap_torrent.systems import (
	add_known_peer,
	create_torrent_entity,
	find_peer_entity,
	get_custom_data,
	get_torrent_entity,
	set_custom_data,
)
from yap_torrent.systems.local_data_system import LocalDataSystem, _export_torrent_data, _save
from yap_torrent.systems.peer_data_system import PeerDataSystem
from yap_torrent.systems.torrents_system import TorrentSystem
from yap_torrent.utils import write_atomic

PIECES = 4


def _env(tmp_path: Path) -> Env:
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	env.config.data_folder = tmp_path
	env.config.active_folder = tmp_path / "active"
	env.config.download_folder = tmp_path / "download"
	env.config.peers_file = str(tmp_path / "peers.dat")
	return env


def _meta(name: str = "t") -> Metainfo:
	info = {
		"name": name.encode(), "piece length": 16384,
		"pieces": b"\x00" * 20 * PIECES, "length": 16384 * PIECES,
	}
	return Metainfo(decode(encode({"info": info})))


def _torrent(env: Env, meta: Metainfo):
	return create_torrent_entity(env, meta.make_info_hash(), env.config.download_folder, {}, meta.info)


# --- the torrent store ------------------------------------------------------
def test_a_torrent_round_trips_through_the_save_file(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		torrent.get_component(TorrentEC).bitfield.set_index(0)
		torrent.get_component(TorrentEC).bitfield.set_index(2)
		torrent.get_component(TorrentStatsEC).update_downloaded(1234)
		torrent.get_component(TorrentStatsEC).state = TorrentState.Inactive
		torrent.add_component(TorrentQueuePositionEC(3))
		_save(tmp_path / "active" / meta.make_info_hash().hex(), _export_torrent_data(env, torrent))

		fresh = _env(tmp_path)
		system = LocalDataSystem(fresh)
		await system.start()

		restored = get_torrent_entity(fresh, meta.make_info_hash())
		assert restored is not None
		assert restored.has_component(TorrentInfoEC)
		assert restored.get_component(TorrentEC).bitfield.have == {0, 2}
		assert restored.get_component(TorrentStatsEC).downloaded == 1234
		assert restored.get_component(TorrentStatsEC).state == TorrentState.Inactive
		assert restored.get_component(TorrentQueuePositionEC).position == 3

	asyncio.run(run())


def test_one_unreadable_save_does_not_take_the_others_with_it(tmp_path):
	# the walk used to load straight into pickle.load, so a single truncated file — the
	# shape a crash mid-save leaves behind — stopped the client from starting at all
	async def run():
		env = _env(tmp_path)
		good = _meta("good")
		_save(tmp_path / "active" / good.make_info_hash().hex(), _export_torrent_data(env, _torrent(env, good)))
		(tmp_path / "active" / ("bb" * 20)).write_bytes(b"\x80\x05 truncated")

		fresh = _env(tmp_path)
		await LocalDataSystem(fresh).start()

		assert get_torrent_entity(fresh, good.make_info_hash()) is not None

	asyncio.run(run())


def test_an_interrupted_save_leaves_the_previous_one_intact(tmp_path):
	path = tmp_path / "active" / "aa"
	write_atomic(path, pickle.dumps({"version": 1}))
	# a .tmp beside it is what an interrupted write leaves; the real file still stands
	path.with_suffix(".tmp").write_bytes(b"half a pickle")

	async def run():
		fresh = _env(tmp_path)
		await LocalDataSystem(fresh).start()  # must not raise on the leftover
		with open(path, "rb") as f:
			assert pickle.load(f) == {"version": 1}

	asyncio.run(run())


def test_pausing_a_torrent_marks_it_for_saving(tmp_path):
	# a pause that only reaches disk at a clean shutdown is undone by any crash
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)
		system = TorrentSystem(env)
		await system.start()

		await asyncio.gather(*env.event_bus.dispatch("request.torrent.stop", meta.make_info_hash()))

		assert torrent.get_component(TorrentStatsEC).state == TorrentState.Inactive
		assert torrent.has_component(SaveTorrentEC)

	asyncio.run(run())


# --- plugin custom data -----------------------------------------------------
# Core stores a plugin's entry and hands it back, without reading it. That is what lets a
# concept only one plugin has survive a restart without becoming a core component.
def test_custom_data_round_trips_whatever_a_plugin_put_there(tmp_path):
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)

		set_custom_data(torrent, "some_plugin", {"labels": ["linux"], "seen": 3})
		_save(tmp_path / "active" / meta.make_info_hash().hex(), _export_torrent_data(env, torrent))

		fresh = _env(tmp_path)
		await LocalDataSystem(fresh).start()

		restored = get_torrent_entity(fresh, meta.make_info_hash())
		assert get_custom_data(restored, "some_plugin") == {"labels": ["linux"], "seen": 3}

	asyncio.run(run())


def test_a_custom_value_need_not_be_a_dict(tmp_path):
	# it is stored as Any: core has no shape in mind for it
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		torrent = _torrent(env, meta)

		set_custom_data(torrent, "counter", 7)
		set_custom_data(torrent, "names", ["a", "b"])
		_save(tmp_path / "active" / meta.make_info_hash().hex(), _export_torrent_data(env, torrent))

		fresh = _env(tmp_path)
		await LocalDataSystem(fresh).start()

		restored = get_torrent_entity(fresh, meta.make_info_hash())
		assert get_custom_data(restored, "counter") == 7
		assert get_custom_data(restored, "names") == ["a", "b"]

	asyncio.run(run())


def test_one_plugin_cannot_write_over_another(tmp_path):
	env = _env(tmp_path)
	torrent = _torrent(env, _meta())

	set_custom_data(torrent, "first", {"a": 1})
	set_custom_data(torrent, "second", {"b": 2})

	assert get_custom_data(torrent, "first") == {"a": 1}
	assert get_custom_data(torrent, "second") == {"b": 2}
	assert get_custom_data(torrent, "third", "fallback") == "fallback"


def test_storing_custom_data_asks_for_a_save(tmp_path):
	# it only reaches disk on a marker or at shutdown, and a crash takes the shutdown path
	env = _env(tmp_path)
	torrent = _torrent(env, _meta())

	set_custom_data(torrent, "p", {"a": 1})

	assert torrent.has_component(SaveTorrentEC)


def test_a_torrent_no_plugin_has_touched_carries_nothing(tmp_path):
	env = _env(tmp_path)
	torrent = _torrent(env, _meta())

	assert torrent.has_component(TorrentCustomDataEC) is False
	assert get_custom_data(torrent, "p") is None
	assert "custom_data" not in _export_torrent_data(env, torrent)


# --- the peer store ---------------------------------------------------------
def test_only_addresses_we_dialled_are_kept(tmp_path):
	# a peer that connected to *us* is recorded at the source port of its socket. Nothing
	# listens there, so every later session would spend attempts on an address that
	# cannot answer.
	env = _env(tmp_path)
	meta = _meta()
	_torrent(env, meta)
	info_hash = meta.make_info_hash()

	dialled = add_known_peer(env, info_hash, PeerInfo("127.0.0.1", 6881))
	dialled.get_component(PeerEC).state = PeerState.Good
	dialled.get_component(PeerEC).can_reach = True

	inbound = add_known_peer(env, info_hash, PeerInfo("127.0.0.1", 57132))
	inbound.get_component(PeerEC).state = PeerState.Good  # inbound peers are Good too

	PeerDataSystem(env).close()

	with open(tmp_path / "peers.dat", "rb") as f:
		assert pickle.load(f) == [(info_hash, "127.0.0.1", 6881)]


def test_reloaded_peers_stay_dialable(tmp_path):
	# otherwise a peer that loads but does not reconnect this session is dropped from the
	# store on close, and the file erodes to nothing
	async def run():
		env = _env(tmp_path)
		meta = _meta()
		_torrent(env, meta)
		info_hash = meta.make_info_hash()
		write_atomic(Path(env.config.peers_file), pickle.dumps([(info_hash, "127.0.0.1", 6881)]))

		system = PeerDataSystem(env)
		await system.start()

		peer = find_peer_entity(env, info_hash, "127.0.0.1", 6881)
		assert peer is not None
		assert peer.get_component(PeerEC).can_reach is True
		assert peer.get_component(PeerEC).state == PeerState.Unknown  # re-proved, not assumed

	asyncio.run(run())
