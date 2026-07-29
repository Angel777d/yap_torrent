"""Tests for choosing what a torrent downloads, and for reporting how far each file got.

The wanted mask is what the whole download path reads, so a selection change has to reach
it — and has to be announced, since interest is otherwise only re-derived when a peer
happens to say something.
"""
import asyncio
import time
from pathlib import Path

from yap_torrent.components.file_ec import FilePriority, TorrentFileEC, TorrentFileStateEC
from yap_torrent.components.peer_ec import LocalInterestedEC, PeerConnectionEC, PeerEC, PeerRateEC
from yap_torrent.components.torrent_ec import (
	SaveTorrentEC,
	TorrentDownloadProgressEC,
	TorrentEC,
	TorrentInfoEC,
)
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo, PeerInfo
from yap_torrent.systems import (
	add_known_peer,
	create_torrent_entity,
	file_bytes_completed,
	get_info_hash,
	iterate_files,
)
from yap_torrent.systems.file_system import FileSystem
from yap_torrent.systems.intrest_system import InterestedSystem

PIECE_LEN = 16384
# a=10000 (piece 0), b=20000 (pieces 0..1), c=40000 (pieces 1..4) — every boundary shared
FILES = [(b"a", 10000), (b"b", 20000), (b"c", 40000)]
TOTAL = sum(length for _, length in FILES)


def _env() -> Env:
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))


def _meta() -> Metainfo:
	info = {
		"name": b"t", "piece length": PIECE_LEN,
		"pieces": b"\x00" * 20 * 5,
		"files": [{"path": [name], "length": length} for name, length in FILES],
	}
	return Metainfo(decode(encode({"info": info})))


async def _torrent_with_files(env: Env):
	"""A torrent whose file entities were built the way FileSystem builds them.

	The collection listener that builds them is dispatched as a task, so the loop has to
	be given a turn before the files exist — the same reason local_swarm settles.
	"""
	system = FileSystem(env)
	await system.start()
	meta = _meta()
	torrent = create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)
	await asyncio.sleep(0)
	return system, torrent


def _by_index(env, torrent):
	return {e.get_component(TorrentFileEC).index: e for e in iterate_files(env, get_info_hash(torrent))}


def _wanted(torrent):
	return torrent.get_component(TorrentDownloadProgressEC).wanted.have


# --- changing the selection -------------------------------------------------
def test_deselecting_a_file_narrows_the_wanted_mask():
	async def run():
		env = _env()
		system, torrent = await _torrent_with_files(env)
		assert _wanted(torrent) == {0, 1, 2, 3, 4}

		# drop file c (pieces 1..4); pieces 0..1 stay because a and b still want them
		await system._on_file_select(get_info_hash(torrent), [2], wanted=False)

		assert _by_index(env, torrent)[2].get_component(TorrentFileStateEC).wanted is False
		assert _wanted(torrent) == {0, 1}
		assert torrent.has_component(SaveTorrentEC)  # a selection lost on restart is not a selection

	asyncio.run(run())


def test_reselecting_a_file_widens_it_again():
	async def run():
		env = _env()
		system, torrent = await _torrent_with_files(env)
		info_hash = get_info_hash(torrent)

		await system._on_file_select(info_hash, [2], wanted=False)
		assert _wanted(torrent) == {0, 1}

		await system._on_file_select(info_hash, [2], wanted=True)
		assert _wanted(torrent) == {0, 1, 2, 3, 4}

	asyncio.run(run())


def test_priority_can_be_set_without_touching_wanted():
	async def run():
		env = _env()
		system, torrent = await _torrent_with_files(env)

		await system._on_file_select(get_info_hash(torrent), [0, 1], priority=FilePriority.High)

		files = _by_index(env, torrent)
		assert files[0].get_component(TorrentFileStateEC).priority == FilePriority.High
		assert files[1].get_component(TorrentFileStateEC).priority == FilePriority.High
		assert files[2].get_component(TorrentFileStateEC).priority == FilePriority.Normal
		assert all(f.get_component(TorrentFileStateEC).wanted for f in files.values())

	asyncio.run(run())


def test_a_selection_that_changes_nothing_is_not_announced():
	async def run():
		env = _env()
		system, torrent = await _torrent_with_files(env)
		seen = []
		env.event_bus.add_listener("action.torrent.files_changed", lambda *a: _record(seen))

		await system._on_file_select(get_info_hash(torrent), [0], wanted=True)  # already wanted

		assert seen == []
		assert torrent.has_component(SaveTorrentEC) is False

	async def _record(seen):
		seen.append(1)

	asyncio.run(run())


def test_deselecting_the_last_wanted_file_releases_the_peer():
	# interest is otherwise only re-derived when a peer says something, so a peer would sit
	# in the download queue holding a slot for a torrent that wants nothing from it
	async def run():
		env = _env()
		files, torrent = await _torrent_with_files(env)
		interested = InterestedSystem(env)
		await interested.start()
		info_hash = get_info_hash(torrent)

		peer = add_known_peer(env, info_hash, PeerInfo("127.0.0.1", 6801))
		peer.add_component(PeerConnectionEC(info_hash, peer.get_component(PeerEC).peer_info, _FakeConnection(), bytes(8)))
		peer.add_component(PeerRateEC())
		for index in range(5):
			peer.get_component(PeerEC).remote_bitfield.set_index(index)

		await asyncio.gather(*env.event_bus.dispatch("peer.connected", torrent, peer))
		assert peer.has_component(LocalInterestedEC)

		await files._on_file_select(info_hash, None, wanted=False)  # nothing wanted at all
		assert peer.has_component(LocalInterestedEC) is False

	asyncio.run(run())


class _FakeConnection:
	def __init__(self):
		self.connection_time = time.monotonic()
		self.sent = []

	async def send(self, message):
		self.sent.append(message)

	def close(self):
		pass


# --- reporting per-file progress --------------------------------------------
def test_completed_bytes_are_counted_per_file_not_per_piece():
	# piece 0 covers bytes 0..16384, which is all of file a and the first 6384 of file b:
	# counting whole pieces over a file's piece range would report both as complete
	async def run():
		env = _env()
		_, torrent = await _torrent_with_files(env)
		torrent.get_component(TorrentEC).bitfield.set_index(0)

		files = _by_index(env, torrent)
		assert file_bytes_completed(torrent, files[0]) == 10000  # all of a
		assert file_bytes_completed(torrent, files[1]) == PIECE_LEN - 10000  # 6384 of b
		assert file_bytes_completed(torrent, files[2]) == 0  # c starts in piece 1

	asyncio.run(run())


def test_a_complete_torrent_reports_every_file_whole():
	async def run():
		env = _env()
		_, torrent = await _torrent_with_files(env)
		for index in range(5):
			torrent.get_component(TorrentEC).bitfield.set_index(index)

		files = _by_index(env, torrent)
		assert [file_bytes_completed(torrent, files[i]) for i in range(3)] == [10000, 20000, 40000]
		assert sum(file_bytes_completed(torrent, f) for f in files.values()) == TOTAL

	asyncio.run(run())
