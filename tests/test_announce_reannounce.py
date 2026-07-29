"""Tests for forcing an announce.

AnnounceSystem is otherwise entirely timer-driven, and it stops announcing a torrent
whose tracker has failed too often — permanently, for the rest of the session. Asking
for a reannounce is the only way back from both.
"""
import asyncio
from pathlib import Path

from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo, TrackerAnnounceResponse
from yap_torrent.systems import create_torrent_entity
from yap_torrent.systems import announce_system as announce_module
from yap_torrent.systems.announce_system import AnnounceSystem

PIECES = 4
TRACKER = "http://tracker.test/announce"


def _env() -> Env:
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))


def _meta() -> Metainfo:
	info = {
		"name": b"t", "piece length": 16384,
		"pieces": b"\x00" * 20 * PIECES, "length": 16384 * PIECES,
	}
	return Metainfo(decode(encode({"info": info})))


def _torrent(env: Env, meta: Metainfo):
	entity = create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)
	entity.add_component(TorrentTrackerEC([[TRACKER]]))
	entity.add_component(TorrentTrackerDataEC())
	return entity


class _Tracker:
	"""Stands in for the HTTP announce; records every call."""

	def __init__(self, peers: bytes = b""):
		self.calls = []
		self._peers = peers

	async def __call__(self, announce, info_hash, **kwargs):
		self.calls.append((announce, kwargs.get("event")))
		return TrackerAnnounceResponse({"interval": 1800, "peers": self._peers}, 1)


def _patch(monkeypatch, tracker):
	monkeypatch.setattr(announce_module, "make_announce", tracker)


def test_reannounce_announces_immediately(monkeypatch):
	async def run():
		env = _env()
		meta = _meta()
		_torrent(env, meta)
		tracker = _Tracker()
		_patch(monkeypatch, tracker)

		system = AnnounceSystem(env)
		await system.start()
		await asyncio.sleep(0)  # start() announces "started" from a task; let it land first
		tracker.calls.clear()

		await asyncio.gather(*env.event_bus.dispatch("request.torrent.reannounce", meta.make_info_hash()))

		assert [announce for announce, _ in tracker.calls] == [TRACKER]

	asyncio.run(run())


def test_reannounce_clears_a_tracker_written_off_as_failed(monkeypatch):
	# five failures set failure_reason, after which the timer skips the torrent forever
	async def run():
		env = _env()
		meta = _meta()
		torrent = _torrent(env, meta)
		tracker_data = torrent.get_component(TorrentTrackerDataEC)
		for _ in range(5):
			tracker_data.fail_announce()
		assert tracker_data.failure_reason

		tracker = _Tracker()
		_patch(monkeypatch, tracker)
		system = AnnounceSystem(env)
		await system.start()
		await asyncio.sleep(0)
		tracker.calls.clear()
		# written off: the timer does not even consider it any more
		assert list(announce_module._iterate_active_torrents(env)) == []

		await asyncio.gather(*env.event_bus.dispatch("request.torrent.reannounce", meta.make_info_hash()))

		assert tracker_data.failure_reason == ""
		assert len(tracker.calls) == 1
		# and it is back in the timer's rotation
		assert list(announce_module._iterate_active_torrents(env)) == [torrent]

	asyncio.run(run())


def test_reannounce_hands_the_peers_it_gets_to_the_swarm(monkeypatch):
	async def run():
		env = _env()
		meta = _meta()
		_torrent(env, meta)
		# one compact peer: 127.0.0.1:6881
		_patch(monkeypatch, _Tracker(peers=bytes([127, 0, 0, 1, 0x1A, 0xE1])))

		seen = []

		async def _on_peers(info_hash, peers):
			seen.append((info_hash, peers))

		env.event_bus.add_listener("peers.update", _on_peers)

		system = AnnounceSystem(env)
		await system.start()
		await asyncio.gather(*env.event_bus.dispatch("request.torrent.reannounce", meta.make_info_hash()))
		await asyncio.sleep(0)

		assert seen and seen[-1][0] == meta.make_info_hash()
		assert seen[-1][1][0].port == 6881

	asyncio.run(run())


def test_reannounce_of_an_unknown_or_trackerless_torrent_is_a_no_op(monkeypatch):
	async def run():
		env = _env()
		meta = _meta()
		# a torrent with no tracker components at all, as a magnet starts out
		create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)
		tracker = _Tracker()
		_patch(monkeypatch, tracker)

		system = AnnounceSystem(env)
		await system.start()

		await asyncio.gather(*env.event_bus.dispatch("request.torrent.reannounce", meta.make_info_hash()))
		await asyncio.gather(*env.event_bus.dispatch("request.torrent.reannounce", b"\x00" * 20))

		assert tracker.calls == []

	asyncio.run(run())
