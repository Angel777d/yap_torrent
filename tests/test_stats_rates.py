"""Tests for rate metering: bytes counted on the transfer path becoming a rate.

`PeerRateEC` accumulates a window on every block and `sample_rate()` empties it, so the
sampling has to happen in exactly one place and the per-torrent figure has to be rebuilt
from the peers each time rather than accumulated.
"""
import asyncio
import time
from pathlib import Path

from yap_torrent.components.peer_ec import PeerConnectionEC, PeerEC, PeerRateEC
from yap_torrent.components.torrent_ec import TorrentRateEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo, PeerInfo
from yap_torrent.systems import add_known_peer, create_torrent_entity, get_info_hash
from yap_torrent.systems.stats_system import StatsSystem, session_rates

PIECES = 4


def _env() -> Env:
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))


def _torrent(env: Env, name: str = "t"):
	info = {
		"name": name.encode(), "piece length": 16384,
		"pieces": b"\x00" * 20 * PIECES, "length": 16384 * PIECES,
	}
	meta = Metainfo(decode(encode({"info": info})))
	return create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)


def _peer(env: Env, torrent, port: int):
	entity = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", port))
	entity.add_component(PeerRateEC())
	return entity


def _rate(torrent):
	return torrent.get_component(TorrentRateEC)


def test_a_torrent_rate_is_the_sum_of_its_peers():
	async def run():
		env = _env()
		torrent = _torrent(env)
		a, b = _peer(env, torrent, 6801), _peer(env, torrent, 6802)
		a.get_component(PeerRateEC).add_downloaded(4000)
		b.get_component(PeerRateEC).add_downloaded(6000)
		b.get_component(PeerRateEC).add_uploaded(1000)

		await StatsSystem(env)._update(1.0)

		peers = [a.get_component(PeerRateEC), b.get_component(PeerRateEC)]
		assert _rate(torrent).down_rate == sum(p.down_rate for p in peers)
		assert _rate(torrent).up_rate == sum(p.up_rate for p in peers)
		assert _rate(torrent).down_rate > 0

	asyncio.run(run())


def test_rates_fall_back_to_zero_when_nothing_moves():
	# the window is consumed by the sample, so a second one with no traffic must read 0
	# rather than repeating the last figure
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)
		peer.get_component(PeerRateEC).add_downloaded(4000)

		system = StatsSystem(env)
		await system._update(1.0)
		assert _rate(torrent).down_rate > 0

		await system._update(1.0)
		assert _rate(torrent).down_rate == 0.0

	asyncio.run(run())


def test_a_departed_peer_stops_counting():
	# rates are rebuilt from the peers each sample; accumulating on the torrent would leave
	# a torrent with no peers reporting a transfer that is not happening
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)
		peer.get_component(PeerRateEC).add_downloaded(4000)

		system = StatsSystem(env)
		await system._update(1.0)
		assert _rate(torrent).down_rate > 0

		peer.remove_component(PeerRateEC)  # what a disconnect strips
		await system._update(1.0)
		assert _rate(torrent).down_rate == 0.0

	asyncio.run(run())


def test_session_rates_span_every_torrent():
	async def run():
		env = _env()
		one, two = _torrent(env, "one"), _torrent(env, "two")
		for torrent, port in ((one, 6801), (two, 6802)):
			peer = _peer(env, torrent, port)
			peer.get_component(PeerRateEC).add_downloaded(5000)
			peer.get_component(PeerRateEC).add_uploaded(500)

		await StatsSystem(env)._update(1.0)

		down, up = session_rates(env)
		assert down == _rate(one).down_rate + _rate(two).down_rate
		assert up == _rate(one).up_rate + _rate(two).up_rate
		assert down > 0 and up > 0

	asyncio.run(run())
