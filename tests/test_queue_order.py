"""Tests for setting the download queue's order.

The queue is a dense 0..n-1 ordering that `iterate_torrents_in_queue_order` and every
contested peer slot read, so any change has to leave it dense — nudging one number would
collide with a neighbour and leave two torrents claiming the same place.

Core holds the ordinals and is handed a finished order. What "up", "bottom" or a position
out of range means belongs to whoever took the request, and is tested in the
transmission_rpc suite; there are no directions here.
"""
import asyncio
from pathlib import Path

from yap_torrent.components.torrent_ec import SaveTorrentEC, TorrentQueuePositionEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.systems import create_torrent_entity, get_info_hash
from yap_torrent.systems.torrents_system import TorrentSystem

PIECES = 4


def _env() -> Env:
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))


def _torrent(env: Env, name: str, position: int):
	info = {
		"name": name.encode(), "piece length": 16384,
		"pieces": b"\x00" * 20 * PIECES, "length": 16384 * PIECES,
	}
	meta = Metainfo(decode(encode({"info": info})))
	entity = create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)
	entity.add_component(TorrentQueuePositionEC(position))
	return entity


async def _queue(env: Env, count: int = 4):
	system = TorrentSystem(env)
	await system.start()
	torrents = [_torrent(env, f"t{i}", i) for i in range(count)]
	return system, torrents


def _order(torrents):
	return [t.get_component(TorrentQueuePositionEC).position for t in torrents]


async def _set_order(env, torrents):
	"""Hand core the order as a list of info_hashes, the way a caller does."""
	await asyncio.gather(*env.event_bus.dispatch(
		"request.torrent.queue_order", [get_info_hash(t) for t in torrents]))


def test_the_list_becomes_the_order():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _set_order(env, [torrents[2], torrents[0], torrents[3], torrents[1]])

		assert _order(torrents) == [1, 3, 0, 2]

	asyncio.run(run())


def test_the_queue_stays_dense():
	async def run():
		env = _env()
		_, torrents = await _queue(env, count=5)
		await _set_order(env, [torrents[4], torrents[1], torrents[0], torrents[3], torrents[2]])

		assert sorted(_order(torrents)) == [0, 1, 2, 3, 4]

	asyncio.run(run())


def test_torrents_left_out_keep_their_relative_order_behind_the_list():
	# a caller that only knows about some of the queue must not drop the rest
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _set_order(env, [torrents[3]])

		assert _order(torrents) == [1, 2, 3, 0]  # t3 leads, 0/1/2 follow in their old order

	asyncio.run(run())


def test_an_unknown_hash_is_skipped():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await asyncio.gather(*env.event_bus.dispatch(
			"request.torrent.queue_order", [b"\xaa" * 20, get_info_hash(torrents[2])]))

		assert _order(torrents) == [1, 2, 0, 3]  # t2 leads; the unknown hash took no place

	asyncio.run(run())


def test_a_repeated_hash_is_placed_once():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _set_order(env, [torrents[1], torrents[1], torrents[0]])

		assert _order(torrents) == [1, 0, 2, 3]

	asyncio.run(run())


def test_an_empty_order_leaves_the_queue_alone():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await asyncio.gather(*env.event_bus.dispatch("request.torrent.queue_order", []))

		assert _order(torrents) == [0, 1, 2, 3]

	asyncio.run(run())


def test_only_the_torrents_that_moved_are_marked_for_saving():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		for torrent in torrents:
			if torrent.has_component(SaveTorrentEC):
				torrent.remove_component(SaveTorrentEC)

		# swap 2 and 3 only
		await _set_order(env, [torrents[0], torrents[1], torrents[3], torrents[2]])

		marked = [i for i, t in enumerate(torrents) if t.has_component(SaveTorrentEC)]
		assert marked == [2, 3]

	asyncio.run(run())


def test_removing_a_torrent_closes_the_gap():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await asyncio.gather(*env.event_bus.dispatch(
			"request.torrent.remove", get_info_hash(torrents[1])))

		assert _order([torrents[0], torrents[2], torrents[3]]) == [0, 1, 2]

	asyncio.run(run())
