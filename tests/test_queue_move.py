"""Tests for moving a torrent within the download queue.

The queue is a dense 0..n-1 ordering that `iterate_torrents_in_queue_order` and every
contested peer slot read, so a move has to leave it dense — nudging one number would
collide with a neighbour and leave two torrents claiming the same place.
"""
import asyncio
from pathlib import Path

from yap_torrent.components.torrent_ec import SaveTorrentEC, TorrentQueuePositionEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.systems import create_torrent_entity
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


async def _move(env, torrent, direction):
	from yap_torrent.systems import get_info_hash
	await asyncio.gather(*env.event_bus.dispatch(
		"request.torrent.queue_move", get_info_hash(torrent), direction))


def test_moving_to_the_top_pushes_everything_else_down():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _move(env, torrents[2], "top")
		# t2 leads; the ones it passed each slide down by one
		assert _order(torrents) == [1, 2, 0, 3]

	asyncio.run(run())


def test_moving_to_the_bottom_pulls_everything_else_up():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _move(env, torrents[1], "bottom")
		assert _order(torrents) == [0, 3, 1, 2]

	asyncio.run(run())


def test_up_and_down_swap_with_one_neighbour():
	async def run():
		env = _env()
		_, torrents = await _queue(env)

		await _move(env, torrents[2], "up")
		assert _order(torrents) == [0, 2, 1, 3]

		await _move(env, torrents[2], "down")
		assert _order(torrents) == [0, 1, 2, 3]

	asyncio.run(run())


def test_moving_past_an_end_stays_at_that_end():
	async def run():
		env = _env()
		_, torrents = await _queue(env)

		await _move(env, torrents[0], "up")
		await _move(env, torrents[3], "down")

		assert _order(torrents) == [0, 1, 2, 3]

	asyncio.run(run())


def test_the_queue_stays_dense_after_a_move():
	async def run():
		env = _env()
		_, torrents = await _queue(env, count=5)
		await _move(env, torrents[4], "top")
		await _move(env, torrents[0], "bottom")

		assert sorted(_order(torrents)) == [0, 1, 2, 3, 4]

	asyncio.run(run())


def test_only_the_torrents_that_moved_are_marked_for_saving():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		for torrent in torrents:
			if torrent.has_component(SaveTorrentEC):
				torrent.remove_component(SaveTorrentEC)

		await _move(env, torrents[3], "up")  # swaps 2 and 3 only

		marked = [i for i, t in enumerate(torrents) if t.has_component(SaveTorrentEC)]
		assert marked == [2, 3]

	asyncio.run(run())


def test_an_unknown_direction_leaves_the_queue_alone():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _move(env, torrents[1], "sideways")
		assert _order(torrents) == [0, 1, 2, 3]

	asyncio.run(run())


def test_removing_a_torrent_closes_the_gap():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		from yap_torrent.systems import get_info_hash
		await asyncio.gather(*env.event_bus.dispatch(
			"request.torrent.remove", get_info_hash(torrents[1])))

		assert _order([torrents[0], torrents[2], torrents[3]]) == [0, 1, 2]

	asyncio.run(run())


def test_a_torrent_can_be_moved_to_an_absolute_position():
	# torrent-set sets queuePosition by number rather than by direction
	async def run():
		env = _env()
		_, torrents = await _queue(env, count=5)
		await _move(env, torrents[0], 3)
		assert _order(torrents) == [3, 0, 1, 2, 4]

	asyncio.run(run())


def test_an_out_of_range_position_clamps_to_an_end():
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _move(env, torrents[1], 99)
		assert _order(torrents) == [0, 3, 1, 2]

	asyncio.run(run())


def test_a_boolean_is_not_treated_as_a_position():
	# bool is an int subclass, so a JSON true would otherwise mean "position 1"
	async def run():
		env = _env()
		_, torrents = await _queue(env)
		await _move(env, torrents[3], True)
		assert _order(torrents) == [0, 1, 2, 3]

	asyncio.run(run())
