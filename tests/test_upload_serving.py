"""Tests for serving REQUESTs: what we answer, what we refuse, and what we bill for.

A reply is served as it arrives and the block is copied out before the send, so serving
holds nothing against the piece — which is what keeps a seeding client's piece cache
bounded by `max_cached_pieces` rather than by how much it has ever uploaded.
"""
import asyncio
import time
from pathlib import Path

from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import (
	LocalUnchokedEC,
	PeerConnectionEC,
	PeerEC,
	PeerRateEC,
	PeerStatsEC,
)
from yap_torrent.components.piece_ec import CompletePieceDataEC, PieceEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentStatsEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg, decode, encode
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import Metainfo, PeerInfo
from yap_torrent.systems import add_known_peer, create_torrent_entity, get_info_hash
from yap_torrent.systems.piece_system import PieceSystem
from yap_torrent.systems.upload_system import MAX_BLOCK_SIZE, UploadSystem

PIECE_LENGTH = 16384 * 2
PIECES = 4


def _env() -> Env:
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))


def _torrent(env: Env):
	info = {
		"name": b"t", "piece length": PIECE_LENGTH,
		"pieces": b"\x00" * 20 * PIECES, "length": PIECE_LENGTH * PIECES,
	}
	meta = Metainfo(decode(encode({"info": info})))
	return create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)


class _FakeConnection:
	def __init__(self):
		self.connection_time = time.monotonic()
		self.sent = []

	async def send(self, message):
		self.sent.append(message)

	def close(self):
		pass


def _peer(env: Env, torrent, port: int, unchoked: bool = True):
	entity = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", port))
	entity.add_component(PeerConnectionEC(
		get_info_hash(torrent), entity.get_component(PeerEC).peer_info, _FakeConnection(), bytes(8)))
	entity.add_component(PeerRateEC())
	if unchoked:
		entity.add_component(LocalUnchokedEC())
	return entity


def _cached_piece(env: Env, torrent, index: int):
	"""A piece already in memory, so serving it needs no disk at all."""
	info = torrent.get_component(TorrentInfoEC).info
	return (env.data_storage.create_entity()
	        .add_component(PieceEC(get_info_hash(torrent), info.get_piece_info(index)))
	        .add_component(CompletePieceDataEC(bytes(PIECE_LENGTH)))
	        .add_component(IdleEC()))


def _sent(peer):
	return peer.get_component(PeerConnectionEC).connection.sent


def _request(index: int, begin: int, length: int) -> Message:
	return Message(msg.request(index, begin, length))


# --- serving must not hold anything against the piece -----------------------
def test_serving_a_request_leaves_the_piece_evictable(tmp_path):
	# Uploading used to mark the piece, and PieceSystem refuses to evict a marked one — so
	# every piece a peer ever asked for stayed in memory and max_cached_pieces stopped
	# meaning anything for a seeding client. Serving may leave nothing behind but the
	# idle stamp.
	async def run():
		env = _env()
		env.config.download_folder = tmp_path  # PieceSystem creates it on construction
		env.config.max_cached_pieces = 0
		env.config.piece_cache_ttl = 0
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)
		piece = _cached_piece(env, torrent, 0)

		system = UploadSystem(env)
		await system.start()
		await system._on_request(torrent, peer, _request(0, 0, MAX_BLOCK_SIZE))
		assert len(_sent(peer)) == 1

		PieceSystem(env)._cleanup()
		assert piece.is_valid() is False

	asyncio.run(run())


def test_a_cancel_is_ignored_without_raising():
	# CANCEL shares REQUEST's payload layout but not its id, so it used to be parsed as a
	# REQUEST and raise inside a fire-and-forget task. There is no upload queue to take a
	# block out of, so the message is simply not ours.
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)
		_cached_piece(env, torrent, 0)

		system = UploadSystem(env)
		await system.start()
		await asyncio.gather(*env.event_bus.dispatch(
			"peer.message", torrent, peer, Message(msg.cancel(0, 0, MAX_BLOCK_SIZE))))

		assert _sent(peer) == []

	asyncio.run(run())


# --- a REQUEST is not to be trusted -----------------------------------------
def test_an_oversized_request_is_refused():
	# the slice answering it would clamp to the piece, but the peer and the torrent were
	# then billed for the length asked for, not the bytes that went out
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)
		_cached_piece(env, torrent, 0)

		system = UploadSystem(env)
		await system.start()
		await system._on_request(torrent, peer, _request(0, 0, MAX_BLOCK_SIZE * 4))

		assert _sent(peer) == []
		assert peer.get_component(PeerStatsEC).uploaded == 0
		assert torrent.get_component(TorrentStatsEC).uploaded == 0

	asyncio.run(run())


def test_a_request_past_the_end_of_a_piece_is_refused():
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)
		_cached_piece(env, torrent, 0)

		system = UploadSystem(env)
		await system.start()
		await system._on_request(torrent, peer, _request(0, PIECE_LENGTH - 10, MAX_BLOCK_SIZE))
		await system._on_request(torrent, peer, _request(PIECES + 5, 0, MAX_BLOCK_SIZE))

		assert _sent(peer) == []
		assert torrent.get_component(TorrentStatsEC).uploaded == 0

	asyncio.run(run())


def test_stats_count_the_bytes_that_went_out():
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)
		_cached_piece(env, torrent, 0)

		system = UploadSystem(env)
		await system.start()
		await system._on_request(torrent, peer, _request(0, 0, MAX_BLOCK_SIZE))

		assert peer.get_component(PeerStatsEC).uploaded == MAX_BLOCK_SIZE
		assert torrent.get_component(TorrentStatsEC).uploaded == MAX_BLOCK_SIZE

	asyncio.run(run())


def test_a_choked_peer_is_not_served():
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801, unchoked=False)
		_cached_piece(env, torrent, 0)

		system = UploadSystem(env)
		await system.start()
		await system._on_request(torrent, peer, _request(0, 0, MAX_BLOCK_SIZE))

		assert _sent(peer) == []

	asyncio.run(run())
