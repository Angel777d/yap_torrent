"""Tests for the request pipeline: what puts blocks in flight, and what gets them back.

A block sitting in `PeerConnectionEC.requested` is charged against that peer's PIPELINE
slots *and* marked as requested on the piece, so nobody else asks for it. Every way a
request can die without an answer therefore has to release both, or the peer stops being
asked for anything and the block stops being offered to anyone.
"""
import asyncio
import time
from pathlib import Path

from yap_torrent.components.peer_ec import (
	LocalInterestedEC,
	PeerConnectionEC,
	PeerEC,
	PeerRateEC,
	RemoteUnchokedEC,
)
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg, decode, encode
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import Metainfo, PeerInfo
from yap_torrent.systems import add_known_peer, create_torrent_entity, get_info_hash
from yap_torrent.systems.download_system import (
	PIPELINE,
	DownloadSystem,
	_expire_requests,
	_process_piece_message,
	_request_from_peer,
)

PIECE_LENGTH = 16384 * 2  # two blocks per piece
PIECES = 20  # 40 blocks — comfortably more than one pipeline, so endgame stays out of it


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


def _peer(env: Env, torrent, port: int):
	"""A connected peer holding every piece, sitting in the download queue."""
	entity = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", port))
	for index in range(PIECES):
		entity.get_component(PeerEC).remote_bitfield.set_index(index)
	entity.add_component(PeerConnectionEC(
		get_info_hash(torrent), entity.get_component(PeerEC).peer_info, _FakeConnection(), bytes(8)))
	entity.add_component(PeerRateEC())
	entity.add_component(LocalInterestedEC())
	entity.add_component(RemoteUnchokedEC())
	return entity


def _pipeline(peer):
	return set(peer.get_component(PeerConnectionEC).requested)


# --- a CHOKE kills every request we had in flight ---------------------------
def test_choke_empties_the_pipeline_and_frees_the_blocks():
	# BEP-3: a choking peer discards our pending requests, so nothing will answer them.
	# Left in the pipeline they hold all PIPELINE slots for the rest of the connection —
	# the peer is never asked again, even after it unchokes us.
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)

		system = DownloadSystem(env)
		await system.start()

		await _request_from_peer(env, torrent, peer)
		in_flight = _pipeline(peer)
		assert len(in_flight) == PIPELINE

		peer.remove_component(RemoteUnchokedEC)
		await asyncio.gather(*env.event_bus.dispatch("peer.local.choked_changed", torrent, peer))
		assert _pipeline(peer) == set()

		# and the blocks are back in the pool, not stranded on the choking peer
		other = _peer(env, torrent, 6802)
		await _request_from_peer(env, torrent, other)
		assert _pipeline(other) == in_flight

	asyncio.run(run())


def test_unchoke_refills_the_pipeline():
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)

		system = DownloadSystem(env)
		await system.start()

		await asyncio.gather(*env.event_bus.dispatch("peer.local.choked_changed", torrent, peer))
		assert len(_pipeline(peer)) == PIPELINE

	asyncio.run(run())


# --- a reply we did not ask for must still clear the slot -------------------
def test_a_block_of_unexpected_length_still_drains_its_slot():
	# The reply is matched on (index, begin): rebuilding the PieceBlockInfo from the
	# length that came back would miss, and that slot would never free up again.
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)

		await _request_from_peer(env, torrent, peer)
		block = sorted(_pipeline(peer), key=lambda b: (b.index, b.begin))[0]

		short = Message(msg.piece(block.index, block.begin, b"x" * (block.length - 10)))
		await _process_piece_message(env, peer, torrent, short)

		assert block not in _pipeline(peer)
		assert len(_pipeline(peer)) == PIPELINE  # refilled with something else

	asyncio.run(run())


# --- a peer that accepts requests and answers nothing -----------------------
def test_timed_out_requests_go_back_to_the_pool():
	# Such a peer holds both queue markers, so it is never dropped as idle: without the
	# sweep its pipeline stays full forever and its blocks stay marked as requested.
	async def run():
		env = _env()
		env.config.block_request_timeout = 0  # anything in flight is already stale
		torrent = _torrent(env)
		silent = _peer(env, torrent, 6801)
		answering = _peer(env, torrent, 6802)

		await _request_from_peer(env, torrent, silent)
		stale = _pipeline(silent)
		assert len(stale) == PIPELINE

		await _expire_requests(env)

		# the freed blocks go to the peer that is answering, before being offered back
		assert _pipeline(answering) == stale
		assert _pipeline(silent).isdisjoint(stale)

	asyncio.run(run())


def test_requests_in_flight_are_kept_until_they_expire():
	async def run():
		env = _env()
		env.config.block_request_timeout = 600
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)

		await _request_from_peer(env, torrent, peer)
		in_flight = _pipeline(peer)

		await _expire_requests(env)
		assert _pipeline(peer) == in_flight

	asyncio.run(run())
