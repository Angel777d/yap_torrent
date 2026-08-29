"""Tests for the request pipeline: what puts blocks in flight, and what gets them back.

A block sitting in `PeerRequestsEC` is charged against that peer's PIPELINE
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
	PeerRequestsEC,
	RemoteUnchokedEC,
)
from yap_torrent.components.piece_ec import PieceEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg, decode, encode
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import Metainfo, PeerInfo, PieceInfo
from yap_torrent.systems import add_known_peer, create_torrent_entity, get_info_hash
from yap_torrent.systems.download_system import (
	PIPELINE,
	DownloadSystem,
	_expire_requests,
	_process_piece_message,
	_request_from_peer,
)
from yap_torrent.systems.intrest_system import InterestedSystem

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
		self.on_send = None

	async def send(self, message):
		self.sent.append(message)
		if self.on_send:
			self.on_send()

	def close(self):
		pass


def _peer(env: Env, torrent, port: int, in_queue: bool = True):
	"""A connected peer holding every piece.

	`in_queue` is what DownloadSystem would have set up once the peer became interesting
	and unchoked us: both queue markers *and* the pipeline component. Without it the peer
	is connected but outside the download queue, which is where a peer starts.
	"""
	entity = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", port))
	for index in range(PIECES):
		entity.get_component(PeerEC).remote_bitfield.set_index(index)
	entity.add_component(PeerConnectionEC(
		get_info_hash(torrent), entity.get_component(PeerEC).peer_info, _FakeConnection(), bytes(8)))
	entity.add_component(PeerRateEC())
	entity.add_component(LocalInterestedEC())
	if in_queue:
		entity.add_component(PeerRequestsEC())
		entity.add_component(RemoteUnchokedEC())
	return entity


def _piece(env: Env, torrent, index: int):
	entity = env.data_storage.create_entity()
	entity.add_component(PieceEC(get_info_hash(torrent),
	                             PieceInfo(size=PIECE_LENGTH, index=index, piece_hash=b"\x00" * 20)))
	return entity


def _pipeline(peer):
	return set(peer.get_component(PeerRequestsEC).blocks)


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

		_request_from_peer(env, torrent, peer)
		in_flight = _pipeline(peer)
		assert len(in_flight) == PIPELINE

		peer.remove_component(RemoteUnchokedEC)
		await asyncio.gather(*env.event_bus.dispatch("peer.local.choked_changed", torrent, peer))
		# leaving the download queue takes the pipeline with it — a peer outside the
		# queue cannot be holding blocks, so it does not get to carry an empty one
		assert peer.has_component(PeerRequestsEC) is False

		# and the blocks are back in the pool, not stranded on the choking peer
		other = _peer(env, torrent, 6802)
		_request_from_peer(env, torrent, other)
		assert _pipeline(other) == in_flight

	asyncio.run(run())


def test_an_unchoke_puts_the_peer_in_the_queue_and_fills_it():
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801, in_queue=False)

		system = DownloadSystem(env)
		await system.start()

		# what ChokeSystem does on UNCHOKE: set the marker, then announce it
		peer.add_component(RemoteUnchokedEC())
		await asyncio.gather(*env.event_bus.dispatch("peer.local.choked_changed", torrent, peer))

		assert peer.has_component(PeerRequestsEC)
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

		_request_from_peer(env, torrent, peer)
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

		_request_from_peer(env, torrent, silent)
		stale = _pipeline(silent)
		assert len(stale) == PIPELINE

		await _expire_requests(env)

		# the blocks are reclaimed and put back in front of the queue. NOTE: which peer
		# gets them is now collection order — the sweep no longer offers them to peers
		# that are answering before the one that just timed out, so the silent peer can
		# take its own blocks straight back.
		assert stale <= (_pipeline(silent) | _pipeline(answering))
		assert len(_pipeline(answering)) == PIPELINE

	asyncio.run(run())


def test_requests_in_flight_are_kept_until_they_expire():
	async def run():
		env = _env()
		env.config.block_request_timeout = 600
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)

		_request_from_peer(env, torrent, peer)
		in_flight = _pipeline(peer)

		await _expire_requests(env)
		assert _pipeline(peer) == in_flight

	asyncio.run(run())


def test_a_disconnect_takes_the_pipeline_with_it():
	# the pipeline is DownloadSystem's and PeerSystem does not strip it, so a peer that
	# drops while still in the download queue would keep an orphan one for the rest of
	# its life as a known peer — and the next connection would find a stale component
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)

		system = DownloadSystem(env)
		await system.start()
		_request_from_peer(env, torrent, peer)
		assert peer.has_component(PeerRequestsEC)

		await asyncio.gather(*env.event_bus.dispatch("peer.disconnected", torrent, peer))
		assert peer.has_component(PeerRequestsEC) is False

	asyncio.run(run())


def test_a_repeated_unchoke_does_not_replace_the_pipeline():
	# a peer may send UNCHOKE again while already in the queue; the blocks already in
	# flight must survive that
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801, in_queue=False)

		system = DownloadSystem(env)
		await system.start()
		peer.add_component(RemoteUnchokedEC())
		await asyncio.gather(*env.event_bus.dispatch("peer.local.choked_changed", torrent, peer))
		in_flight = _pipeline(peer)
		assert len(in_flight) == PIPELINE

		await asyncio.gather(*env.event_bus.dispatch("peer.local.choked_changed", torrent, peer))
		assert _pipeline(peer) == in_flight

	asyncio.run(run())


# --- the swarm may move while a completed piece is being announced ----------
def test_a_peer_connecting_mid_announce_still_leaves_every_peer_told():
	# the HAVE for a completed piece is sent to each connected peer in turn, with an await
	# between them; a peer connecting in that window changes the connected-peer collection,
	# and walking it live raised out of the piece.complete dispatch mid-broadcast
	async def run():
		env = _env()
		torrent = _torrent(env)
		first = _peer(env, torrent, 6801)
		second = _peer(env, torrent, 6802)

		system = InterestedSystem(env)
		await system.start()

		first.get_component(PeerConnectionEC).connection.on_send = lambda: _peer(env, torrent, 6803)

		await asyncio.gather(*env.event_bus.dispatch("piece.complete", torrent, _piece(env, torrent, 0)))

		have = msg.have(0)
		assert have in first.get_component(PeerConnectionEC).connection.sent
		assert have in second.get_component(PeerConnectionEC).connection.sent

	asyncio.run(run())


def test_a_peer_dropping_mid_announce_does_not_break_the_broadcast():
	async def run():
		env = _env()
		torrent = _torrent(env)
		first = _peer(env, torrent, 6801)
		second = _peer(env, torrent, 6802)
		third = _peer(env, torrent, 6803)

		system = InterestedSystem(env)
		await system.start()

		# what PeerSystem._process_disconnected leaves behind for a peer torn down mid-tick
		first.get_component(PeerConnectionEC).connection.on_send = lambda: second.remove_component(PeerConnectionEC)

		await asyncio.gather(*env.event_bus.dispatch("piece.complete", torrent, _piece(env, torrent, 0)))

		assert msg.have(0) in third.get_component(PeerConnectionEC).connection.sent

	asyncio.run(run())


def test_the_requests_actually_reach_the_connection():
	# _request_from_peer fires each send as a task rather than awaiting it; the task must
	# keep a strong reference or asyncio can GC it before it runs and drop the REQUEST
	async def run():
		env = _env()
		torrent = _torrent(env)
		peer = _peer(env, torrent, 6801)

		_request_from_peer(env, torrent, peer)
		await asyncio.sleep(0)  # let the fired send tasks run

		sent = peer.get_component(PeerConnectionEC).connection.sent
		assert len(sent) == PIPELINE
		assert all(m[0] == msg.MessageId.REQUEST.value for m in sent)

	asyncio.run(run())
