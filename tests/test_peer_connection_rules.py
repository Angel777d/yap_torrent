"""Tests for the connection rules: probe -> classify -> keep or release.

A connection is worth holding only while one side wants something from the other.
`download_value` / `upload_value` are what decide that, and PeerEC is what remembers
the answer after the socket is gone.
"""
import time
from pathlib import Path

from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import (
	LocalInterestedEC,
	LocalUnchokedEC,
	PeerConnectionInProgressEC,
	PeerConnectionEC,
	PeerDisconnectedEC,
	PeerEC,
	PeerRateEC,
	PeerState,
	PeerStatsEC,
	RemoteInterestedEC,
	RemoteUnchokedEC,
)
from yap_torrent.components.torrent_ec import (
	TorrentDownloadProgressEC,
	TorrentEC,
	TorrentInfoEC,
	TorrentQueuePositionEC,
)
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Bitfield, Metainfo, PeerInfo
from yap_torrent.systems import add_known_peer, create_torrent_entity, get_info_hash
from yap_torrent.systems.peer_logic import QUESTIONABLE_RETRY
from yap_torrent.systems.peer_system import (
	PeerSystem,
	calculate_candidates,
	_in_any_queue,
	_in_download_queue,
	_in_upload_queue,
	download_value,
	upload_value,
)

PIECE_LEN = 16384


class _FakeConnection:
	"""Stands in for net.Connection: PeerConnectionEC only stores and closes it here."""

	def __init__(self):
		self.connection_time = time.monotonic()
		self.closed = False

	def close(self):
		self.closed = True


def _attach_connection(peer_entity) -> _FakeConnection:
	"""Mirror what PeerSystem._add_peer does to a peer that just connected."""
	connection = _FakeConnection()
	peer_entity.add_component(PeerConnectionEC(b"h" * 20, PeerInfo("127.0.0.1", 1), connection, bytes(8)))
	peer_entity.add_component(PeerRateEC())
	peer_entity.get_component(IdleEC).touch()  # created with the entity, never re-added
	return connection


def _backdate(idle: IdleEC, seconds: float) -> None:
	"""Pretend the component has been idle for `seconds`. IdleEC has no public setter."""
	setattr(idle, "_IdleEC__last_update", time.monotonic() - seconds)


def _env() -> Env:
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))


def _metainfo(pieces: int = 4) -> Metainfo:
	info = {
		"name": b"conn.bin", "piece length": PIECE_LEN,
		"pieces": b"\x00" * 20 * pieces, "length": PIECE_LEN * pieces,
	}
	return Metainfo(decode(encode({"info": info})))


def _torrent_and_peer(local_pieces=(), remote_pieces=(), wanted=None, pieces=4, metadata=True):
	"""Build a torrent + one known peer.

	``wanted`` defaults to every piece, mirroring what FileSystem attaches in production
	when no file has been deselected. ``metadata=False`` builds a magnet: no TorrentInfoEC,
	so nothing can be scored on pieces.
	"""
	env = _env()
	meta = _metainfo(pieces)
	if not metadata:
		torrent = create_torrent_entity(env, meta.make_info_hash(), Path("."), {})
		peer = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", 6881))
		return env, torrent, peer

	torrent = create_torrent_entity(env, meta.make_info_hash(), Path("."), {}, meta.info)
	# TorrentSystem attaches this to every torrent that gains metadata; candidate selection
	# reads it to order the queue, so mirror it here
	torrent.add_component(TorrentQueuePositionEC(0))
	for index in local_pieces:
		torrent.get_component(TorrentEC).bitfield.set_index(index)

	wanted_bitfield = Bitfield()
	for index in range(pieces) if wanted is None else wanted:
		wanted_bitfield.set_index(index)
	torrent.add_component(TorrentDownloadProgressEC(wanted_bitfield))

	peer = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", 6881))
	peer_ec = peer.get_component(PeerEC)
	for index in remote_pieces:
		peer_ec.remote_bitfield.set_index(index)
	return env, torrent, peer


# --- scoring for redial ----------------------------------------------------
def test_a_peer_that_told_us_nothing_is_only_an_upload_prospect():
	# an empty stored bitfield reads as "has nothing for us" but "lacks everything of ours",
	# which is why redialling on the upload score is held off by a cooldown
	_, torrent, peer = _torrent_and_peer(local_pieces=(0, 1), remote_pieces=())
	assert download_value(torrent, peer) == 0
	assert upload_value(torrent, peer) == 4


def test_peer_with_pieces_we_lack_is_a_download_candidate():
	_, torrent, peer = _torrent_and_peer(local_pieces=(0,), remote_pieces=(0, 1, 2))
	assert download_value(torrent, peer) == 2  # pieces 1 and 2
	assert upload_value(torrent, peer) == 1    # it still lacks piece 3, which we intend to hold


def test_peer_missing_our_pieces_is_an_upload_candidate():
	_, torrent, peer = _torrent_and_peer(local_pieces=(0, 1, 2), remote_pieces=(0,))
	assert upload_value(torrent, peer) == 3  # it lacks 1, 2 and 3 of our wanted set
	assert download_value(torrent, peer) == 0


def test_two_seeds_want_nothing_from_each_other():
	# the case the probe exists to detect: both complete -> release the connection
	_, torrent, peer = _torrent_and_peer(local_pieces=(0, 1, 2, 3), remote_pieces=(0, 1, 2, 3))
	assert download_value(torrent, peer) == 0
	assert upload_value(torrent, peer) == 0


def test_two_fresh_leechers_are_worth_dialling():
	# Neither holds anything yet, so a have-based test would score 0 both ways. Measured
	# against what we WANT, the peer is still worth a connection: we will have pieces to
	# trade shortly.
	_, torrent, peer = _torrent_and_peer(local_pieces=(), remote_pieces=())
	assert download_value(torrent, peer) == 0  # it genuinely has nothing for us yet
	assert upload_value(torrent, peer) == 4    # but it lacks everything we intend to hold


def test_upload_value_ignores_pieces_we_never_intend_to_hold():
	# deselected files: a peer lacking only those is no reason to hold a connection
	_, torrent, peer = _torrent_and_peer(local_pieces=(0, 1), remote_pieces=(0, 1), wanted=(0, 1))
	assert upload_value(torrent, peer) == 0
	assert download_value(torrent, peer) == 0


def test_bitfield_survives_disconnect():
	# the whole point of holding it on PeerEC: decide on a reconnect without dialling first
	_, torrent, peer = _torrent_and_peer(local_pieces=(), remote_pieces=(0, 1, 2, 3))
	assert download_value(torrent, peer) == 4

	# a disconnect strips the connection components; PeerEC and its bitfield stay
	from yap_torrent.systems.peer_system import _CONNECTION_COMPONENTS
	for component in _CONNECTION_COMPONENTS:
		if peer.has_component(component):
			peer.remove_component(component)

	assert download_value(torrent, peer) == 4


# --- redial cooldown -------------------------------------------------------
def _candidates(env, _torrent, now, free_download=8, free_upload=4):
	# selection is global now; the limits are read off config rather than passed in
	env.config.download_peers_limit = free_download
	env.config.upload_peers_limit = free_upload
	return calculate_candidates(env, now)


def test_upload_only_prospect_waits_for_the_cooldown():
	# a peer that told us nothing scores only on upload, so redialling it is a guess
	env, torrent, peer = _torrent_and_peer(local_pieces=(0, 1), remote_pieces=())
	cooldown = env.config.upload_retry_cooldown
	now = time.monotonic()

	peer.get_component(PeerEC).last_attempt = now  # just tried it
	assert peer not in _candidates(env, torrent, now)

	assert peer in _candidates(env, torrent, now + cooldown + 1)


def test_download_prospect_is_not_delayed_by_the_cooldown():
	# its bitfield is evidence, not a guess — no reason to sit on our hands
	env, torrent, peer = _torrent_and_peer(local_pieces=(), remote_pieces=(0, 1, 2, 3))
	now = time.monotonic()

	peer.get_component(PeerEC).last_attempt = now
	assert peer in _candidates(env, torrent, now)


def test_magnet_peers_are_dialled_without_a_piece_score():
	# no TorrentInfoEC means no pieces to score on, and these peers are the only source of
	# BEP-9 metadata — they must stay dialable
	env, torrent, peer = _torrent_and_peer(metadata=False)
	assert peer in _candidates(env, torrent, time.monotonic())


def test_magnet_peers_still_obey_the_connect_state_machine():
	# the metadata shortcut must not skip should_attempt: Suspicious means never dial, and a
	# failing peer's backoff has to hold on magnets too, where DHT supplies the most junk
	env, torrent, peer = _torrent_and_peer(metadata=False)
	now = time.monotonic()
	peer_ec = peer.get_component(PeerEC)

	peer_ec.state = PeerState.Suspicious
	assert peer not in _candidates(env, torrent, now)

	peer_ec.state = PeerState.Questionable
	peer_ec.last_attempt = now
	assert peer not in _candidates(env, torrent, now)                       # inside backoff
	assert peer in _candidates(env, torrent, now + QUESTIONABLE_RETRY + 1)  # backoff expired


def test_a_peer_useful_to_neither_side_is_never_redialled():
	# both complete: nothing to fetch, nothing to serve, regardless of cooldown
	env, torrent, peer = _torrent_and_peer(local_pieces=(0, 1, 2, 3), remote_pieces=(0, 1, 2, 3))
	now = time.monotonic()
	assert peer not in _candidates(env, torrent, now + env.config.upload_retry_cooldown * 10)


# --- connection lifecycle --------------------------------------------------
def test_add_component_does_not_replace_an_existing_connection():
	# the assumption _add_peer's duplicate guard rests on: a second add is silently
	# dropped, so without the guard the newer socket would leak un-closed
	_, _, peer = _torrent_and_peer()
	first = _attach_connection(peer)

	peer.add_component(PeerConnectionEC(b"h" * 20, PeerInfo("127.0.0.1", 1), _FakeConnection(), bytes(8)))

	assert peer.get_component(PeerConnectionEC).connection is first


def test_connecting_marker_is_not_torn_down_with_the_connection():
	# PeerConnectionInProgressEC belongs to the in-flight _connect task; stripping it here would let
	# _connect_to_peers dial a peer that is already being dialled
	from yap_torrent.systems.peer_system import _CONNECTION_COMPONENTS
	assert PeerConnectionInProgressEC not in _CONNECTION_COMPONENTS


def test_byte_totals_outlive_the_connection_but_rates_do_not():
	# a reconnect must not erase what the peer has given us — choke reads the total to
	# decide whether it ever reciprocated. Rates are the opposite: carried across a gap
	# they would describe a link that no longer exists.
	from yap_torrent.systems.peer_system import _CONNECTION_COMPONENTS
	assert PeerStatsEC not in _CONNECTION_COMPONENTS
	assert PeerRateEC in _CONNECTION_COMPONENTS

	env, _, peer = _torrent_and_peer()
	_attach_connection(peer)
	peer.get_component(PeerStatsEC).add_downloaded(4096)
	peer.get_component(PeerRateEC).add_downloaded(4096)

	for component in _CONNECTION_COMPONENTS:  # what _process_disconnected strips
		if peer.has_component(component):
			peer.remove_component(component)

	assert peer.get_component(PeerStatsEC).downloaded == 4096  # still reciprocated
	assert not peer.has_component(PeerRateEC)                  # stale rate is gone


def test_idle_marker_outlives_the_connection():
	# IdleEC belongs to the peer entity, not the connection: add_known_peer attaches it only
	# to entities it creates, and _add_peer touches it rather than re-adding. Tearing it down
	# with the connection would crash the next _add_peer on any reconnect.
	from yap_torrent.systems.peer_system import _CONNECTION_COMPONENTS
	assert IdleEC not in _CONNECTION_COMPONENTS

	env, torrent, peer = _torrent_and_peer()
	_attach_connection(peer)

	for component in _CONNECTION_COMPONENTS:  # what _process_disconnected strips
		if peer.has_component(component):
			peer.remove_component(component)

	# the redial path: find-or-create returns the surviving entity (so nothing re-attaches
	# IdleEC), and _add_peer touches it — this raises if teardown took it away
	again = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", 6881))
	assert again is peer
	again.get_component(IdleEC).touch()


def test_idle_timer_runs_from_leaving_the_queue_not_from_connect():
	env, _, peer = _torrent_and_peer()
	env.config.peer_idle_timeout = 30
	_attach_connection(peer)
	idle = peer.get_component(IdleEC)

	# a long-lived connection still holding a queue place is refreshed, never dropped
	_backdate(idle, 600)
	for component in (LocalInterestedEC, RemoteUnchokedEC):
		peer.add_component(component())
	PeerSystem(env)._drop_idle_connections()
	assert not peer.has_component(PeerDisconnectedEC)
	assert not idle.overlives_period(1)

	# once it leaves it gets the full timeout from that moment, rather than being
	# judged on how long ago it connected
	peer.remove_component(LocalInterestedEC)
	PeerSystem(env)._drop_idle_connections()
	assert not peer.has_component(PeerDisconnectedEC)

	_backdate(idle, 31)
	PeerSystem(env)._drop_idle_connections()
	assert peer.has_component(PeerDisconnectedEC)


# --- the two queues are the protocol pairs ---------------------------------
def test_queue_membership_is_the_protocol_pair():
	_, _, peer = _torrent_and_peer()
	assert not _in_any_queue(peer)

	# half a pair is not membership
	peer.add_component(LocalInterestedEC())
	assert not _in_download_queue(peer)
	peer.add_component(RemoteUnchokedEC())
	assert _in_download_queue(peer)
	assert _in_any_queue(peer)

	peer.add_component(RemoteInterestedEC())
	assert not _in_upload_queue(peer)
	peer.add_component(LocalUnchokedEC())
	assert _in_upload_queue(peer)


def test_a_peer_can_hold_a_slot_in_both_queues():
	_, _, peer = _torrent_and_peer()
	for component in (LocalInterestedEC, RemoteUnchokedEC, RemoteInterestedEC, LocalUnchokedEC):
		peer.add_component(component())
	assert _in_download_queue(peer) and _in_upload_queue(peer)


def test_new_peers_start_unknown():
	_, _, peer = _torrent_and_peer()
	assert peer.get_component(PeerEC).state == PeerState.Unknown


def test_torrent_without_metadata_has_no_pieces_to_judge():
	# magnet: no TorrentInfoEC, so classification must never run against it
	env = _env()
	torrent = create_torrent_entity(env, b"\x01" * 20, Path("."), {})
	assert not torrent.has_component(TorrentInfoEC)
