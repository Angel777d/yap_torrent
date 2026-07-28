"""Tests for the download queue: torrent priority + the global peer limit.

Peers are selected globally now — `download_peers_limit` / `upload_peers_limit` cap the
whole client rather than each torrent — and contested slots go to the peer whose torrent
has the lower priority number. TorrentSystem is left owning only the priority numbers
themselves; `peer_system.calculate_candidates` does the selection.
"""
import asyncio
import time
from pathlib import Path

from yap_torrent.components.peer_ec import PeerConnectionInProgressEC, PeerEC
from yap_torrent.components.torrent_ec import TorrentPriorityEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo, PeerInfo
from yap_torrent.systems import add_known_peer, create_torrent_entity, get_info_hash
from yap_torrent.systems.peer_system import calculate_candidates
from yap_torrent.systems.torrents_system import TorrentSystem

PIECES = 4


def _env(download_limit: int = 8) -> Env:
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	env.config.download_peers_limit = download_limit
	return env


def _make(env: Env, name: str, priority: int = None):
	info = {
		"name": name.encode(), "piece length": 16384,
		"pieces": b"\x00" * 20 * PIECES, "length": 16384 * PIECES,
	}
	meta = Metainfo(decode(encode({"info": info})))
	entity = create_torrent_entity(env, meta.make_info_hash(), Path("D:/dl"), {}, meta.info)
	if priority is not None:
		entity.add_component(TorrentPriorityEC(priority))
	return entity


def _peer(env: Env, torrent, port: int, pieces: int = PIECES):
	"""A known, disconnected peer holding `pieces` pieces we lack — a download candidate."""
	entity = add_known_peer(env, get_info_hash(torrent), PeerInfo("127.0.0.1", port))
	for index in range(pieces):
		entity.get_component(PeerEC).remote_bitfield.set_index(index)
	return entity


def _priorities(*torrents):
	return [t.get_component(TorrentPriorityEC).priority for t in torrents]


# --- priority assignment (TorrentSystem) -----------------------------------
def test_new_torrents_take_the_next_free_position():
	async def run():
		env = _env()
		t1, t2, t3 = (_make(env, f"t{i}") for i in range(1, 4))
		await TorrentSystem(env).start()  # processes existing torrents in creation order
		assert _priorities(t1, t2, t3) == [0, 1, 2]

	asyncio.run(run())


def test_a_restored_priority_survives_the_torrent_added_hook():
	# LocalDataSystem re-attaches the saved TorrentPriorityEC before TorrentSystem sees the
	# torrent; without the guard the hook would overwrite it with "append to the end"
	async def run():
		env = _env()
		first = _make(env, "first")
		restored = _make(env, "restored", priority=0)  # saved as the head of the queue

		await TorrentSystem(env).start()

		assert restored.get_component(TorrentPriorityEC).priority == 0
		assert first.get_component(TorrentPriorityEC).priority != 0

	asyncio.run(run())


def test_remove_compacts_priorities():
	async def run():
		env = _env()
		t1, t2, t3 = (_make(env, f"t{i}") for i in range(1, 4))
		system = TorrentSystem(env)
		await system.start()
		assert _priorities(t1, t2, t3) == [0, 1, 2]

		await system._on_torrent_remove(get_info_hash(t1))

		# the survivors close the gap rather than keeping 1 and 2
		assert _priorities(t2, t3) == [0, 1]

	asyncio.run(run())


# --- global candidate selection (peer_system) ------------------------------
def test_download_candidates_are_capped_by_the_global_limit():
	# the cap is client-wide now: three willing peers across two torrents, two slots
	env = _env(download_limit=2)
	a, b = _make(env, "a", priority=0), _make(env, "b", priority=1)
	_peer(env, a, 6801), _peer(env, a, 6802), _peer(env, b, 6803)

	assert len(calculate_candidates(env, time.monotonic())) == 2


def test_a_higher_priority_torrent_wins_the_contested_slot():
	# one slot, one peer each: the torrent nearer the head of the queue takes it
	env = _env(download_limit=1)
	head = _make(env, "head", priority=0)
	tail = _make(env, "tail", priority=1)
	head_peer = _peer(env, head, 6801)
	_peer(env, tail, 6802)

	assert calculate_candidates(env, time.monotonic()) == {head_peer}


def test_priority_outranks_how_much_a_peer_offers():
	# the tail torrent's peer has more of what we want, but priority decides first
	env = _env(download_limit=1)
	head = _make(env, "head", priority=0)
	tail = _make(env, "tail", priority=1)
	head_peer = _peer(env, head, 6801, pieces=1)
	_peer(env, tail, 6802, pieces=PIECES)

	assert calculate_candidates(env, time.monotonic()) == {head_peer}


def test_within_one_torrent_the_peer_offering_most_wins():
	env = _env(download_limit=1)
	torrent = _make(env, "one", priority=0)
	_peer(env, torrent, 6801, pieces=1)
	rich = _peer(env, torrent, 6802, pieces=PIECES)

	assert calculate_candidates(env, time.monotonic()) == {rich}


def test_a_connected_peer_is_not_a_candidate_again():
	env = _env(download_limit=8)
	torrent = _make(env, "one", priority=0)
	peer = _peer(env, torrent, 6801)
	assert calculate_candidates(env, time.monotonic()) == {peer}

	peer.add_component(PeerConnectionInProgressEC())
	assert calculate_candidates(env, time.monotonic()) == set()
