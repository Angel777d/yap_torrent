"""Tests for the piece download progress + wanted-mask logic (Part B/D)."""
import time
from pathlib import Path

from yap_torrent.components.file_ec import TorrentFileEC, TorrentFileStateEC, FilePriority
from yap_torrent.components.peer_ec import PeerConnectionEC, PeerEC, PeerRateEC
from yap_torrent.components.piece_ec import PieceDownloadProgressEC
from yap_torrent.components.torrent_ec import (
	TorrentDownloadProgressEC,
	TorrentEC,
	TorrentInfoEC,
	TorrentPieceAvailabilityEC,
)
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import PieceInfo, Metainfo, Bitfield, PeerInfo
from yap_torrent.systems import (
	add_known_peer,
	compute_wanted_bitfield,
	create_torrent_entity,
	get_info_hash,
	interested_pieces,
)
from yap_torrent.systems.download_system import _find_rarest, _rarest_order


def _wanted(env, torrent):
	return compute_wanted_bitfield(env, get_info_hash(torrent), torrent.get_component(TorrentInfoEC).info)


# --- piece progress --------------------------------------------------------
def test_piece_progress_accumulates_and_completes():
	# a 3-block piece: 16384 + 16384 + 100
	info = PieceInfo(size=16384 * 2 + 100, index=0, piece_hash=b"\x00" * 20)
	progress = PieceDownloadProgressEC(info)

	# request every block exactly once
	requested = []
	block = progress.next_block()
	while block is not None:
		progress.mark_requested(block)
		requested.append(block)
		block = progress.next_block()
	assert len(requested) == 3
	assert progress.all_requested()
	assert not progress.is_full()

	# deliver them
	for i, block in enumerate(sorted(requested, key=lambda b: b.begin)):
		full = progress.add_block(block.begin, b"x" * block.length)
		assert full == (i == 2)
	assert progress.is_full()


def test_duplicate_block_is_ignored():
	info = PieceInfo(size=16384, index=0, piece_hash=b"\x00" * 20)
	progress = PieceDownloadProgressEC(info)
	assert progress.add_block(0, b"x" * 16384) is True
	# adding again does not break completeness
	assert progress.add_block(0, b"x" * 16384) is True


def test_block_of_the_wrong_length_is_dropped():
	# data is written by slice, so a length we never asked for would resize the buffer
	# and the piece could never hash again
	info = PieceInfo(size=16384 * 2, index=0, piece_hash=b"\x00" * 20)
	progress = PieceDownloadProgressEC(info)
	assert progress.add_block(0, b"x" * 20000) is False
	assert len(progress.data) == 16384 * 2
	assert len(progress.missing_blocks()) == 2


def test_block_at_an_unknown_offset_is_dropped():
	# a bogus offset would count towards is_full() and complete a piece full of holes
	info = PieceInfo(size=16384 * 2, index=0, piece_hash=b"\x00" * 20)
	progress = PieceDownloadProgressEC(info)
	progress.add_block(0, b"x" * 16384)
	assert progress.add_block(999, b"y" * 16384) is False
	assert progress.is_full() is False


def test_missing_blocks_shrinks_as_received():
	# 2 blocks: 16384 + 100
	info = PieceInfo(size=16384 + 100, index=0, piece_hash=b"\x00" * 20)
	progress = PieceDownloadProgressEC(info)
	assert len(progress.missing_blocks()) == 2  # endgame candidates before any data
	progress.add_block(0, b"x" * 16384)
	missing = progress.missing_blocks()
	assert len(missing) == 1 and missing[0].begin == 16384
	progress.add_block(16384, b"y" * 100)
	assert progress.missing_blocks() == []


# --- wanted-mask -----------------------------------------------------------
def _make_torrent(env):
	# 3 files across pieces (piece length 16384): sizes chosen to give known ranges
	info = {
		"name": b"t",
		"piece length": 16384,
		"pieces": b"\x00" * 20 * 5,
		"files": [
			{"path": [b"a"], "length": 10000},   # pieces 0
			{"path": [b"b"], "length": 20000},   # pieces 0..1
			{"path": [b"c"], "length": 40000},   # pieces 1..4
		],
	}
	metainfo = Metainfo(decode(encode({"info": info})))
	return create_torrent_entity(env, metainfo.make_info_hash(), Path("D:/dl"), {}, metainfo.info)


def _add_file(env, info_hash, index, path, first_piece, pieces_length, wanted):
	e = env.data_storage.create_entity()
	e.add_component(TorrentFileEC(info_hash, index, path, first_piece, pieces_length))
	e.add_component(TorrentFileStateEC(wanted))


def test_compute_wanted_defaults_to_all_pieces_without_files():
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	torrent = _make_torrent(env)
	wanted = _wanted(env, torrent)
	assert wanted.have_num == 5  # all pieces


def test_compute_wanted_excludes_unwanted_files():
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	torrent = _make_torrent(env)
	info_hash = torrent.get_component(TorrentEC).info_hash
	# only file c wanted (pieces 1..4); a and b unwanted (pieces 0..1)
	_add_file(env, info_hash, 0, "a", 0, 1, wanted=False)
	_add_file(env, info_hash, 1, "b", 0, 2, wanted=False)
	_add_file(env, info_hash, 2, "c", 1, 4, wanted=True)

	wanted = _wanted(env, torrent)
	# pieces 1,2,3,4 wanted; piece 0 not
	assert wanted.have_index(0) is False
	assert all(wanted.have_index(i) for i in (1, 2, 3, 4))


def test_interested_pieces_respects_wanted_mask():
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))
	torrent = _make_torrent(env)
	# want only pieces 1..4
	wanted = Bitfield()
	for i in (1, 2, 3, 4):
		wanted.set_index(i)
	torrent.add_component(TorrentDownloadProgressEC(wanted))

	# a remote peer that has every piece
	remote = Bitfield()
	for i in range(5):
		remote.set_index(i)

	interested = interested_pieces(torrent, remote)
	assert interested == {1, 2, 3, 4}  # piece 0 masked out


# --- rarest-first availability ---------------------------------------------
# One order per torrent, shared by every peer of it. Counts are only ever raised between
# rebuilds — nothing decrements — so a peer leaving throws the count away rather than
# trusting a delta, and the order can never quietly describe a swarm that has gone.
def _availability(holdings, wanted):
	availability = TorrentPieceAvailabilityEC()
	availability.rebuild(holdings, wanted)
	return availability


def _env():
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__none__.json"))


def _torrent(env, held=(4, 5, 6, 7)):
	"""An 8-piece torrent already holding `held`.

	Four held pieces by default, because below that `_find_rarest` deliberately picks at
	random — the warm-up that gets us something to reciprocate with before rarity is worth
	anything. A test of the *order* has to be past it.
	"""
	info = {"name": b"t", "piece length": 16384, "pieces": b"\x00" * 20 * 8, "length": 16384 * 8}
	metainfo = Metainfo(decode(encode({"info": info})))
	torrent = create_torrent_entity(env, metainfo.make_info_hash(), Path("D:/dl"), {}, metainfo.info)
	for index in held:
		torrent.get_component(TorrentEC).bitfield.set_index(index)
	return torrent


class _FakeConnection:
	def __init__(self):
		self.connection_time = time.monotonic()

	async def send(self, message):
		pass

	def close(self):
		pass


def _peer(env, torrent, port, holds):
	"""A connected peer of the torrent holding exactly `holds`."""
	info_hash = get_info_hash(torrent)
	entity = add_known_peer(env, info_hash, PeerInfo("127.0.0.1", port))
	for index in holds:
		entity.get_component(PeerEC).remote_bitfield.set_index(index)
	entity.add_component(PeerConnectionEC(
		info_hash, entity.get_component(PeerEC).peer_info, _FakeConnection(), bytes(8)))
	entity.add_component(PeerRateEC())
	return entity


def test_pieces_are_ordered_by_how_many_peers_hold_them():
	# piece 0 held by one peer, 1 by two, 2 by three
	availability = _availability([{0, 1, 2}, {1, 2}, {2}], {0, 1, 2})

	assert availability.rarest_first() == [0, 1, 2]
	assert [availability.count(i) for i in (0, 1, 2)] == [1, 2, 3]


def test_a_piece_nobody_holds_is_left_out_of_the_order():
	# unobtainable, not rarest: asking for it would stall the pipeline on nothing
	availability = _availability([{0}], {0, 1})

	assert availability.rarest_first() == [0]
	assert availability.count(1) == 0


def test_only_wanted_pieces_are_counted():
	# the order exists to choose what to download; a piece outside the wanted mask is not
	# a candidate however many peers have it
	availability = _availability([{0, 1, 2}, {2}], {0, 1})

	assert availability.rarest_first() == [0, 1]
	assert availability.count(2) == 0


def test_a_have_raises_a_count_and_reorders():
	availability = _availability([{0, 1}, {1}], {0, 1})
	assert availability.rarest_first() == [0, 1]

	availability.add_have(0)  # now two peers hold piece 0, same as piece 1
	availability.add_have(0)  # and now three

	assert availability.count(0) == 3
	assert availability.rarest_first() == [1, 0]


def test_a_have_for_an_untracked_piece_is_ignored():
	# a peer announcing something we do not want must not add it to the candidates
	availability = _availability([{0}], {0})

	availability.add_have(7)

	assert availability.count(7) == 0
	assert availability.rarest_first() == [0]


def test_a_finished_piece_leaves_the_order():
	availability = _availability([{0, 1}, {1}], {0, 1})

	availability.drop(0)

	assert availability.rarest_first() == [1]
	assert availability.count(0) == 0


def test_rarest_of_picks_the_rarest_candidate_not_the_rarest_piece():
	# a peer is only offered what it actually holds, so the global rarest may not be on offer
	availability = _availability([{0, 1, 2}, {1, 2}, {2}], {0, 1, 2})

	assert availability.rarest_of({1, 2}) == 1  # 0 is rarer but is not a candidate
	assert availability.rarest_of({2}) == 2


def test_rarest_of_still_answers_before_anything_has_been_counted():
	# a fresh torrent has no counts yet; selection must not fail on the first block
	availability = TorrentPieceAvailabilityEC()

	assert availability.rarest_of({4}) == 4


def test_a_peer_leaving_forces_a_recount_rather_than_a_decrement():
	availability = _availability([{0, 1}, {1}], {0, 1})
	assert availability.needs_rebuild is False

	availability.invalidate()

	assert availability.needs_rebuild is True
	availability.rebuild([{1}], {0, 1})  # the peer holding 0 has gone
	assert availability.rarest_first() == [1]
	assert availability.count(0) == 0


def test_the_order_is_rebuilt_from_the_connected_peers():
	# the wiring: what the download path reads has to come from the peers actually there
	env = _env()
	torrent = _torrent(env)
	_peer(env, torrent, 6801, holds={0, 1, 2})
	_peer(env, torrent, 6802, holds={1, 2})
	_peer(env, torrent, 6803, holds={2})

	order = _rarest_order(env, torrent)

	assert order.rarest_first() == [0, 1, 2]  # piece 3 is wanted but nobody has it
	assert _find_rarest(env, torrent, {1, 2}) == 1


def test_pieces_we_already_hold_are_not_in_the_order():
	env = _env()
	torrent = _torrent(env)  # holds 4..7
	_peer(env, torrent, 6801, holds=set(range(8)))

	assert _rarest_order(env, torrent).rarest_first() == [0, 1, 2, 3]


def test_a_recount_is_only_done_when_the_swarm_moved():
	env = _env()
	torrent = _torrent(env)
	_peer(env, torrent, 6801, holds={0, 1})

	first = _rarest_order(env, torrent)
	assert first.needs_rebuild is False

	_rarest_order(env, torrent)  # second read must not recount
	assert first.needs_rebuild is False

	first.invalidate()
	assert _rarest_order(env, torrent).needs_rebuild is False  # read repairs it
