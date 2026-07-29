"""Tests for the piece download progress + wanted-mask logic (Part B/D)."""
from pathlib import Path

from yap_torrent.components.file_ec import TorrentFileEC, TorrentFileStateEC, FilePriority
from yap_torrent.components.piece_ec import PieceDownloadProgressEC
from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentEC, TorrentInfoEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import PieceInfo, Metainfo, Bitfield
from yap_torrent.systems import compute_wanted_bitfield, create_torrent_entity, get_info_hash
from yap_torrent.systems.intrest_system import interested_pieces


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
