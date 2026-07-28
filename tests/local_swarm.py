#!/usr/bin/env python3
"""Local swarm integration tests for the engine cutover.

Stands up real in-process instances on 127.0.0.1 (no UPnP/DHT/tracker/plugins)
and drives the tick loop over real sockets. Scenarios:

  1. basic transfer         seeder -> leecher, full file, byte-for-byte match
  2. paused seeder          an Inactive seeder uploads nothing
  3. partial selection      leecher with an unwanted file downloads only wanted pieces
  4. peer state machine     a failed connect moves a peer Unknown -> Questionable

Run:  python tests/local_swarm.py     (exits 0 if all scenarios pass)
"""
import asyncio
import hashlib
import shutil
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT / "src", _ROOT.parent / "py_core"):
	if _p.is_dir() and str(_p) not in sys.path:
		sys.path.insert(0, str(_p))

from yap_torrent.components.file_ec import TorrentFileEC, TorrentFileStateEC
from yap_torrent.components.peer_ec import PeerDisconnectedEC, PeerEC, PeerState
from yap_torrent.components.piece_ec import CompletePieceDataEC, PieceEC
from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentEC, TorrentState, TorrentStatsEC
from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import decode, encode
from yap_torrent.protocol.structures import Metainfo, PeerInfo
from yap_torrent.systems import (
	compute_wanted_bitfield,
	create_torrent_entity,
	find_peer_entity,
	get_torrent_entity,
	is_torrent_complete,
	iterate_files,
	iterate_peers,
	iterate_connected_peers,
)
from yap_torrent.systems.choke_system import ChokeSystem
from yap_torrent.systems.download_system import DownloadSystem
from yap_torrent.systems.file_system import FileSystem
from yap_torrent.systems.intrest_system import InterestedSystem
from yap_torrent.systems.peer_system import PeerSystem
from yap_torrent.systems.piece_system import PieceSystem
from yap_torrent.systems.torrents_system import TorrentSystem
from yap_torrent.systems.upload_system import UploadSystem

PIECE_LEN = 16384


# --- metainfo builders -----------------------------------------------------
def _hashes(content: bytes) -> bytes:
	return b"".join(hashlib.sha1(content[i:i + PIECE_LEN]).digest() for i in range(0, len(content), PIECE_LEN))


def single_file(content: bytes, name: str = "swarmfile.bin") -> Metainfo:
	info = {"name": name.encode(), "piece length": PIECE_LEN, "pieces": _hashes(content), "length": len(content)}
	return Metainfo(decode(encode({"info": info})))


def multi_file(files, folder: str = "swarmdir") -> tuple:
	content = b"".join(c for _, c in files)
	info = {
		"name": folder.encode(), "piece length": PIECE_LEN, "pieces": _hashes(content),
		"files": [{"path": [n.encode()], "length": len(c)} for n, c in files],
	}
	return Metainfo(decode(encode({"info": info}))), content


def write_files(root: Path, info, content: bytes):
	for file in info.files:
		path = info.get_file_path(root, file)
		path.parent.mkdir(parents=True, exist_ok=True)
		path.write_bytes(content[file.start:file.start + file.length])


# --- instance harness ------------------------------------------------------
class Instance:
	def __init__(self, port: int, root: Path, peer_id: bytes, dht: bool = False):
		root.mkdir(parents=True, exist_ok=True)
		cfg = Config(path=str(root / "__missing__.json"))  # missing -> in-memory defaults
		cfg.data_folder = root
		cfg.download_folder = root / "download"
		cfg.active_folder = root / "active"
		cfg.peers_file = str(root / "peers.dat")
		cfg.port = port
		cfg.dht_port = port + 1000
		cfg.download_folder.mkdir(parents=True, exist_ok=True)

		self.env = Env(peer_id, "127.0.0.1", "127.0.0.1", cfg)
		self.systems = [
			FileSystem(self.env), PeerSystem(self.env), ChokeSystem(self.env), InterestedSystem(self.env),
			DownloadSystem(self.env), UploadSystem(self.env), PieceSystem(self.env), TorrentSystem(self.env),
		]
		self.dht = None
		if dht:
			from yap_torrent.systems.dht_system import DHTSystem
			self.dht = DHTSystem(self.env)
			self.systems.append(self.dht)

	async def start(self):
		for s in self.systems:
			await s.start()

	async def tick(self, dt: float = 0.01):
		for s in self.systems:
			await s.update(dt)

	async def stop(self):
		for s in self.systems:
			await s.stop()
		for s in self.systems:
			s.close()


async def settle(instances, rounds: int, sleep: float = 0.05):
	for _ in range(rounds):
		for inst in instances:
			await inst.tick()
		await asyncio.sleep(sleep)


async def run_until(instances, predicate, rounds: int = 80, sleep: float = 0.1) -> bool:
	for _ in range(rounds):
		for inst in instances:
			await inst.tick()
		await asyncio.sleep(sleep)
		if predicate():
			return True
	return False


def reconstruct(env, info_hash: bytes, pieces_num: int, size: int) -> bytes:
	out = bytearray()
	for index in range(pieces_num):
		e = env.data_storage.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		if e is None or not e.has_component(CompletePieceDataEC):
			return b""
		out += e.get_component(CompletePieceDataEC).data
	return bytes(out[:size])


# --- scenarios -------------------------------------------------------------
async def scenario_basic_transfer(work: Path) -> bool:
	content = __import__("os").urandom(20000)  # 2 pieces
	meta = single_file(content)
	ih = meta.make_info_hash()

	seeder = Instance(6801, work / "s1_seed", b"-PY0001-SEEDER000001")
	leecher = Instance(6802, work / "s1_leech", b"-PY0001-LEECHER00001")
	write_files(seeder.env.config.download_folder, meta.info, content)
	await seeder.start();
	await leecher.start()

	seed = create_torrent_entity(seeder.env, ih, seeder.env.config.download_folder, {}, meta.info)
	for i in range(meta.info.pieces_num):
		seed.get_component(TorrentEC).bitfield.set_index(i)
	create_torrent_entity(leecher.env, ih, leecher.env.config.download_folder, {}, meta.info)

	await settle([seeder, leecher], 3)
	leecher.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6801)])
	leech = get_torrent_entity(leecher.env, ih)

	ok = await run_until([seeder, leecher], lambda: is_torrent_complete(leech))
	ok = ok and reconstruct(leecher.env, ih, meta.info.pieces_num, len(content)) == content
	await leecher.stop();
	await seeder.stop()
	return ok


async def scenario_paused_seeder(work: Path) -> bool:
	content = __import__("os").urandom(20000)
	meta = single_file(content)
	ih = meta.make_info_hash()

	seeder = Instance(6811, work / "s2_seed", b"-PY0001-SEEDER000002")
	leecher = Instance(6812, work / "s2_leech", b"-PY0001-LEECHER00002")
	write_files(seeder.env.config.download_folder, meta.info, content)
	await seeder.start();
	await leecher.start()

	seed = create_torrent_entity(seeder.env, ih, seeder.env.config.download_folder, {}, meta.info)
	for i in range(meta.info.pieces_num):
		seed.get_component(TorrentEC).bitfield.set_index(i)
	seed.get_component(TorrentStatsEC).state = TorrentState.Inactive  # PAUSED -> must not upload
	create_torrent_entity(leecher.env, ih, leecher.env.config.download_folder, {}, meta.info)

	await settle([seeder, leecher], 3)
	leecher.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6811)])
	leech = get_torrent_entity(leecher.env, ih)

	# expect: leecher never completes, seeder uploads nothing
	completed = await run_until([seeder, leecher], lambda: is_torrent_complete(leech), rounds=40)
	uploaded = seed.get_component(TorrentStatsEC).uploaded
	await leecher.stop();
	await seeder.stop()
	return (not completed) and uploaded == 0


async def scenario_partial_selection(work: Path) -> bool:
	# file a = pieces 0..1 (unwanted), file b = pieces 2..3 (wanted)
	meta, content = multi_file([("a.bin", __import__("os").urandom(PIECE_LEN * 2)),
	                            ("b.bin", __import__("os").urandom(PIECE_LEN * 2))])
	ih = meta.make_info_hash()

	seeder = Instance(6821, work / "s3_seed", b"-PY0001-SEEDER000003")
	leecher = Instance(6822, work / "s3_leech", b"-PY0001-LEECHER00003")
	write_files(seeder.env.config.download_folder, meta.info, content)
	await seeder.start();
	await leecher.start()

	seed = create_torrent_entity(seeder.env, ih, seeder.env.config.download_folder, {}, meta.info)
	for i in range(meta.info.pieces_num):
		seed.get_component(TorrentEC).bitfield.set_index(i)
	leech = create_torrent_entity(leecher.env, ih, leecher.env.config.download_folder, {}, meta.info)

	# let file entities materialize + initial TorrentDownloadProgressEC form, then mark file a unwanted
	await settle([seeder, leecher], 3)
	for file_entity in iterate_files(leecher.env, ih):
		if file_entity.get_component(TorrentFileEC).index == 0:  # file a
			file_entity.get_component(TorrentFileStateEC).wanted = False
	# recompute the wanted mask from the new file selection (event-driven TorrentSystem
	# only computes it on metadata-add, so a selection change must refresh it explicitly)
	if leech.has_component(TorrentDownloadProgressEC):
		leech.remove_component(TorrentDownloadProgressEC)
	leech.add_component(TorrentDownloadProgressEC(compute_wanted_bitfield(leecher.env, ih, meta.info)))
	await settle([leecher], 2)

	leecher.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6821)])

	bitfield = leech.get_component(TorrentEC).bitfield
	# the torrent must COMPLETE with only the wanted pieces (2,3), never fetching 0,1
	completed = await run_until([seeder, leecher], lambda: is_torrent_complete(leech))
	ok = (completed
	      and bitfield.have_index(2) and bitfield.have_index(3)
	      and not bitfield.have_index(0) and not bitfield.have_index(1)
	      and bitfield.have_num == 2)
	await leecher.stop();
	await seeder.stop()
	return ok


async def scenario_peer_state_machine(work: Path) -> bool:
	content = __import__("os").urandom(20000)
	meta = single_file(content)
	ih = meta.make_info_hash()

	leecher = Instance(6832, work / "s4_leech", b"-PY0001-LEECHER00004")
	await leecher.start()
	create_torrent_entity(leecher.env, ih, leecher.env.config.download_folder, {}, meta.info)
	await settle([leecher], 3)

	# a peer at a dead port -> connect must fail and demote the peer to Questionable
	leecher.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 9)])

	def demoted():
		p = find_peer_entity(leecher.env, ih, "127.0.0.1", 9)
		return p is not None and p.get_component(PeerEC).state == PeerState.Questionable

	ok = await run_until([leecher], demoted, rounds=40, sleep=0.15)
	peer = find_peer_entity(leecher.env, ih, "127.0.0.1", 9)
	ok = ok and peer is not None and peer.get_component(PeerEC).fail_count >= 1
	await leecher.stop()
	return ok


async def scenario_multi_leecher(work: Path) -> bool:
	# one seeder, two leechers that also share with each other (swarm + endgame)
	content = __import__("os").urandom(50000)  # 4 pieces
	meta = single_file(content)
	ih = meta.make_info_hash()

	seeder = Instance(6841, work / "s5_seed", b"-PY0001-SEEDER000005")
	l1 = Instance(6842, work / "s5_l1", b"-PY0001-LEECHER00051")
	l2 = Instance(6843, work / "s5_l2", b"-PY0001-LEECHER00052")
	write_files(seeder.env.config.download_folder, meta.info, content)
	for inst in (seeder, l1, l2):
		await inst.start()

	seed = create_torrent_entity(seeder.env, ih, seeder.env.config.download_folder, {}, meta.info)
	for i in range(meta.info.pieces_num):
		seed.get_component(TorrentEC).bitfield.set_index(i)
	create_torrent_entity(l1.env, ih, l1.env.config.download_folder, {}, meta.info)
	create_torrent_entity(l2.env, ih, l2.env.config.download_folder, {}, meta.info)

	await settle([seeder, l1, l2], 3)
	# both leechers know the seeder and each other
	l1.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6841), PeerInfo("127.0.0.1", 6843)])
	l2.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6841), PeerInfo("127.0.0.1", 6842)])

	t1 = get_torrent_entity(l1.env, ih)
	t2 = get_torrent_entity(l2.env, ih)
	both = await run_until([seeder, l1, l2],
	                       lambda: is_torrent_complete(t1) and is_torrent_complete(t2), rounds=120)
	ok = (both
	      and reconstruct(l1.env, ih, meta.info.pieces_num, len(content)) == content
	      and reconstruct(l2.env, ih, meta.info.pieces_num, len(content)) == content)
	for inst in (l1, l2, seeder):
		await inst.stop()
	return ok


async def scenario_choke_over_limit(work: Path) -> bool:
	# seeder with upload_peers_limit=1 + two interested leechers -> exactly one unchoked.
	# Works now that peers are keyed by (info_hash, host, port): the two inbound leechers
	# arrive on distinct source ports, so the seeder holds them as two distinct peers.
	from yap_torrent.components.peer_ec import LocalUnchokedEC, PeerConnectionEC

	content = __import__("os").urandom(20000)
	meta = single_file(content)
	ih = meta.make_info_hash()

	seeder = Instance(6851, work / "s6_seed", b"-PY0001-SEEDER000006")
	seeder.env.config.upload_peers_limit = 1
	l1 = Instance(6852, work / "s6_l1", b"-PY0001-LEECHER00061")
	l2 = Instance(6853, work / "s6_l2", b"-PY0001-LEECHER00062")
	write_files(seeder.env.config.download_folder, meta.info, content)
	for inst in (seeder, l1, l2):
		await inst.start()

	seed = create_torrent_entity(seeder.env, ih, seeder.env.config.download_folder, {}, meta.info)
	for i in range(meta.info.pieces_num):
		seed.get_component(TorrentEC).bitfield.set_index(i)
	create_torrent_entity(l1.env, ih, l1.env.config.download_folder, {}, meta.info)
	create_torrent_entity(l2.env, ih, l2.env.config.download_folder, {}, meta.info)

	await settle([seeder, l1, l2], 3)
	l1.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6851)])
	l2.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6851)])

	# sample the seeder over the run: it must hold both leechers at some point, yet the
	# upload queue (unchoked peers) must never exceed the limit. (A completed leecher
	# disconnects, so a single post-hoc snapshot would be racy.)
	limit = seeder.env.config.upload_peers_limit
	max_connected = max_unchoked = 0
	over_limit = False
	for _ in range(60):
		for inst in (seeder, l1, l2):
			await inst.tick()
		await asyncio.sleep(0.1)
		conns = list(seeder.env.data_storage.get_collection(PeerConnectionEC))
		unchoked = sum(1 for e in conns if e.has_component(LocalUnchokedEC))
		max_connected = max(max_connected, len(conns))
		max_unchoked = max(max_unchoked, unchoked)
		if unchoked > limit:
			over_limit = True

	ok = max_connected >= 2 and max_unchoked >= 1 and not over_limit
	for inst in (l1, l2, seeder):
		await inst.stop()
	return ok


async def scenario_dht_discovery(work: Path) -> bool:
	# real DHT path: the seeder announces itself to a bootstrap node (the leecher's DHT
	# node), the announce reaches the leecher which then BT-connects and downloads. Only
	# the bootstrap (seeder knows the leecher's DHT node) is faked — the announce, token,
	# get_peers and discovery all run over real UDP KRPC.
	content = __import__("os").urandom(20000)
	meta = single_file(content)
	ih = meta.make_info_hash()

	seeder = Instance(6861, work / "s7_seed", b"-PY0001-SEEDER000007", dht=True)
	leecher = Instance(6862, work / "s7_leech", b"-PY0001-LEECHER00007", dht=True)
	write_files(seeder.env.config.download_folder, meta.info, content)
	await seeder.start()
	await leecher.start()

	seed = create_torrent_entity(seeder.env, ih, seeder.env.config.download_folder, {}, meta.info)
	for i in range(meta.info.pieces_num):
		seed.get_component(TorrentEC).bitfield.set_index(i)
	create_torrent_entity(leecher.env, ih, leecher.env.config.download_folder, {}, meta.info)

	await settle([seeder, leecher], 3)

	# fake bootstrap: the seeder knows the leecher's DHT node...
	seeder.dht.pending_nodes.append((leecher.dht._my_node_id, "127.0.0.1", leecher.env.config.dht_port))
	# ...and gets asked to publish itself to the DHT for this torrent
	seeder.env.event_bus.dispatch("request.torrent.dht_ask_peers", ih)

	leech = get_torrent_entity(leecher.env, ih)
	completed = await run_until([seeder, leecher], lambda: is_torrent_complete(leech), rounds=120)
	ok = completed and reconstruct(leecher.env, ih, meta.info.pieces_num, len(content)) == content
	await leecher.stop()
	await seeder.stop()
	return ok


async def scenario_queue_promotion(work: Path) -> bool:
	# Two torrents, one client-wide download slot. A is created first so it holds the lower
	# priority number and takes the slot; B only gets dialled once A completes and releases
	# it. Both must finish — a stuck promotion shows up as B never starting.
	content_a = __import__("os").urandom(20000)
	content_b = __import__("os").urandom(20000)
	meta_a = single_file(content_a, "a.bin")
	meta_b = single_file(content_b, "b.bin")
	ih_a, ih_b = meta_a.make_info_hash(), meta_b.make_info_hash()

	seeder = Instance(6871, work / "s8_seed", b"-PY0001-SEEDER000008")
	leecher = Instance(6872, work / "s8_leech", b"-PY0001-LEECHER00008")
	leecher.env.config.download_peers_limit = 1
	write_files(seeder.env.config.download_folder, meta_a.info, content_a)
	write_files(seeder.env.config.download_folder, meta_b.info, content_b)
	await seeder.start()
	await leecher.start()

	for meta in (meta_a, meta_b):
		s = create_torrent_entity(seeder.env, meta.make_info_hash(), seeder.env.config.download_folder, {}, meta.info)
		for i in range(meta.info.pieces_num):
			s.get_component(TorrentEC).bitfield.set_index(i)
	create_torrent_entity(leecher.env, ih_a, leecher.env.config.download_folder, {}, meta_a.info)
	create_torrent_entity(leecher.env, ih_b, leecher.env.config.download_folder, {}, meta_b.info)

	await settle([seeder, leecher], 3)
	leecher.env.event_bus.dispatch("peers.update", ih_a, [PeerInfo("127.0.0.1", 6871)])
	leecher.env.event_bus.dispatch("peers.update", ih_b, [PeerInfo("127.0.0.1", 6871)])

	leech_a = get_torrent_entity(leecher.env, ih_a)
	leech_b = get_torrent_entity(leecher.env, ih_b)
	both = await run_until([seeder, leecher],
	                       lambda: is_torrent_complete(leech_a) and is_torrent_complete(leech_b), rounds=150)
	await leecher.stop()
	await seeder.stop()
	return both


async def scenario_peer_drop_recovery(work: Path) -> bool:
	# leecher downloads from seeder A; A drops mid-transfer — its in-flight blocks must be
	# released back to the pool — and seeder B then finishes the download. Without the
	# release fix, A's in-flight blocks stay "requested" and the affected pieces stall.
	content = __import__("os").urandom(PIECE_LEN * 40)  # 40 pieces: many not in progress at once
	meta = single_file(content)
	ih = meta.make_info_hash()

	seeder_a = Instance(6881, work / "s9_a", b"-PY0001-SEEDERA00009")
	seeder_b = Instance(6882, work / "s9_b", b"-PY0001-SEEDERB00009")
	leecher = Instance(6883, work / "s9_leech", b"-PY0001-LEECHER00009")
	for s in (seeder_a, seeder_b):
		write_files(s.env.config.download_folder, meta.info, content)
	for inst in (seeder_a, seeder_b, leecher):
		await inst.start()
	for s in (seeder_a, seeder_b):
		st = create_torrent_entity(s.env, ih, s.env.config.download_folder, {}, meta.info)
		for i in range(meta.info.pieces_num):
			st.get_component(TorrentEC).bitfield.set_index(i)
	create_torrent_entity(leecher.env, ih, leecher.env.config.download_folder, {}, meta.info)

	await settle([seeder_a, seeder_b, leecher], 3)
	leech = get_torrent_entity(leecher.env, ih)
	bitfield = leech.get_component(TorrentEC).bitfield

	# download from A only, then drop it once partway through (leaving pieces in progress)
	leecher.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6881)])
	await run_until([seeder_a, leecher],
	                lambda: 3 <= bitfield.have_num < meta.info.pieces_num, rounds=100, sleep=0.03)
	for peer_entity in list(iterate_connected_peers(leecher.env, ih)):
		peer_entity.add_component(PeerDisconnectedEC())

	# offer seeder B; the leecher must recover the stalled blocks and complete
	leecher.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6882)])
	completed = await run_until([seeder_a, seeder_b, leecher], lambda: is_torrent_complete(leech), rounds=200)
	ok = completed and reconstruct(leecher.env, ih, meta.info.pieces_num, len(content)) == content
	for inst in (leecher, seeder_a, seeder_b):
		await inst.stop()
	return ok


async def scenario_uninteresting_peer_released(work: Path) -> bool:
	"""Two complete seeds connect, enter neither queue, and are released on the idle timeout.

	Neither can give the other anything, so the connection never joins a queue and the
	timeout reaps it. The bitfield each learned meanwhile is what stops them from dialling
	each other again afterwards.
	"""
	content = __import__("os").urandom(20000)
	meta = single_file(content)
	ih = meta.make_info_hash()

	seed_a = Instance(6891, work / "s10_a", b"-PY0001-SEEDERA00010")
	seed_b = Instance(6892, work / "s10_b", b"-PY0001-SEEDERB00010")
	for inst in (seed_a, seed_b):
		write_files(inst.env.config.download_folder, meta.info, content)
	await seed_a.start();
	await seed_b.start()

	for inst in (seed_a, seed_b):
		entity = create_torrent_entity(inst.env, ih, inst.env.config.download_folder, {}, meta.info)
		for i in range(meta.info.pieces_num):
			entity.get_component(TorrentEC).bitfield.set_index(i)

	# the default is 30s of wall clock; shorten it so the scenario stays quick
	for inst in (seed_a, seed_b):
		inst.env.config.peer_idle_timeout = 0.5

	await settle([seed_a, seed_b], 3)
	seed_a.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6892)])

	# they must genuinely connect and exchange bitfields first...
	learned = await run_until(
		[seed_a, seed_b],
		lambda: (lambda p: p is not None and p.get_component(PeerEC).remote_bitfield.have_num
		                   >= meta.info.pieces_num)(find_peer_entity(seed_a.env, ih, "127.0.0.1", 6892)),
		rounds=60, sleep=0.03)

	# ...then the purposeless connection is released on both sides
	released = await run_until(
		[seed_a, seed_b],
		lambda: not list(iterate_connected_peers(seed_a.env, ih)) and not list(iterate_connected_peers(seed_b.env, ih)),
		rounds=60, sleep=0.03)

	# and stays released — a peer useful to neither side is not dialled again
	await settle([seed_a, seed_b], 20, sleep=0.03)
	stayed_off = not list(iterate_connected_peers(seed_a.env, ih))

	ok = learned and released and stayed_off
	await seed_a.stop();
	await seed_b.stop()
	return ok


async def scenario_torrent_remove_clears_swarm(work: Path) -> bool:
	"""Removing a torrent must disconnect its peers and delete their entities."""
	content = __import__("os").urandom(20000)
	meta = single_file(content)
	ih = meta.make_info_hash()

	seeder = Instance(6901, work / "s11_seed", b"-PY0001-SEEDER000011")
	leecher = Instance(6902, work / "s11_leech", b"-PY0001-LEECHER00011")
	write_files(seeder.env.config.download_folder, meta.info, content)
	await seeder.start();
	await leecher.start()

	seed = create_torrent_entity(seeder.env, ih, seeder.env.config.download_folder, {}, meta.info)
	for i in range(meta.info.pieces_num):
		seed.get_component(TorrentEC).bitfield.set_index(i)
	create_torrent_entity(leecher.env, ih, leecher.env.config.download_folder, {}, meta.info)

	await settle([seeder, leecher], 3)
	leecher.env.event_bus.dispatch("peers.update", ih, [PeerInfo("127.0.0.1", 6901)])

	connected = await run_until(
		[seeder, leecher], lambda: bool(list(iterate_connected_peers(leecher.env, ih))), rounds=60, sleep=0.03)

	await asyncio.gather(*leecher.env.event_bus.dispatch("request.torrent.remove", ih))
	await settle([seeder, leecher], 3)

	# no connections, no peer entities, and nothing redials the vanished torrent
	cleared = (not list(iterate_connected_peers(leecher.env, ih))
	           and not list(iterate_peers(leecher.env, ih))
	           and len(leecher.env.data_storage.get_collection(PeerEC)) == 0)

	await settle([seeder, leecher], 10, sleep=0.03)
	stayed_clear = not list(iterate_peers(leecher.env, ih))

	ok = connected and cleared and stayed_clear
	await leecher.stop();
	await seeder.stop()
	return ok


SCENARIOS = [
	("basic transfer", scenario_basic_transfer),
	("paused seeder uploads nothing", scenario_paused_seeder),
	("partial file selection completes", scenario_partial_selection),
	("peer state machine on failed connect", scenario_peer_state_machine),
	("multi-leecher swarm (upload while incomplete)", scenario_multi_leecher),
	("choke caps the upload queue at the limit", scenario_choke_over_limit),
	("DHT discovery (announce -> discover -> download)", scenario_dht_discovery),
	("queue promotion starts a queued torrent", scenario_queue_promotion),
	("peer drop mid-download recovers via another peer", scenario_peer_drop_recovery),
	("uninteresting peer released on idle, then not redialled", scenario_uninteresting_peer_released),
	("torrent removal disconnects and clears its peers", scenario_torrent_remove_clears_swarm),
]


async def main() -> int:
	work = Path(tempfile.mkdtemp(prefix="yap_swarm_"))
	failures = 0
	try:
		for name, scenario in SCENARIOS:
			try:
				ok = await scenario(work)
			except Exception as ex:  # noqa: BLE001
				ok = False
				print(f"  ERROR in {name}: {ex!r}")
			print(f"[{'PASS' if ok else 'FAIL'}] {name}")
			failures += 0 if ok else 1
	finally:
		shutil.rmtree(work, ignore_errors=True)

	print(f"\n{len(SCENARIOS) - failures}/{len(SCENARIOS)} scenarios passed")
	return 0 if failures == 0 else 1


if __name__ == "__main__":
	sys.exit(asyncio.run(main()))
