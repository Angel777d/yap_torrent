"""Unit tests for the Transmission RPC plugin.

Run with:  pytest plugins/transmission_rpc

Only the test runner needs installing (pytest + pytest-aiohttp); yap_torrent, the
plugin, and angelovich.core are imported from source by conftest.py, so no
`pip install -e` of the packages is required.

These exercise the RPC layer directly through aiohttp's test client; no real
Transmission client or network sockets are involved.
"""
import asyncio
import base64

import pytest

from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent.protocol import encode
from yap_torrent.systems.file_system import FileSystem
from yap_torrent.systems.magnet_system import MagnetSystem
from yap_torrent.systems.metainfo_system import MetainfoSystem
from yap_torrent.systems.stats_system import StatsSystem
from yap_torrent.systems.torrents_system import TorrentSystem
from yap_torrent_transmission_rpc.server import CSRF_HEADER, RpcServer

RPC_PATH = "/transmission/rpc"


def make_metainfo(name: bytes = b"hello.txt") -> bytes:
	info = {"name": name, "piece length": 16384, "pieces": b"\x00" * 20, "length": 10}
	return encode({"info": info, "announce": b"http://tracker.example/announce"})


def make_multifile_metainfo(name: bytes = b"folder") -> bytes:
	info = {
		"name": name, "piece length": 16384, "pieces": b"\x00" * 20 * 3,
		"files": [
			{"path": [b"a.bin"], "length": 16384},
			{"path": [b"b.bin"], "length": 16384},
			{"path": [b"c.bin"], "length": 16384},
		],
	}
	return encode({"info": info, "announce": b"http://tracker.example/announce"})


@pytest.fixture
async def server():
	# Config() with a missing path falls back to defaults, so tests do not depend
	# on the repo's config.json.
	env = Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path="__no_such_config__.json"))
	# The RPC layer owns no state: every method delegates to core through the event bus,
	# so the systems that answer those events have to be live. AnnounceSystem is left out
	# deliberately — it would announce to the metainfo's tracker URL over the network.
	systems = [
		MetainfoSystem(env), MagnetSystem(env), FileSystem(env), TorrentSystem(env), StatsSystem(env),
	]
	for system in systems:
		await system.start()
	yield RpcServer(env)
	for system in systems:
		await system.stop()
		system.close()


async def add_torrent(client, session_id, metainfo: bytes) -> str:
	"""Add a torrent and settle the tasks that build its entity, files and queue slot."""
	added = await rpc(client, session_id, "torrent-add",
	                  {"metainfo": base64.b64encode(metainfo).decode()})
	assert added["result"] == "success"
	await asyncio.sleep(0.01)
	return added["arguments"]["torrent-added"]["hashString"]


async def get_field(client, session_id, info_hash: str, field: str):
	data = await rpc(client, session_id, "torrent-get", {"ids": [info_hash], "fields": ["hashString", field]})
	return next(t[field] for t in data["arguments"]["torrents"] if t["hashString"] == info_hash)


@pytest.fixture
async def client(server, aiohttp_client):
	return await aiohttp_client(server.app)


async def handshake(client) -> str:
	"""Perform the initial 409 exchange and return the session id."""
	resp = await client.post(RPC_PATH, json={"method": "session-get"})
	assert resp.status == 409
	assert CSRF_HEADER in resp.headers
	return resp.headers[CSRF_HEADER]


async def rpc(client, session_id, method, arguments=None, tag=None):
	body = {"method": method, "arguments": arguments or {}}
	if tag is not None:
		body["tag"] = tag
	resp = await client.post(RPC_PATH, json=body, headers={CSRF_HEADER: session_id})
	assert resp.status == 200
	return await resp.json()


async def test_csrf_handshake_returns_409_then_succeeds(client):
	# No session id -> 409 with the id in the header.
	first = await client.post(RPC_PATH, json={"method": "session-get"})
	assert first.status == 409
	session_id = first.headers[CSRF_HEADER]

	# Wrong session id -> still 409.
	wrong = await client.post(RPC_PATH, json={"method": "session-get"}, headers={CSRF_HEADER: "nope"})
	assert wrong.status == 409

	# Correct id -> 200.
	ok = await client.post(RPC_PATH, json={"method": "session-get"}, headers={CSRF_HEADER: session_id})
	assert ok.status == 200


async def test_session_get(client):
	session_id = await handshake(client)
	data = await rpc(client, session_id, "session-get", tag=42)
	assert data["result"] == "success"
	assert data["tag"] == 42
	assert data["arguments"]["rpc-version"] == 17
	assert "version" in data["arguments"]


async def test_session_get_field_filter(client):
	session_id = await handshake(client)
	data = await rpc(client, session_id, "session-get", {"fields": ["rpc-version"]})
	assert data["arguments"] == {"rpc-version": 17}


async def test_torrent_add_metainfo_then_get(client):
	session_id = await handshake(client)
	metainfo = base64.b64encode(make_metainfo()).decode()

	added = await rpc(client, session_id, "torrent-add", {"metainfo": metainfo})
	assert added["result"] == "success"
	stub = added["arguments"]["torrent-added"]
	info_hash = stub["hashString"]
	assert stub["name"] == "hello.txt"
	assert len(info_hash) == 40

	got = await rpc(client, session_id, "torrent-get", {"fields": ["id", "hashString", "name", "status"]})
	torrents = got["arguments"]["torrents"]
	assert len(torrents) == 1
	assert torrents[0]["hashString"] == info_hash
	assert torrents[0]["name"] == "hello.txt"
	assert torrents[0]["id"] == stub["id"]


async def test_torrent_add_duplicate(client):
	session_id = await handshake(client)
	metainfo = base64.b64encode(make_metainfo()).decode()

	await rpc(client, session_id, "torrent-add", {"metainfo": metainfo})
	again = await rpc(client, session_id, "torrent-add", {"metainfo": metainfo})
	assert "torrent-duplicate" in again["arguments"]


async def test_torrent_add_requires_source(client):
	session_id = await handshake(client)
	data = await rpc(client, session_id, "torrent-add", {})
	assert data["result"] != "success"
	assert "metainfo" in data["result"]


async def test_torrent_add_magnet(client):
	session_id = await handshake(client)
	magnet = "magnet:?xt=urn:btih:" + "a" * 40 + "&dn=magname"
	data = await rpc(client, session_id, "torrent-add", {"filename": magnet})
	assert data["result"] == "success"
	stub = data["arguments"]["torrent-added"]
	assert stub["hashString"] == "a" * 40
	assert stub["name"] == "magname"


async def test_torrent_start_dispatches_event(client, server):
	captured = []

	async def on_start(info_hash):
		captured.append(info_hash)

	server.env.event_bus.add_listener("request.torrent.start", on_start)

	session_id = await handshake(client)
	metainfo = base64.b64encode(make_metainfo()).decode()
	added = await rpc(client, session_id, "torrent-add", {"metainfo": metainfo})
	info_hash = added["arguments"]["torrent-added"]["hashString"]

	await rpc(client, session_id, "torrent-start", {"ids": [info_hash]})
	await asyncio.sleep(0.01)  # let the dispatched task run
	assert bytes.fromhex(info_hash) in captured


async def test_unknown_method(client):
	session_id = await handshake(client)
	data = await rpc(client, session_id, "no-such-method")
	assert "not recognized" in data["result"]


async def test_unimplemented_method_is_recognised(client):
	session_id = await handshake(client)
	data = await rpc(client, session_id, "session-set", {})
	# Not "success", but also not the unknown-method error.
	assert data["result"] != "success"
	assert "not recognized" not in data["result"]
	assert "session-set" in data["result"]


async def test_free_space(client):
	session_id = await handshake(client)
	data = await rpc(client, session_id, "free-space", {"path": "."})
	assert data["result"] == "success"
	assert data["arguments"]["size-bytes"] >= 0


# ---------------------------------------------------------------------------
# torrent-set (rpc-spec 3.2)
# ---------------------------------------------------------------------------
async def test_torrent_set_labels_round_trip(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "labels": ["linux", "iso"]})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, info_hash, "labels") == ["linux", "iso"]


async def test_torrent_set_files_unwanted_is_reflected_in_file_stats(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_multifile_metainfo())

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "files-unwanted": [1]})
	await asyncio.sleep(0.01)

	stats = await get_field(client, session_id, info_hash, "fileStats")
	assert [f["wanted"] for f in stats] == [True, False, True]
	assert await get_field(client, session_id, info_hash, "wanted") == [1, 0, 1]


async def test_torrent_set_file_priority_is_reflected(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_multifile_metainfo())

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "priority-high": [0, 2]})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, info_hash, "priorities") == [1, 0, 1]


async def test_torrent_set_ignores_arguments_core_cannot_honour(client):
	# Transmission is lenient about unknown/unsupported args; failing the whole call
	# would break clients that always send a full settings block
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	data = await rpc(client, session_id, "torrent-set",
	                 {"ids": [info_hash], "seedRatioLimit": 2.0, "uploadLimit": 50, "labels": ["keep"]})
	await asyncio.sleep(0.01)

	assert data["result"] == "success"
	assert await get_field(client, session_id, info_hash, "labels") == ["keep"]


# ---------------------------------------------------------------------------
# queue movement (rpc-spec 4.7)
# ---------------------------------------------------------------------------
async def test_queue_move_changes_queue_position(client):
	session_id = await handshake(client)
	first = await add_torrent(client, session_id, make_metainfo(b"first.txt"))
	second = await add_torrent(client, session_id, make_metainfo(b"second.txt"))

	assert await get_field(client, session_id, first, "queuePosition") == 0
	assert await get_field(client, session_id, second, "queuePosition") == 1

	await rpc(client, session_id, "queue-move-bottom", {"ids": [first]})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, first, "queuePosition") == 1
	assert await get_field(client, session_id, second, "queuePosition") == 0


# ---------------------------------------------------------------------------
# reannounce (rpc-spec 3.1)
# ---------------------------------------------------------------------------
async def test_torrent_reannounce_dispatches_event(client, server):
	captured = []

	async def on_reannounce(info_hash):
		captured.append(info_hash)

	server.env.event_bus.add_listener("request.torrent.reannounce", on_reannounce)

	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	data = await rpc(client, session_id, "torrent-reannounce", {"ids": [info_hash]})
	await asyncio.sleep(0.01)

	assert data["result"] == "success"
	assert bytes.fromhex(info_hash) in captured


# ---------------------------------------------------------------------------
# rates and dates
# ---------------------------------------------------------------------------
async def test_rates_and_dates_are_reported(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	data = await rpc(client, session_id, "torrent-get", {
		"ids": [info_hash],
		"fields": ["rateDownload", "rateUpload", "eta", "addedDate", "doneDate"],
	})
	torrent = data["arguments"]["torrents"][0]

	# no peers yet, so the rates are 0 and the ETA is unknown rather than absent
	assert torrent["rateDownload"] == 0 and torrent["rateUpload"] == 0
	assert torrent["eta"] == -1
	assert torrent["addedDate"] > 0  # the date core stamps on the entity
	assert torrent["doneDate"] == 0


async def test_session_stats_reports_speeds(client):
	session_id = await handshake(client)
	await add_torrent(client, session_id, make_metainfo())

	data = await rpc(client, session_id, "session-stats")
	assert data["result"] == "success"
	assert data["arguments"]["downloadSpeed"] == 0
	assert data["arguments"]["uploadSpeed"] == 0
	assert data["arguments"]["torrentCount"] == 1
