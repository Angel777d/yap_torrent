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
from yap_torrent.systems.settings_system import SettingsSystem
from yap_torrent.systems.stats_system import StatsSystem
from yap_torrent.systems.torrents_system import TorrentSystem
from yap_torrent_transmission_rpc.components import get_speed_settings
from yap_torrent_transmission_rpc.methods import CORE_SETTINGS
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
		SettingsSystem(env), MetainfoSystem(env), MagnetSystem(env), FileSystem(env), TorrentSystem(env),
		StatsSystem(env),
	]
	for system in systems:
		await system.start()
	# the fixture never binds the HTTP server, so do by hand the one part of RpcServer.start
	# that is not about sockets: telling core which config properties this RPC exposes
	await env.event_bus.dispatch_async("request.setting.register", CORE_SETTINGS)
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
	data = await rpc(client, session_id, "torrent-rename-path", {})
	# Not "success", but also not the unknown-method error.
	assert data["result"] != "success"
	assert "not recognized" not in data["result"]
	assert "torrent-rename-path" in data["result"]


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


# ---------------------------------------------------------------------------
# spec conformance findings
# ---------------------------------------------------------------------------
async def test_wanted_relative_progress_reaches_one_hundred_percent(client, server):
	# percentDone is of the files the user *wants*; percentComplete is of the whole
	# torrent. Reporting the total for both leaves a torrent with a deselected file
	# stuck below 100% for ever while it reports itself as finished.
	from yap_torrent.components.torrent_ec import TorrentEC
	from yap_torrent.systems import get_torrent_entity

	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_multifile_metainfo())

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "files-unwanted": [2]})
	await asyncio.sleep(0.01)

	# hold the two wanted files' pieces, not the third file's
	entity = get_torrent_entity(server.env, bytes.fromhex(info_hash))
	for index in (0, 1):
		entity.get_component(TorrentEC).bitfield.set_index(index)

	data = await rpc(client, session_id, "torrent-get", {
		"ids": [info_hash],
		"fields": ["percentDone", "percentComplete", "sizeWhenDone", "totalSize",
		           "leftUntilDone", "isFinished"],
	})
	torrent = data["arguments"]["torrents"][0]

	assert torrent["percentDone"] == 1.0  # everything wanted is here
	assert torrent["percentComplete"] < 1.0  # but a third of the torrent is not
	assert torrent["sizeWhenDone"] < torrent["totalSize"]
	assert torrent["leftUntilDone"] == 0
	assert torrent["isFinished"] is True


async def test_tracker_failure_is_reported_as_a_tracker_error(client, server):
	# tr_stat_errtype: TR_STAT_TRACKER_ERROR is 2; 3 is TR_STAT_LOCAL_ERROR
	from yap_torrent.components.tracker_ec import TorrentTrackerDataEC
	from yap_torrent.systems import get_torrent_entity

	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	entity = get_torrent_entity(server.env, bytes.fromhex(info_hash))
	for _ in range(5):
		entity.get_component(TorrentTrackerDataEC).fail_announce()

	data = await rpc(client, session_id, "torrent-get",
	                 {"ids": [info_hash], "fields": ["error", "errorString"]})
	torrent = data["arguments"]["torrents"][0]
	assert torrent["error"] == 2
	assert torrent["errorString"]


async def test_torrent_get_table_format(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	data = await rpc(client, session_id, "torrent-get",
	                 {"fields": ["id", "hashString", "name"], "format": "table"})
	rows = data["arguments"]["torrents"]

	assert rows[0] == ["id", "hashString", "name"]
	assert len(rows) == 2
	assert rows[1][1] == info_hash
	assert len(rows[1]) == len(rows[0])  # every row lines up with the header


async def test_every_requested_field_comes_back(client):
	# transmission-rpc reads fields as self.fields[name] and raises KeyError on an
	# absent one, so omitting a field a client asked for breaks it
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	fields = [
		"etaIdle", "file-count", "maxConnectedPeers", "manualAnnounceTime", "peer-limit",
		"pieces", "primary-mime-type", "secondsDownloading", "secondsSeeding",
		"sequentialDownload", "torrentFile", "trackerList", "webseeds", "availability",
	]
	data = await rpc(client, session_id, "torrent-get", {"ids": [info_hash], "fields": fields})
	torrent = data["arguments"]["torrents"][0]

	assert sorted(torrent) == sorted(fields)


async def test_upload_ratio_uses_the_not_applicable_value(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	# nothing up, nothing down -> TR_RATIO_NA
	assert await get_field(client, session_id, info_hash, "uploadRatio") == -1


async def test_rpc_version_and_semver_agree(client):
	# the spec's version table maps rpc-version 17 to semver 5.3.0; clients gate
	# features on the semver
	session_id = await handshake(client)
	data = await rpc(client, session_id, "session-get")
	arguments = data["arguments"]
	assert arguments["rpc-version"] == 17
	assert arguments["rpc-version-semver"] == "5.3.0"
	assert arguments["rpc-version-minimum"] <= arguments["rpc-version"]


# ---------------------------------------------------------------------------
# session-set (rpc-spec 4.1)
# ---------------------------------------------------------------------------
async def test_session_set_changes_a_setting_and_session_get_reads_it_back(client, server):
	session_id = await handshake(client)

	data = await rpc(client, session_id, "session-set", {"speed-limit-up": 250, "speed-limit-up-enabled": True})
	assert data["result"] == "success"

	got = await rpc(client, session_id, "session-get", {"fields": ["speed-limit-up", "speed-limit-up-enabled"]})
	assert got["arguments"] == {"speed-limit-up": 250, "speed-limit-up-enabled": True}
	assert server.env.config.speed_limit_up == 250


async def test_session_set_ignores_keys_core_has_no_notion_of(client):
	session_id = await handshake(client)
	data = await rpc(client, session_id, "session-set", {"utp-enabled": True, "no-such-key": 1})
	assert data["result"] == "success"


# ---------------------------------------------------------------------------
# torrent-add applies the settings arguments it is given
# ---------------------------------------------------------------------------
async def test_torrent_add_applies_labels_and_file_selection(client):
	session_id = await handshake(client)
	added = await rpc(client, session_id, "torrent-add", {
		"metainfo": base64.b64encode(make_multifile_metainfo()).decode(),
		"labels": ["fresh"],
		"files-unwanted": [1],
	})
	await asyncio.sleep(0.01)
	info_hash = added["arguments"]["torrent-added"]["hashString"]

	assert await get_field(client, session_id, info_hash, "labels") == ["fresh"]
	assert await get_field(client, session_id, info_hash, "wanted") == [1, 0, 1]


async def test_torrent_set_stores_limits_and_queue_position(client):
	session_id = await handshake(client)
	first = await add_torrent(client, session_id, make_metainfo(b"one.txt"))
	second = await add_torrent(client, session_id, make_metainfo(b"two.txt"))

	await rpc(client, session_id, "torrent-set", {
		"ids": [first], "uploadLimit": 40, "uploadLimited": True, "queuePosition": 1,
	})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, first, "uploadLimit") == 40
	assert await get_field(client, session_id, first, "uploadLimited") is True
	assert await get_field(client, session_id, first, "queuePosition") == 1
	assert await get_field(client, session_id, second, "queuePosition") == 0


# ---------------------------------------------------------------------------
# alt speed (turtle mode) is the plugin's, not core's
# ---------------------------------------------------------------------------
async def test_alt_speed_round_trips_without_touching_core(client, server):
	session_id = await handshake(client)

	await rpc(client, session_id, "session-set",
	          {"alt-speed-down": 100, "alt-speed-up": 50, "alt-speed-enabled": True})

	got = await rpc(client, session_id, "session-get",
	                {"fields": ["alt-speed-down", "alt-speed-up", "alt-speed-enabled"]})
	assert got["arguments"] == {"alt-speed-down": 100, "alt-speed-up": 50, "alt-speed-enabled": True}

	# core keeps one pair of speed limits and knows nothing about a second one
	assert not hasattr(server.env.config, "alt_speed_down")


async def test_alt_speed_is_runtime_state_and_is_not_written_back(client, server):
	# it is a live knob, not a stored preference: config.json is untouched
	session_id = await handshake(client)
	before = dict(server.env.config.data)
	await rpc(client, session_id, "session-set", {"alt-speed-down": 42})

	assert get_speed_settings(server.env).alt_speed_down == 42
	assert server.env.config.data == before


async def test_the_normal_speed_limits_still_go_to_core(client, server):
	# one session-set carrying both: the normal pair is core's, the alt pair is ours
	session_id = await handshake(client)
	await rpc(client, session_id, "session-set", {
		"speed-limit-down": 300, "speed-limit-down-enabled": True, "alt-speed-down": 42,
	})

	assert server.env.config.speed_limit_down == 300
	assert get_speed_settings(server.env).alt_speed_down == 42


async def test_a_limit_of_zero_in_core_is_reported_as_switched_off(client, server):
	# core keeps one number per direction where 0 means no limit; Transmission wants a
	# number and a flag, and expects its number to still be in the box once it is off
	session_id = await handshake(client)

	await rpc(client, session_id, "session-set", {"speed-limit-up": 250, "speed-limit-up-enabled": True})
	assert server.env.config.speed_limit_up == 250

	await rpc(client, session_id, "session-set", {"speed-limit-up-enabled": False})
	assert server.env.config.speed_limit_up == 0  # off is 0, not a flag

	got = await rpc(client, session_id, "session-get",
	                {"fields": ["speed-limit-up", "speed-limit-up-enabled"]})
	assert got["arguments"] == {"speed-limit-up": 250, "speed-limit-up-enabled": False}


async def test_switching_a_limit_back_on_restores_the_number(client, server):
	session_id = await handshake(client)
	await rpc(client, session_id, "session-set", {"speed-limit-up": 250, "speed-limit-up-enabled": True})
	await rpc(client, session_id, "session-set", {"speed-limit-up-enabled": False})

	# the flag alone, with no number: the remembered one comes back
	await rpc(client, session_id, "session-set", {"speed-limit-up-enabled": True})
	assert server.env.config.speed_limit_up == 250


async def test_a_number_alone_does_not_switch_a_disabled_limit_on(client, server):
	# that is what the flag is for; the number is remembered for when it is switched on
	session_id = await handshake(client)
	await rpc(client, session_id, "session-set", {"speed-limit-down": 300})

	assert server.env.config.speed_limit_down == 0
	got = await rpc(client, session_id, "session-get",
	                {"fields": ["speed-limit-down", "speed-limit-down-enabled"]})
	assert got["arguments"] == {"speed-limit-down": 300, "speed-limit-down-enabled": False}


async def test_the_speed_settings_are_one_component_for_the_whole_app(client, server):
	# a singleton in the shared ECS, not per-request or per-server state: anything in the
	# app that wants to know whether turtle mode is on reads the same component
	from yap_torrent_transmission_rpc.components import SpeedSettingsEC

	session_id = await handshake(client)
	await rpc(client, session_id, "session-set", {"alt-speed-enabled": True})

	collection = server.env.data_storage.get_collection(SpeedSettingsEC)
	assert len(collection) == 1
	assert get_speed_settings(server.env) is get_speed_settings(server.env)
	assert get_speed_settings(server.env).alt_speed_enabled is True


async def test_the_initial_values_come_from_config(client, server):
	# runtime state, but not from nowhere: config.json seeds it at startup
	from yap_torrent_transmission_rpc.components import PLUGIN_CONFIG_KEY, get_speed_settings as get_it

	env = server.env
	env.config.data[PLUGIN_CONFIG_KEY] = {"alt_speed_down": 77, "alt_speed_enabled": True}
	# drop the singleton the fixture seeded so the next read rebuilds it from config
	for entity in list(env.data_storage.get_collection(type(get_it(env)))):
		env.data_storage.remove_entity(entity)

	settings = get_it(env)
	assert settings.alt_speed_down == 77
	assert settings.alt_speed_enabled is True


# ---------------------------------------------------------------------------
# state this plugin owns
# ---------------------------------------------------------------------------
# Labels are a Transmission idea. Core has no component for one — it keeps them in the
# torrent's custom_data under our name and hands them back without reading them, so
# everything that decides what a label *is* has to be tested here.
def test_labels_are_cleaned_before_they_are_stored():
	from yap_torrent_transmission_rpc.components import clean_labels

	assert clean_labels([" iso ", "", "linux", "iso", "   "]) == ["iso", "linux"]
	assert clean_labels(None) == []


async def test_setting_labels_replaces_the_previous_set(client):
	# torrent-set labels is a replace, not a merge
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "labels": ["a", "b"]})
	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "labels": ["c"]})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, info_hash, "labels") == ["c"]


async def test_blanks_and_duplicates_are_dropped_in_order(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	await rpc(client, session_id, "torrent-set",
	          {"ids": [info_hash], "labels": [" iso ", "", "linux", "iso", "   "]})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, info_hash, "labels") == ["iso", "linux"]


async def test_an_unlabelled_torrent_reports_an_empty_list(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	assert await get_field(client, session_id, info_hash, "labels") == []


async def test_labels_live_in_the_torrent_custom_data(client, server):
	# where they land matters: custom_data is what LocalDataSystem persists, so a label
	# stored anywhere else would not survive a restart
	from yap_torrent.systems import get_custom_data, get_torrent_entity
	from yap_torrent.components.torrent_ec import SaveTorrentEC
	from yap_torrent_transmission_rpc.components import PLUGIN_CONFIG_KEY

	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "labels": ["linux"]})
	await asyncio.sleep(0.01)

	entity = get_torrent_entity(server.env, bytes.fromhex(info_hash))
	assert get_custom_data(entity, PLUGIN_CONFIG_KEY) == {"labels": ["linux"]}
	assert entity.has_component(SaveTorrentEC)  # a label lost to a restart is not a label


async def test_setting_the_same_labels_again_does_not_ask_for_a_save(client, server):
	from yap_torrent.systems import get_torrent_entity
	from yap_torrent.components.torrent_ec import SaveTorrentEC

	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())
	entity = get_torrent_entity(server.env, bytes.fromhex(info_hash))

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "labels": ["a"]})
	await asyncio.sleep(0.01)
	entity.remove_component(SaveTorrentEC)

	await rpc(client, session_id, "torrent-set", {"ids": [info_hash], "labels": ["a"]})
	await asyncio.sleep(0.01)

	assert entity.has_component(SaveTorrentEC) is False


async def test_a_boolean_queue_position_is_not_position_one(client):
	# bool is an int subclass, so a JSON true would otherwise move the torrent to slot 1.
	# Core takes an int at face value; catching this is ours.
	session_id = await handshake(client)
	first = await add_torrent(client, session_id, make_metainfo(b"first.txt"))
	second = await add_torrent(client, session_id, make_metainfo(b"second.txt"))

	await rpc(client, session_id, "torrent-set", {"ids": [first], "queuePosition": True})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, first, "queuePosition") == 0
	assert await get_field(client, session_id, second, "queuePosition") == 1


async def test_an_absolute_queue_position_still_moves_the_torrent(client):
	session_id = await handshake(client)
	first = await add_torrent(client, session_id, make_metainfo(b"first.txt"))
	second = await add_torrent(client, session_id, make_metainfo(b"second.txt"))

	await rpc(client, session_id, "torrent-set", {"ids": [first], "queuePosition": 1})
	await asyncio.sleep(0.01)

	assert await get_field(client, session_id, first, "queuePosition") == 1
	assert await get_field(client, session_id, second, "queuePosition") == 0


# --- per-file byte counts ---------------------------------------------------
# file_bytes_completed lives here now: core downloads and reports whole pieces, and the
# first and last piece of a file are shared with its neighbours, so scaling the overall
# percentage would report a file complete that is not.
async def test_completed_bytes_are_counted_per_file_not_per_piece(client, server):
	from yap_torrent.components.torrent_ec import TorrentEC
	from yap_torrent.systems import get_torrent_entity

	session_id = await handshake(client)
	# a=10000 (piece 0), b=20000 (pieces 0..1), c=40000 (pieces 1..4) — boundaries shared
	info = {
		"name": b"t", "piece length": 16384, "pieces": b"\x00" * 20 * 5,
		"files": [{"path": [b"a"], "length": 10000},
		          {"path": [b"b"], "length": 20000},
		          {"path": [b"c"], "length": 40000}],
	}
	info_hash = await add_torrent(client, session_id, encode({"info": info}))

	entity = get_torrent_entity(server.env, bytes.fromhex(info_hash))
	entity.get_component(TorrentEC).bitfield.set_index(0)  # piece 0 only

	files = await get_field(client, session_id, info_hash, "files")
	assert [f["bytesCompleted"] for f in files] == [10000, 16384 - 10000, 0]


async def test_a_complete_torrent_reports_every_file_whole(client, server):
	from yap_torrent.components.torrent_ec import TorrentEC
	from yap_torrent.systems import get_torrent_entity

	session_id = await handshake(client)
	info = {
		"name": b"t", "piece length": 16384, "pieces": b"\x00" * 20 * 5,
		"files": [{"path": [b"a"], "length": 10000},
		          {"path": [b"b"], "length": 20000},
		          {"path": [b"c"], "length": 40000}],
	}
	info_hash = await add_torrent(client, session_id, encode({"info": info}))

	entity = get_torrent_entity(server.env, bytes.fromhex(info_hash))
	for index in range(5):
		entity.get_component(TorrentEC).bitfield.set_index(index)

	files = await get_field(client, session_id, info_hash, "files")
	assert [f["bytesCompleted"] for f in files] == [10000, 20000, 40000]
	assert sum(f["bytesCompleted"] for f in files) == 70000


# ---------------------------------------------------------------------------
# queue directions (rpc-spec 4.7)
# ---------------------------------------------------------------------------
# Core holds ordinals and is handed a finished order; top/up/down/bottom and an absolute
# queuePosition are Transmission's vocabulary, so the whole meaning of a move is tested
# here rather than in core.
async def _positions(client, session_id, hashes):
	return [await get_field(client, session_id, h, "queuePosition") for h in hashes]


async def _queue_of(client, session_id, count=4):
	hashes = []
	for index in range(count):
		hashes.append(await add_torrent(client, session_id, make_metainfo(f"t{index}.txt".encode())))
	assert await _positions(client, session_id, hashes) == list(range(count))
	return hashes


async def test_move_to_top_pushes_everything_else_down(client):
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "queue-move-top", {"ids": [hashes[2]]})
	await asyncio.sleep(0.01)

	assert await _positions(client, session_id, hashes) == [1, 2, 0, 3]


async def test_move_to_bottom_pulls_everything_else_up(client):
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "queue-move-bottom", {"ids": [hashes[1]]})
	await asyncio.sleep(0.01)

	assert await _positions(client, session_id, hashes) == [0, 3, 1, 2]


async def test_up_and_down_swap_with_one_neighbour(client):
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "queue-move-up", {"ids": [hashes[2]]})
	await asyncio.sleep(0.01)
	assert await _positions(client, session_id, hashes) == [0, 2, 1, 3]

	await rpc(client, session_id, "queue-move-down", {"ids": [hashes[2]]})
	await asyncio.sleep(0.01)
	assert await _positions(client, session_id, hashes) == [0, 1, 2, 3]


async def test_moving_past_an_end_stays_at_that_end(client):
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "queue-move-up", {"ids": [hashes[0]]})
	await rpc(client, session_id, "queue-move-down", {"ids": [hashes[3]]})
	await asyncio.sleep(0.01)

	assert await _positions(client, session_id, hashes) == [0, 1, 2, 3]


async def test_a_multi_torrent_move_keeps_the_selection_in_order(client):
	# bottom-ward moves are applied in reverse, or the selection is turned inside out
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "queue-move-bottom", {"ids": [hashes[0], hashes[1]]})
	await asyncio.sleep(0.01)

	assert await _positions(client, session_id, hashes) == [2, 3, 0, 1]


async def test_an_absolute_position_moves_the_torrent(client):
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "torrent-set", {"ids": [hashes[0]], "queuePosition": 3})
	await asyncio.sleep(0.01)

	assert await _positions(client, session_id, hashes) == [3, 0, 1, 2]


async def test_an_out_of_range_position_clamps_to_an_end(client):
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "torrent-set", {"ids": [hashes[1]], "queuePosition": 99})
	await asyncio.sleep(0.01)

	assert await _positions(client, session_id, hashes) == [0, 3, 1, 2]


async def test_a_multi_torrent_move_to_top_keeps_the_selection_in_order(client):
	# the mirror of the bottom case, and it needs the opposite application order
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "queue-move-top", {"ids": [hashes[2], hashes[3]]})
	await asyncio.sleep(0.01)

	assert await _positions(client, session_id, hashes) == [2, 3, 0, 1]


async def test_a_multi_torrent_step_keeps_the_selection_in_order(client):
	session_id = await handshake(client)
	hashes = await _queue_of(client, session_id)

	await rpc(client, session_id, "queue-move-down", {"ids": [hashes[0], hashes[1]]})
	await asyncio.sleep(0.01)
	assert await _positions(client, session_id, hashes) == [1, 2, 0, 3]

	await rpc(client, session_id, "queue-move-up", {"ids": [hashes[0], hashes[1]]})
	await asyncio.sleep(0.01)
	assert await _positions(client, session_id, hashes) == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# session torrent ids
# ---------------------------------------------------------------------------
# Core names a torrent by its info_hash and holds no ordinal, so the small int a client
# uses is minted and kept here. It only has to be unique and stable for the session.
async def test_a_torrent_keeps_its_id_for_the_session(client):
	session_id = await handshake(client)
	info_hash = await add_torrent(client, session_id, make_metainfo())

	first = await get_field(client, session_id, info_hash, "id")
	second = await get_field(client, session_id, info_hash, "id")

	assert first == second


async def test_every_torrent_gets_a_distinct_id(client):
	session_id = await handshake(client)
	hashes = [await add_torrent(client, session_id, make_metainfo(f"t{i}.txt".encode())) for i in range(3)]

	ids = [await get_field(client, session_id, h, "id") for h in hashes]

	assert len(set(ids)) == 3


async def test_a_torrent_can_be_addressed_by_its_id(client):
	# the whole point of the id: ids and hashes have to select the same torrent
	session_id = await handshake(client)
	first = await add_torrent(client, session_id, make_metainfo(b"first.txt"))
	second = await add_torrent(client, session_id, make_metainfo(b"second.txt"))
	first_id = await get_field(client, session_id, first, "id")

	data = await rpc(client, session_id, "torrent-get", {"ids": [first_id], "fields": ["hashString"]})

	assert [t["hashString"] for t in data["arguments"]["torrents"]] == [first]
	assert second not in [t["hashString"] for t in data["arguments"]["torrents"]]


async def test_the_id_reported_on_add_is_the_one_torrent_get_uses(client, server):
	session_id = await handshake(client)
	added = await rpc(client, session_id, "torrent-add",
	                  {"metainfo": base64.b64encode(make_metainfo()).decode()})
	await asyncio.sleep(0.01)
	stub = added["arguments"]["torrent-added"]

	assert await get_field(client, session_id, stub["hashString"], "id") == stub["id"]


async def test_ids_are_not_shared_between_sessions(server):
	# they are runtime only: a fresh app has minted nothing
	from yap_torrent_transmission_rpc.components import TorrentIdsEC, get_torrent_ids

	ids = get_torrent_ids(server.env)
	assert get_torrent_ids(server.env) is ids  # one instance for the app
	assert len(server.env.data_storage.get_collection(TorrentIdsEC)) == 1

	first = ids.id_for(b"\x01" * 20)
	assert ids.id_for(b"\x01" * 20) == first
	assert ids.id_for(b"\x02" * 20) != first


async def test_a_removed_torrent_does_not_keep_its_id_for_ever(client, server):
	# the id used to die with the torrent entity; held in our own map it has to be dropped,
	# or a long session accumulates an entry per torrent it ever saw
	from yap_torrent_transmission_rpc.components import get_torrent_ids

	await server.start()
	try:
		session_id = await handshake(client)
		info_hash = await add_torrent(client, session_id, make_metainfo())
		ids = get_torrent_ids(server.env)
		minted = ids.id_for(bytes.fromhex(info_hash))

		await rpc(client, session_id, "torrent-remove", {"ids": [info_hash]})
		await asyncio.sleep(0.01)

		assert bytes.fromhex(info_hash) not in ids._ids
		# and the number is not handed out again
		assert ids.id_for(b"\x07" * 20) != minted
	finally:
		await server.stop()
