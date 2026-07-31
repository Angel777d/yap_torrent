"""Tests for settings: the knobs a plugin offers, layered over core's config.

Core registers nothing — a setting exists because some interface chose to expose a config
property and brought its own way of reading a value for it. What core owns is the guarded
write path, which is the whole reason this lives in core at all, so the events matter as
much as the values: they are how anything else finds out.
"""
import asyncio
import json
from pathlib import Path

from yap_torrent.components.setting_ec import SettingEC
from yap_torrent.config import Config, as_bool
from yap_torrent.env import Env
from yap_torrent.settings import Setting, SettingResult
from yap_torrent.systems.settings_system import SettingsSystem


def _env(tmp_path: Path, **values) -> Env:
	path = tmp_path / "config.json"
	path.write_text(json.dumps(values))
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", Config(path=str(path)))


async def _started(env: Env, *settings: Setting) -> SettingsSystem:
	system = SettingsSystem(env)
	await system.start()
	if settings:
		await env.event_bus.dispatch_async("request.setting.register", settings)
	return system


def _listen(env: Env, event: str) -> list:
	seen = []

	async def on_event(*args):
		seen.append(args)

	env.event_bus.add_listener(event, on_event)
	return seen


async def _apply(env: Env, key, value) -> SettingResult:
	results = _listen(env, "action.setting.applied")
	await env.event_bus.dispatch_async("request.setting.apply", key, value)
	return results[-1][1]


# --- registration -----------------------------------------------------------
def test_registering_creates_one_entity_per_property(tmp_path):
	async def run():
		env = _env(tmp_path)
		await _started(env, Setting("download_peers_limit", int), Setting("upload_peers_limit", int))

		collection = env.data_storage.get_collection(SettingEC)
		assert len(collection) == 2
		assert collection.find(SettingEC.make_hash("download_peers_limit")) is not None

	asyncio.run(run())


def test_a_setting_for_a_property_core_does_not_have_is_refused(tmp_path):
	async def run():
		env = _env(tmp_path)
		await _started(env, Setting("no_such_property", int))

		assert len(env.data_storage.get_collection(SettingEC)) == 0

	asyncio.run(run())


def test_registering_a_key_again_replaces_it_rather_than_adding(tmp_path):
	async def run():
		env = _env(tmp_path)
		await _started(env, Setting("download_peers_limit", int))
		await env.event_bus.dispatch_async(
			"request.setting.register", [Setting("download_peers_limit", int, note="superseded")])

		collection = env.data_storage.get_collection(SettingEC)
		assert len(collection) == 1
		assert collection.find(SettingEC.make_hash("download_peers_limit")).get_component(
			SettingEC).note == "superseded"

	asyncio.run(run())


# --- applying ---------------------------------------------------------------
def test_an_unregistered_key_changes_nothing(tmp_path):
	async def run():
		env = _env(tmp_path)
		await _started(env)

		assert await _apply(env, "download_peers_limit", 3) is SettingResult.UNKNOWN
		assert env.config.download_peers_limit == 8

	asyncio.run(run())


def test_a_registered_setting_changes_the_property_and_the_file(tmp_path):
	async def run():
		env = _env(tmp_path, download_peers_limit=8)
		await _started(env, Setting("download_peers_limit", int))

		assert await _apply(env, "download_peers_limit", 3) is SettingResult.APPLIED
		assert env.config.download_peers_limit == 3
		assert json.loads((tmp_path / "config.json").read_text())["download_peers_limit"] == 3

	asyncio.run(run())


def test_the_same_value_again_is_not_a_change(tmp_path):
	async def run():
		env = _env(tmp_path, download_peers_limit=8)
		await _started(env, Setting("download_peers_limit", int))

		assert await _apply(env, "download_peers_limit", 8) is SettingResult.UNCHANGED

	asyncio.run(run())


def test_a_value_that_will_not_cast_is_refused(tmp_path):
	async def run():
		env = _env(tmp_path, download_peers_limit=8)
		await _started(env, Setting("download_peers_limit", int))

		assert await _apply(env, "download_peers_limit", "not a number") is SettingResult.INVALID
		assert env.config.download_peers_limit == 8

	asyncio.run(run())


def test_the_cast_is_the_interfaces_own(tmp_path):
	# core stores a bool; how a client spells one is the interface's problem
	async def run():
		env = _env(tmp_path)
		await _started(env, Setting("blocklist_enabled", as_bool))

		await _apply(env, "blocklist_enabled", "yes")
		assert env.config.blocklist_enabled is True

		await _apply(env, "blocklist_enabled", "false")
		assert env.config.blocklist_enabled is False

	asyncio.run(run())


# --- what a change announces ------------------------------------------------
def test_a_change_announces_the_config_key_and_its_new_value(tmp_path):
	# this is the event a core system owning that property listens for
	async def run():
		env = _env(tmp_path, download_peers_limit=8)
		await _started(env, Setting("download_peers_limit", int))
		seen = _listen(env, "action.config.changed")

		await _apply(env, "download_peers_limit", 3)

		assert seen == [("download_peers_limit", 3)]

	asyncio.run(run())


def test_a_config_change_is_re_announced_for_a_property_someone_offers(tmp_path):
	# an interface pushes the new value to its clients without caring who moved it
	async def run():
		env = _env(tmp_path, download_peers_limit=8)
		await _started(env, Setting("download_peers_limit", int))
		seen = _listen(env, "action.setting.changed")

		await env.event_bus.dispatch_async("action.config.changed", "download_peers_limit", 3)

		assert seen == [("download_peers_limit", 3)]

	asyncio.run(run())


def test_a_config_change_nobody_offers_is_not_re_announced(tmp_path):
	async def run():
		env = _env(tmp_path)
		await _started(env)  # nothing registered
		seen = _listen(env, "action.setting.changed")

		await env.event_bus.dispatch_async("action.config.changed", "download_peers_limit", 3)

		assert seen == []

	asyncio.run(run())


def test_nothing_is_announced_when_nothing_changed(tmp_path):
	async def run():
		env = _env(tmp_path, download_peers_limit=8)
		await _started(env, Setting("download_peers_limit", int))
		seen = _listen(env, "action.config.changed")

		await _apply(env, "download_peers_limit", 8)

		assert seen == []

	asyncio.run(run())


# --- properties read once at start-up ---------------------------------------
def test_a_startup_only_setting_is_stored_without_taking_effect(tmp_path):
	# the socket is already bound; changing the property would make the running client
	# report a port it is not listening on
	async def run():
		env = _env(tmp_path, port=6889)
		await _started(env, Setting("port", int))
		restarts = _listen(env, "action.setting.need_restart")

		assert await _apply(env, "port", 7000) is SettingResult.NEEDS_RESTART
		assert env.config.port == 6889  # still what we are listening on
		assert json.loads((tmp_path / "config.json").read_text())["port"] == 7000
		assert restarts == [("port",)]

	asyncio.run(run())


def test_a_startup_only_setting_does_not_announce_a_live_change(tmp_path):
	async def run():
		env = _env(tmp_path, port=6889)
		await _started(env, Setting("port", int))
		seen = _listen(env, "action.config.changed")

		await _apply(env, "port", 7000)

		assert seen == []  # nothing changed on the running client

	asyncio.run(run())


# --- settings nothing acts on ------------------------------------------------
def test_an_unenforced_setting_still_reads_back(tmp_path):
	# a client that sets a speed limit and reads back 0 assumes the call failed, so the
	# value is kept — the note is what says nothing acts on it
	async def run():
		env = _env(tmp_path)
		await _started(env, Setting("speed_limit_down", int, note="nothing limits bandwidth"))

		assert await _apply(env, "speed_limit_down", 500) is SettingResult.APPLIED
		assert env.config.speed_limit_down == 500

	asyncio.run(run())


def test_a_setting_with_no_note_counts_as_enforced():
	assert Setting("download_peers_limit", int).enforced is True
	assert Setting("speed_limit_down", int, note="nothing acts on it").enforced is False
