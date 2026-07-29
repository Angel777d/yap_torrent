"""Tests for runtime configuration.

Settings used to be load-only, so the interesting part is not that a value changes — it
is that a caller can tell an applied setting from one that is merely stored. Both read
back identically, and a client that cannot tell them apart reports a speed limit as
working when nothing enforces it.
"""
import asyncio
import json
from pathlib import Path

from yap_torrent.config import Config, SettingStatus
from yap_torrent.env import Env
from yap_torrent.systems.config_system import ConfigSystem


def _config(tmp_path: Path, **values) -> Config:
	path = tmp_path / "config.json"
	path.write_text(json.dumps(values))
	return Config(path=str(path))


def _env(config: Config) -> Env:
	return Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", config)


# --- the three outcomes a caller has to tell apart --------------------------
def test_an_enforced_setting_applies_immediately(tmp_path):
	config = _config(tmp_path, download_peers_limit=8)
	assert config.set("download_peers_limit", 3) is SettingStatus.CHANGED
	assert config.download_peers_limit == 3


def test_a_stored_but_unenforced_setting_still_reads_back(tmp_path):
	# a client that sets a speed limit and reads back 0 assumes the call failed, so the
	# value is kept — the warning is what says nothing acts on it
	config = _config(tmp_path)
	assert config.set("speed_limit_down", 500) is SettingStatus.CHANGED
	assert config.speed_limit_down == 500
	assert Config.setting("speed_limit_down").enforced is False


def test_a_restart_only_setting_is_stored_without_taking_effect(tmp_path):
	# the socket is already bound; changing the attribute would make the running client
	# report a port it is not listening on
	config = _config(tmp_path, port=6889)
	assert config.set("port", 7000) is SettingStatus.RESTART_REQUIRED
	assert config.port == 6889
	assert json.loads((tmp_path / "config.json").read_text())["port"] == 7000


def test_unknown_keys_and_bad_values_are_refused(tmp_path):
	config = _config(tmp_path)
	assert config.set("no_such_setting", 1) is SettingStatus.UNKNOWN
	assert config.set("download_peers_limit", "not a number") is SettingStatus.INVALID
	assert "no_such_setting" not in config.data


def test_setting_the_same_value_reports_unchanged(tmp_path):
	config = _config(tmp_path, download_peers_limit=8)
	assert config.set("download_peers_limit", 8) is SettingStatus.UNCHANGED


# --- persistence ------------------------------------------------------------
def test_a_change_survives_a_restart(tmp_path):
	config = _config(tmp_path, download_peers_limit=8)
	config.set("download_peers_limit", 2)
	config.set("speed_limit_up", 250)

	reloaded = Config(path=str(tmp_path / "config.json"))
	assert reloaded.download_peers_limit == 2
	assert reloaded.speed_limit_up == 250


def test_booleans_accept_what_a_json_client_sends(tmp_path):
	config = _config(tmp_path)
	config.set("blocklist_enabled", True)
	assert config.blocklist_enabled is True
	config.set("blocklist_enabled", "false")
	assert config.blocklist_enabled is False


def test_a_speed_limit_of_zero_is_how_a_limit_is_turned_off(tmp_path):
	# no separate enabled flag: a limit that is set but switched off is the same thing
	# as no limit, and two fields that can disagree are two fields to keep in sync
	config = _config(tmp_path, speed_limit_down=500)
	assert config.speed_limit_down == 500
	assert Config.setting("speed_limit_down_enabled") is None
	assert not hasattr(config, "speed_limit_down_enabled")

	config.set("speed_limit_down", 0)
	assert config.speed_limit_down == 0


# --- the event surface ------------------------------------------------------
def test_the_system_applies_a_batch_and_announces_only_real_changes(tmp_path):
	async def run():
		config = _config(tmp_path, download_peers_limit=8)
		env = _env(config)
		system = ConfigSystem(env)
		await system.start()

		announced = []

		async def on_changed(changed):
			announced.append(changed)

		env.event_bus.add_listener("action.config.changed", on_changed)

		await asyncio.gather(*env.event_bus.dispatch("request.config.set", {
			"download_peers_limit": 4,  # applied
			"upload_peers_limit": 4,  # unchanged from the default
			"port": 7000,  # restart only
			"nonsense": 1,  # unknown
		}))
		await asyncio.sleep(0)

		assert config.download_peers_limit == 4
		assert announced == [{"download_peers_limit": 4}]

	asyncio.run(run())


def test_nothing_is_announced_when_nothing_changed(tmp_path):
	async def run():
		env = _env(_config(tmp_path))
		system = ConfigSystem(env)
		await system.start()

		announced = []

		async def on_changed(changed):
			announced.append(changed)

		env.event_bus.add_listener("action.config.changed", on_changed)

		await asyncio.gather(*env.event_bus.dispatch("request.config.set", {"nonsense": 1}))
		await asyncio.sleep(0)

		assert announced == []

	asyncio.run(run())


# --- what core does and does not model --------------------------------------
def test_core_has_one_pair_of_speed_limits(tmp_path):
	# a second "alternative" pair and a switch between them (turtle mode) is a
	# client-side idea; core holds only the limits in force
	config = _config(tmp_path)
	assert Config.setting("speed_limit_down") is not None
	assert Config.setting("alt_speed_down") is None
	assert Config.setting("alt_speed_enabled") is None
	assert not hasattr(config, "alt_speed_down")


def test_a_plugin_can_persist_settings_core_does_not_model(tmp_path):
	config = _config(tmp_path)
	config.set_plugin_config("some_plugin", {"turtle": True, "level": 3})
	config.set_plugin_config("some_plugin", {"level": 4})  # merges, not replaces

	reloaded = Config(path=str(tmp_path / "config.json"))
	assert reloaded.get_plugin_config("some_plugin") == {"turtle": True, "level": 4}
