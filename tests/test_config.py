"""Tests for core config: properties with defaults, overridden from config.json.

Config is deliberately dumb — it knows types and defaults and nothing else. What a user
interface calls a property, whether it is worth offering, and what happens when it changes
is a *setting*, and lives in `test_settings.py`.
"""
import json
from pathlib import Path

from yap_torrent.config import Config


def _config(tmp_path: Path, **values) -> Config:
	path = tmp_path / "config.json"
	path.write_text(json.dumps(values))
	return Config(path=str(path))


def test_defaults_apply_when_the_file_says_nothing(tmp_path):
	config = _config(tmp_path)

	assert config.download_peers_limit == 8
	assert config.port == 6889
	assert config.dht_enabled is True


def test_the_file_overrides_a_default(tmp_path):
	config = _config(tmp_path, download_peers_limit=3, port=7000)

	assert config.download_peers_limit == 3
	assert config.port == 7000


def test_a_missing_file_falls_back_to_defaults(tmp_path):
	# a client run from a fresh directory has to start, not crash
	config = Config(path=str(tmp_path / "nothing-here.json"))

	assert config.download_peers_limit == 8


def test_an_unreadable_file_falls_back_to_defaults(tmp_path):
	path = tmp_path / "config.json"
	path.write_text("{ not json")

	assert Config(path=str(path)).port == 6889


def test_paths_are_derived_from_the_data_folder(tmp_path):
	config = _config(tmp_path, data_folder="D:/somewhere")

	assert config.active_folder == Path("D:/somewhere/active")
	assert config.download_folder == Path("D:/somewhere/download")
	assert str(config.peers_file).endswith("peers.dat")


def test_a_derived_default_can_still_be_overridden(tmp_path):
	config = _config(tmp_path, data_folder="D:/somewhere", download_folder="E:/elsewhere")

	assert config.download_folder == Path("E:/elsewhere")


def test_the_per_torrent_peer_limit_follows_the_global_one(tmp_path):
	config = _config(tmp_path, max_connections=99)

	assert config.peer_limit_per_torrent == 99


def test_a_written_out_boolean_is_read_back_as_one(tmp_path):
	# "false" is a non-empty string: read with bool() it would switch the option *on*
	config = _config(tmp_path, use_log_file="false", dht_enabled="true")

	assert config.use_log_file is False
	assert config.dht_enabled is True


def test_a_peer_id_is_generated_once_and_kept(tmp_path):
	config = _config(tmp_path)
	assert len(config.peer_id) == 20

	reloaded = Config(path=str(tmp_path / "config.json"))
	assert reloaded.peer_id == config.peer_id


def test_a_config_with_no_file_does_not_write_one(tmp_path):
	path = tmp_path / "nothing-here.json"
	Config(path=str(path))  # generates a peer_id, which would otherwise be saved

	assert path.exists() is False


# --- writing back -----------------------------------------------------------
def test_apply_changes_the_property_and_the_file(tmp_path):
	config = _config(tmp_path, download_peers_limit=8)

	config.apply("download_peers_limit", 3)

	assert config.download_peers_limit == 3
	assert json.loads((tmp_path / "config.json").read_text())["download_peers_limit"] == 3


def test_store_writes_the_file_without_touching_the_running_value(tmp_path):
	# how a property that was read once at start-up is changed for the *next* run
	config = _config(tmp_path, port=6889)

	config.store("port", 7000)

	assert config.port == 6889
	assert json.loads((tmp_path / "config.json").read_text())["port"] == 7000


def test_a_path_is_written_as_text(tmp_path):
	config = _config(tmp_path)

	config.apply("download_folder", Path("D:/elsewhere"))

	assert isinstance(json.loads((tmp_path / "config.json").read_text())["download_folder"], str)


def test_has_recognises_properties_and_nothing_else(tmp_path):
	config = _config(tmp_path)

	assert config.has("download_peers_limit") is True
	assert config.has("no_such_property") is False
	assert config.has("_path") is False  # private state is not a property


# --- plugin sections --------------------------------------------------------
def test_a_plugin_can_persist_settings_core_does_not_model(tmp_path):
	config = _config(tmp_path)
	config.set_plugin_config("some_plugin", {"turtle": True, "level": 3})
	config.set_plugin_config("some_plugin", {"level": 4})  # merges, not replaces

	reloaded = Config(path=str(tmp_path / "config.json"))
	assert reloaded.get_plugin_config("some_plugin") == {"turtle": True, "level": 4}
