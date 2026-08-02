"""State this plugin owns: runtime settings in the ECS, per-torrent data in custom_data."""
import logging
from typing import Any, Dict, Iterable, List, Optional

from angelovich.core.DataStorage import Entity, EntityComponent

from yap_torrent.env import Env
from yap_torrent.systems import get_custom_data, set_custom_data

logger = logging.getLogger(__name__)

# the `name` these take is the plugin's entry point name, carried on ServerInfo

# --- per-torrent state ------------------------------------------------------
# Labels are a Transmission idea: core has no use for one and never had a reason to know
# what it was. They live in the torrent's custom_data under our name, which core stores
# and reloads without looking inside.
_LABELS = "labels"


def clean_labels(labels: Iterable[str]) -> List[str]:
	"""Strip, drop blanks, and de-duplicate while keeping the order given."""
	seen: List[str] = []
	for label in labels or ():
		text = str(label).strip()
		if text and text not in seen:
			seen.append(text)
	return seen


def get_labels(entity: Entity, name: str) -> List[str]:
	return list(get_custom_data(entity, name, {}).get(_LABELS, ()))


def set_labels(entity: Entity, name: str, labels: Iterable[str]) -> None:
	"""Replace the label set, cleaned. torrent-set's labels is a replace, not a merge.

	Writing the same labels again is not worth a save, and core leaves that judgement to
	us — it stores whatever it is handed.
	"""
	cleaned = clean_labels(labels)
	if cleaned == get_labels(entity, name):
		return

	data = get_custom_data(entity, name, {})
	if cleaned:
		data[_LABELS] = cleaned
	else:
		data.pop(_LABELS, None)
	set_custom_data(entity, name, data)


# --- session torrent ids ----------------------------------------------------
class TorrentIdsEC(EntityComponent):
	"""Transmission's session-scoped integer torrent ids. **Runtime only.**

	The RPC lets a client name a torrent by a small int that need only be unique and
	stable for the life of the session. Core identifies a torrent by its info_hash and
	nothing else — an ordinal is not state its logic needs — so the mapping is ours, is
	never persisted, and starts empty every run, which is exactly what the spec says an
	id is.

	Exactly one instance exists for the whole app; reach it with `get_torrent_ids`.
	"""

	def __init__(self) -> None:
		super().__init__()
		self._ids: Dict[bytes, int] = {}
		self._last: int = 0

	def id_for(self, info_hash: bytes) -> int:
		"""This torrent's session id, minting one on first sight."""
		if info_hash not in self._ids:
			self._last += 1
			self._ids[info_hash] = self._last
		return self._ids[info_hash]

	def forget(self, info_hash: bytes) -> None:
		"""Drop a removed torrent's entry, so a long session's map tracks live torrents
		rather than every torrent it ever saw. `_last` only ever goes up, so the number
		itself is not handed out again."""
		self._ids.pop(info_hash, None)


def get_torrent_ids(env: Env) -> TorrentIdsEC:
	"""The app's one TorrentIdsEC, created on first use."""
	for entity in env.data_storage.get_collection(TorrentIdsEC):
		return entity.get_component(TorrentIdsEC)

	ids = TorrentIdsEC()
	env.data_storage.create_entity().add_component(ids)
	return ids


def torrent_id(env: Env, info_hash: bytes) -> int:
	"""The session id this plugin reports for a torrent."""
	return get_torrent_ids(env).id_for(info_hash)


# --- runtime settings -------------------------------------------------------
class SpeedSettingsEC(EntityComponent):
	"""The parts of Transmission's speed model core does not have. **Runtime only.**

	Core keeps one number per direction where 0 means no limit. Transmission splits each
	limit into a number and an on/off flag, and carries a second "alternative" pair
	(turtle mode) with its own switch. Both are client-side ideas, so they live here.

	Seeded from config at startup and **never written back**: these are live knobs, not
	stored preferences. Turning turtle mode on is a thing you do now, not a thing the
	next run should inherit — and the only value that outlives the process is the one
	core already keeps, the limit actually in force.

	Exactly one instance exists for the whole app; reach it with `get_speed_settings`.

	TODO: once core enforces speed limits, enabling turtle mode should push the alt pair
	 into config.speed_limit_* and restore the normal pair on the way out. Nothing
	 enforces anything yet, so swapping would change nothing but what session-get says.
	"""
	FIELDS = (
		"alt_speed_down", "alt_speed_up", "alt_speed_enabled",
		"last_speed_limit_down", "last_speed_limit_up",
	)

	def __init__(self, stored: Optional[Dict[str, Any]] = None,
	             speed_limit_down: int = 0, speed_limit_up: int = 0):
		super().__init__()
		stored = stored or {}
		self.alt_speed_down: int = int(stored.get("alt_speed_down", 0))
		self.alt_speed_up: int = int(stored.get("alt_speed_up", 0))
		self.alt_speed_enabled: bool = bool(stored.get("alt_speed_enabled", False))
		# what to restore when a limit is switched back on. Seeded from the limits core
		# came up with, so switching one off and on again after a restart gets the number
		# back rather than a zero.
		self.last_speed_limit_down: int = int(stored.get("last_speed_limit_down", speed_limit_down))
		self.last_speed_limit_up: int = int(stored.get("last_speed_limit_up", speed_limit_up))

	def export(self) -> Dict[str, Any]:
		return {name: getattr(self, name) for name in self.FIELDS}

	def update(self, values: Dict[str, Any]) -> bool:
		"""Apply the fields present in `values`; returns whether anything moved."""
		changed = False
		for name in self.FIELDS:
			if name not in values:
				continue
			cast = type(getattr(self, name))
			try:
				new_value = cast(values[name])
			except (TypeError, ValueError):
				logger.warning("Ignoring bad value for %s: %r", name, values[name])
				continue
			if getattr(self, name) != new_value:
				setattr(self, name, new_value)
				changed = True
		return changed

	def reported_limit(self, in_force: int, attr: str) -> int:
		"""The number a client should see in its limit box: the live one, or the last."""
		return in_force or getattr(self, attr)

	def resolve_limit(self, in_force: int, attr: str, value: Any, enabled: Any) -> Optional[int]:
		"""Fold Transmission's (number, flag) pair into core's single "0 means off".

		Returns the value core should hold, or None to leave it alone. A number sent on
		its own does not switch a disabled limit on — that is what the flag is for — so
		it is only remembered.
		"""
		if value is None and enabled is None:
			return None

		if value is not None:
			try:
				remembered = max(0, int(value))
			except (TypeError, ValueError):
				logger.warning("Ignoring bad speed limit %r", value)
				return None
			if remembered:
				setattr(self, attr, remembered)
		else:
			remembered = self.reported_limit(in_force, attr)

		turned_on = bool(enabled) if enabled is not None else bool(in_force)
		return remembered if turned_on else 0


def get_speed_settings(env: Env, config: Optional[Dict[str, Any]] = None) -> SpeedSettingsEC:
	"""The app's one SpeedSettingsEC, created from `config` on first use.

	`config` is only read when the singleton does not exist yet, which is why RpcServer
	seeds it at start-up and every later reader can leave it out.
	"""
	collection = env.data_storage.get_collection(SpeedSettingsEC)
	for entity in collection:
		return entity.get_component(SpeedSettingsEC)

	settings = SpeedSettingsEC(
		config,
		env.config.speed_limit_down,
		env.config.speed_limit_up,
	)
	env.data_storage.create_entity().add_component(settings)
	return settings
