"""Runtime state this plugin owns, held in the ECS the rest of the app shares."""
import logging
from typing import Any, Dict, Optional

from angelovich.core.DataStorage import EntityComponent

from yap_torrent.env import Env

logger = logging.getLogger(__name__)

# our section in config.json, read at startup for the initial values
PLUGIN_CONFIG_KEY = "yap_torrent_transmission_rpc"


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


def get_speed_settings(env: Env) -> SpeedSettingsEC:
	"""The app's one SpeedSettingsEC, created from config on first use."""
	collection = env.data_storage.get_collection(SpeedSettingsEC)
	for entity in collection:
		return entity.get_component(SpeedSettingsEC)

	settings = SpeedSettingsEC(
		env.config.get_plugin_config(PLUGIN_CONFIG_KEY),
		env.config.speed_limit_down,
		env.config.speed_limit_up,
	)
	env.data_storage.create_entity().add_component(settings)
	return settings
