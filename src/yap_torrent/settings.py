"""Settings: the knobs a user interface offers, layered over core's config.

Core holds *config* — plain properties with defaults, overridden from `config.json` — and
nothing else. A *setting* is a plugin's idea: one config property that plugin chose to
expose, with its own way of reading a value off the wire and its own note about what
acting on it does. Core registers none of them.

The only thing core gets out of this is a guarded write path to its own config, which is
why `SettingsSystem` lives in core rather than in whichever plugin happens to want it:

	request.setting.register(settings)  ->  a SettingEC entity per exposed property
	request.setting.apply(key, value)   ->  cast, compare, write config
	                                    ->  action.config.changed(key, value)
	                                    ->  action.setting.applied(key, result)

`action.config.changed` is announced by *whoever* writes config — this system, or core
code changing a property directly — and is where a core system listens for the property it
owns. `SettingsSystem` listens too, and re-announces as `action.setting.changed` for the
keys some plugin actually exposes, so an interface can push a new value to its clients
without caring who moved it.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


class SettingResult(Enum):
	APPLIED = "applied"  # changed, and the running client is on the new value
	NEEDS_RESTART = "needs_restart"  # stored, but the property is read once at start-up
	UNCHANGED = "unchanged"
	UNKNOWN = "unknown"  # no plugin registered this key
	INVALID = "invalid"  # the value would not cast


@dataclass(frozen=True)
class Setting:
	"""A config property a plugin offers its users.

	`cast` turns whatever the interface sends into the type the property holds — the
	plugin's problem, because only it knows what its clients send. `note` says why nothing
	acts on the value yet, and is empty when something does.
	"""
	key: str
	cast: Callable[[Any], Any]
	note: str = ""

	@property
	def enforced(self) -> bool:
		return not self.note
