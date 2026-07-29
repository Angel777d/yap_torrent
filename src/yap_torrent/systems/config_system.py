import logging
from typing import Any, Dict

from yap_torrent.config import SettingStatus
from yap_torrent.env import Env
from yap_torrent.system import System

logger = logging.getLogger(__name__)


class ConfigSystem(System):
	"""Runtime settings: the one way anything changes `Config` after start.

	Settings were load-only, so a UI or a remote client had no way to change anything
	without editing config.json and restarting. Changes are written straight back to the
	file (`Config.set` persists), and `action.config.changed` carries only the keys that
	actually took effect — a listener re-reading on a no-op would be doing work for a
	value that did not move.
	"""

	async def start(self):
		self.add_listener("request.config.set", self._on_set)

	async def _on_set(self, values: Dict[str, Any]):
		if not values:
			return

		changed: Dict[str, Any] = {}
		for key, value in values.items():
			status = self.env.config.set(key, value)
			if status is SettingStatus.CHANGED:
				changed[key] = self.env.config.get(key)

		if changed:
			logger.info("Config changed: %s", ", ".join(sorted(changed)))
			await self.env.event_bus.dispatch_async("action.config.changed", changed)
