import logging
from typing import Any, Dict

from yap_torrent.config import SettingStatus
from yap_torrent.system import System

logger = logging.getLogger(__name__)


class ConfigSystem(System):
	"""request.config.set -> Config.set (persists), announcing only keys that changed."""

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
