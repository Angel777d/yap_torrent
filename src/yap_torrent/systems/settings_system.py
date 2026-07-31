import logging
from typing import Any, Iterable

from yap_torrent.components.setting_ec import SettingEC
from yap_torrent.config import STARTUP_ONLY
from yap_torrent.settings import Setting, SettingResult
from yap_torrent.system import System

logger = logging.getLogger(__name__)


class SettingsSystem(System):
	"""The guarded write path to core's config — the one thing settings owe core.

	Plugins register what they expose and ask for changes; core registers nothing and only
	listens. Everything an interface brings — the name its clients use for a property, how
	to read a value for it, what to say about a value nothing acts on — stays in the
	plugin.
	"""

	async def start(self):
		self.add_listener("request.setting.register", self._on_register)  # plugin register settings
		self.add_listener("request.setting.apply", self._on_apply)  # plugin requested value change
		self.add_listener("action.config.changed", self._on_config_changed)  # config changed by core or other settings

	def _find(self, key: str):
		return self.env.data_storage.get_collection(SettingEC).find(SettingEC.make_hash(key))

	async def _on_config_changed(self, key: str, value: Any):
		"""Re-announce a config change, but only for a property some interface offers."""
		if self._find(key) is None:
			return
		await self.env.event_bus.dispatch_async("action.setting.changed", key, value)

	async def _on_register(self, settings: Iterable[Setting]):
		for setting in settings:
			if not self.env.config.has(setting.key):
				logger.warning("Ignoring setting for unknown config property '%s'", setting.key)
				continue

			entity = self._find(setting.key)
			if entity is None:
				entity = self.env.data_storage.create_entity()
				entity.add_component(SettingEC(setting.key, setting.cast, setting.note))
				continue

			# registered again: the interface that asked last decides how a value is read
			existing = entity.get_component(SettingEC)
			existing.cast = setting.cast
			existing.note = setting.note

	async def _on_apply(self, key: str, value: Any):
		result = await self._apply(key, value)
		await self.env.event_bus.dispatch_async("action.setting.applied", key, result)

	async def _apply(self, key: str, value: Any) -> SettingResult:
		entity = self._find(key)
		if entity is None:
			logger.warning("Ignoring unregistered setting '%s'", key)
			return SettingResult.UNKNOWN

		setting = entity.get_component(SettingEC)
		try:
			new_value = setting.cast(value)
		except (TypeError, ValueError) as ex:
			logger.warning("Ignoring bad value for '%s': %r (%s)", key, value, ex)
			return SettingResult.INVALID

		config = self.env.config
		if getattr(config, key) == new_value:
			return SettingResult.UNCHANGED

		if setting.note:
			logger.warning("Setting '%s' is stored but NOT enforced: %s", key, setting.note)

		if key in STARTUP_ONLY:
			# the running client read this once already; changing the property would make
			# it report a port it is not listening on
			config.store(key, new_value)
			logger.warning("Setting '%s' is stored but only takes effect after a restart", key)
			await self.env.event_bus.dispatch_async("action.setting.need_restart", key)
			return SettingResult.NEEDS_RESTART

		config.apply(key, new_value)
		logger.info("Config changed: %s", key)
		await self.env.event_bus.dispatch_async("action.config.changed", key, new_value)
		return SettingResult.APPLIED
