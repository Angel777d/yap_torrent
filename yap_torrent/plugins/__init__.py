import importlib
import logging
import pkgutil
from typing import List, Set

from yap_torrent import Config, Env

logger = logging.getLogger(__name__)


class TorrentPlugin:
	async def start(self, env: Env):
		raise NotImplementedError

	async def update(self, delta_time: float):
		pass

	def close(self):
		pass

	@staticmethod
	def get_purpose() -> Set[str]:
		return set()


def discover_plugins(config: Config) -> List[TorrentPlugin]:
	discovered_plugins = []
	purposes: Set[str] = set()
	for finder, name, is_pkg in pkgutil.iter_modules(__path__, __name__ + "."):
		if not is_pkg:
			continue

		if name in config.disabled_plugins:
			logger.info(f"Plugin {name} disabled")
			continue

		try:
			module = importlib.import_module(name)
			if not hasattr(module, "plugin"):
				logger.warning(f"Plugin module {name} has no 'plugin' attribute")
				continue

			if not isinstance(module.plugin, TorrentPlugin):
				logger.warning(f"Plugin {name} is not inherited from TorrentPlugin")
				continue

			plugin_purpose = module.plugin.get_purpose()
			if purposes.intersection(plugin_purpose):
				logger.warning(f"Plugin '{name}' has conflicted purposes '{plugin_purpose}'. Skipped")
				continue
			purposes.update(plugin_purpose)

			discovered_plugins.append(module.plugin)
			logger.info(f"Plugin module {name} discovered. Purposes: '{plugin_purpose}'")
		except ImportError as ex:
			logger.error(f"Plugin module {name} import error: {ex}")
			continue
		except Exception as ex:
			logger.error(f"Plugin module {name} common error: {ex}")
			continue

	return discovered_plugins
