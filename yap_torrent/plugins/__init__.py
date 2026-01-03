import importlib
import logging
import pkgutil
from typing import List

from yap_torrent import Config, Env

logger = logging.getLogger(__name__)


class TorrentPlugin:
	async def start(self, env: Env):
		raise NotImplementedError

	async def update(self, delta_time: float):
		pass

	def close(self):
		pass


def discover_plugins(config: Config) -> List[TorrentPlugin]:
	discovered_plugins = []
	for finder, name, is_pkg in pkgutil.iter_modules(__path__, __name__ + "."):
		if not is_pkg:
			continue

		if name in config.disabled_plugins:
			logger.info(f"plugin module {name} disabled")
			continue

		try:
			module = importlib.import_module(name)
			if not hasattr(module, "plugin"):
				logger.warning(f"plugin module {name} has no 'plugin' attribute")
				continue

			if not isinstance(module.plugin, TorrentPlugin):
				logger.warning(f"plugin module {name} plugin is not inherited from TorrentPlugin")
				continue

			discovered_plugins.append(module.plugin)
			logger.info(f"plugin module {name} discovered")
		except ImportError as ex:
			logger.error(f"plugin module {name} import error: {ex}")
			continue
		except Exception as ex:
			logger.error(f"plugin module {name} common error: {ex}")
			continue

	return discovered_plugins
