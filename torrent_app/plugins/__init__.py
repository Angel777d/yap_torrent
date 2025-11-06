import importlib
import logging
import pkgutil
from typing import List

from torrent_app import Config, Env

logger = logging.getLogger(__name__)


class TorrentPlugin:
	def __init__(self, name: str, module):
		self.name = name
		self.module = module

	async def start(self, env: Env):
		await self.module.start(env)

	async def update(self, delta_time: float):
		await self.module.update(delta_time)

	def close(self):
		self.module.close()


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
			if not hasattr(module, "start"):
				logger.warning(f"plugin module {name} has no start function")
				continue
			if not hasattr(module, "update"):
				logger.warning(f"plugin module {name} has no update function")
				continue
			if not hasattr(module, "close"):
				logger.warning(f"plugin module {name} has no close function")
				continue

			discovered_plugins.append(TorrentPlugin(name, module))
			logger.info(f"plugin module {name} discovered")
		except ImportError as ex:
			logger.error(f"plugin module {name} import error: {ex}")
			continue
		except Exception as ex:
			logger.error(f"plugin module {name} common error: {ex}")
			continue

	return discovered_plugins
