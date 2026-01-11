import logging
from typing import List, Set

from yap_torrent.config import Config
from yap_torrent.plugins.torrent_plugin import TorrentPlugin

logger = logging.getLogger(__name__)


def discover_plugins(config: Config) -> List[TorrentPlugin]:
	from importlib.metadata import entry_points

	discovered_plugins = []
	purposes: Set[str] = set()

	plugins = entry_points(group='yap_torrent.plugins')
	for p in plugins:
		name = p.name
		if name in config.disabled_plugins:
			logger.info(f"Plugin {name} disabled")
			continue

		try:
			module = p.load()
			if not hasattr(module, "plugin"):
				logger.warning(f"Plugin module {name} has no 'plugin' attribute")
				continue

			plugin = module.plugin
			if not isinstance(plugin, TorrentPlugin):
				logger.warning(f"Plugin {name} is not inherited from TorrentPlugin")
				continue

			# TODO: rework with specific entry point for UI
			plugin_purpose = plugin.get_purpose()
			if purposes.intersection(plugin_purpose):
				logger.warning(f"Plugin '{name}' has conflicted purposes '{plugin_purpose}'. Skipped")
				continue
			purposes.update(plugin_purpose)

			discovered_plugins.append(plugin)
			logger.info(f"Plugin module {name} discovered. Purposes: '{plugin_purpose}'")
		except ImportError as ex:
			logger.error(f"Plugin module {name} import error: {ex}")
			continue
		except Exception as ex:
			logger.error(f"Plugin module {name} common error: {ex}")
			continue

	return discovered_plugins
