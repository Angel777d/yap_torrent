import importlib
import logging
import pkgutil

from torrent_app import Config

logger = logging.getLogger(__name__)


def discover_plugins(config: Config):
	discovered_plugins = {}
	for finder, name, is_pkg in pkgutil.iter_modules(__path__, __name__ + "."):
		if not is_pkg:
			continue

		if name in config.disabled_plugins:
			logger.info(f"plugin module {name} disabled")
			continue

		try:
			module = importlib.import_module(name)
			if not hasattr(module, "init_plugin"):
				logger.warning(f"plugin module {name} has no init_plugin function")
				continue
			discovered_plugins[name] = module
			logger.info(f"plugin module {name} discovered")
		except ImportError as ex:
			logger.error(f"plugin module {name} import error: {ex}")
			continue
		except Exception as ex:
			logger.error(f"plugin module {name} common error: {ex}")
			continue

	return discovered_plugins
