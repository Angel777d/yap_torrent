import argparse
import asyncio
import logging
from pathlib import Path
from typing import Optional

from yap_torrent.config import Config
from yap_torrent.logs import setup_logger

logger = logging.getLogger()


def run(config_path: Optional[str] = None):
	# allow several instances on one host, each with its own config (peer id + ports)
	if config_path is None:
		parser = argparse.ArgumentParser(prog="yap_torrent")
		parser.add_argument("-c", "--config", default=Config.DEFAULT_CONFIG, help="path to the config file")
		config_path = parser.parse_args().config

	cfg = Config(config_path)

	setup_logger(Path(cfg.log_path), logger, use_file=cfg.use_log_file, level=logging.INFO)
	logger.info("Starting yap-torrent")

	close_event = asyncio.Event()

	from yap_torrent.application import Application
	app = Application(cfg)

	try:
		asyncio.run(app.run(close_event))
	except KeyboardInterrupt:
		close_event.set()
