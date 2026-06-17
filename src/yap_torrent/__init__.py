import asyncio
import logging
from pathlib import Path

from yap_torrent.config import Config
from yap_torrent.logs import setup_logger

logger = logging.getLogger()


def run():
	cfg = Config()

	setup_logger(Path(cfg.log_path), logger, level=logging.INFO)
	logger.info("Starting yap-torrent")

	close_event = asyncio.Event()

	from yap_torrent.application import Application
	app = Application(cfg)

	try:
		asyncio.run(app.run(close_event))
	except KeyboardInterrupt:
		close_event.set()
