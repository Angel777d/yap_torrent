import asyncio
import logging

from yap_torrent.logs import setup_logger

logger = logging.getLogger()


def run():
	setup_logger(logger, level=logging.INFO)
	logger.info("Starting yap-torrent")

	close_event = asyncio.Event()

	from yap_torrent.application import Application
	app = Application()

	try:
		asyncio.run(app.run(close_event))
	except KeyboardInterrupt:
		close_event.set()
