import asyncio
import logging

from torrent_app.application import Application

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# handler = logging.StreamHandler()
# handler.setFormatter(logging.Formatter(LOG_FORMAT))
# handler.setLevel(logging.DEBUG)
# logger.addHandler(handler)


handler = logging.FileHandler('torrent.log')
handler.setFormatter(logging.Formatter(LOG_FORMAT))
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)




if __name__ == '__main__':
	event = asyncio.Event()
	application = Application()

	try:
		asyncio.run(application.run(event))
	except KeyboardInterrupt:
		event.set()
