import asyncio
import logging

from terminal.TerminalApp import TerminalApp
from torrent_app.application import Application

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(LOG_FORMAT))
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# handler = logging.FileHandler('torrent.log')
# handler.setFormatter(logging.Formatter(LOG_FORMAT))
# handler.setLevel(logging.ERROR)
# logger.addHandler(handler)


class MainLoop:
	def __init__(self):
		self.application = Application()
		self.terminal_app = TerminalApp()

	async def run(self, close_event: asyncio.Event):
		asyncio.create_task(self.application.run())
		asyncio.create_task(self.terminal_app.run())
		await close_event.wait()

	def stop(self):
		self.application.stop()
		self.terminal_app.stop()


if __name__ == '__main__':
	event = asyncio.Event()
	main = MainLoop()

	try:
		asyncio.run(main.run(event))
	except KeyboardInterrupt:
		main.stop()
