import asyncio
import logging

from torrent_app.application import Application

logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class MainLoop:
	def __init__(self):
		self.application = Application()

	async def run(self, close_event: asyncio.Event):
		asyncio.create_task(self.application.run())
		await close_event.wait()

	def stop(self):
		self.application.stop()


if __name__ == '__main__':
	event = asyncio.Event()
	main = MainLoop()

	try:
		asyncio.run(main.run(event))
	except KeyboardInterrupt:
		main.stop()
