import asyncio

from torrent_app.application import Application


def start():

	application = Application()
	try:
		asyncio.run(application.run())
	except KeyboardInterrupt:
		application.close()


if __name__ == '__main__':
	start()
