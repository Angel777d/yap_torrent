import asyncio
import logging

from torrent_app import Env
from torrent_app.plugins.ui.ui_app import TorrentUIApp

logger = logging.getLogger(__name__)

app = TorrentUIApp()


# torrent_app.plugins.ui
async def start(env: Env):
	loop = asyncio.get_running_loop()
	app.start(loop, env)


async def update(delta_time: float):
	pass


def close():
	app.stop()


logger.info(f"Torrent App UI plugin imported")
