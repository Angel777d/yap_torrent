import asyncio
import logging

from yap_torrent import Env
from yap_torrent.plugins.ui.ui_app import TorrentUIApp

logger = logging.getLogger(__name__)

app = TorrentUIApp()


# yap_torrent.plugins.ui
async def start(env: Env):
	loop = asyncio.get_running_loop()
	app.start(loop, env)


async def update(delta_time: float):
	pass


def close():
	app.stop()


logger.info(f"Torrent App UI plugin imported")
