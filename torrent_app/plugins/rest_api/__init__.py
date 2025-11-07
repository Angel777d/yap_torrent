from torrent_app import Env
from .root import app
from .web_app import rest_app


# torrent_app.plugins.rest_api
async def start(env: Env):
	await rest_app.start(app, env)


async def update(delta_time: float):
	pass


def close():
	rest_app.stop()
