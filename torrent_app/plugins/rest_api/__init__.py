import asyncio

from torrent_app import Env


async def create_app():
	from fastapi import FastAPI
	from torrent_app.plugins.rest_api.routers import torrents
	from torrent_app.plugins.rest_api.routers import index

	app = FastAPI()
	app.include_router(index.router)
	app.include_router(torrents.router)

	import uvicorn
	config = uvicorn.Config(app, host='localhost', port=8000)
	server = uvicorn.Server(config)

	state.task = asyncio.create_task(server.serve())


class State:
	def __init__(self):
		self.env = None
		self.server = None
		self.task = None


state = State()


def get_env() -> Env:
	return state.env


# torrent_app.plugins.rest_api
async def start(env: Env):
	state.env = env
	state.task = asyncio.create_task(create_app())


async def update(delta_time: float):
	pass


def close():
	state.task.cancel()
