import asyncio

from yap_torrent import Env
from yap_torrent.plugins.rest_api.global_state import state


async def create_app():
	from fastapi import FastAPI
	from yap_torrent.plugins.rest_api.routers import torrents
	from yap_torrent.plugins.rest_api.routers import index

	app = FastAPI()
	app.include_router(index.router)
	app.include_router(torrents.router)

	import uvicorn
	config = uvicorn.Config(app, host='localhost', port=8000)
	state.server = uvicorn.Server(config)
	asyncio.create_task(state.server.serve())


# yap_torrent.plugins.rest_api
async def start(env: Env):
	state.env = env
	asyncio.create_task(create_app())


async def update(delta_time: float):
	pass


def close():
	asyncio.create_task(state.server.shutdown())
