import asyncio

import uvicorn
from fastapi import FastAPI

from torrent_app import Env


class WebApp:
	def __init__(self):
		self.__env = None
		self.__task = None

	async def start(self, app: FastAPI, env: Env):
		self.__env = env

		config = uvicorn.Config(app, host='localhost', port=8000)
		server = uvicorn.Server(config)

		self.__task = asyncio.create_task(server.serve())

	def stop(self):
		self.__task.cancel()

	@property
	def env(self) -> Env:
		return self.__env


rest_app = WebApp()
