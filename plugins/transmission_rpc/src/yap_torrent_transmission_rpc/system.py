from typing import Optional

from angelovich.core.System import System

from yap_torrent.env import Env
from .server import RpcServer


class TransmissionRpcSystem(System):
	def __init__(self, env: Env, name: str):
		super().__init__(env)
		self._name = name
		self._server: Optional[RpcServer] = None

	async def start(self):
		self._server = await RpcServer(self._name, self.env).start()

	async def stop(self):
		if self._server:
			await self._server.stop()
		await super().stop()

	def close(self):
		self._server = None
		super().close()
