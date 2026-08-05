from angelovich.core.System import System

from yap_torrent.env import Env


class UISystem(System):
	def __init__(self, env: Env):
		super().__init__(env)
		self._app = None

	async def start(self):
		from .ui_app import TorrentUIApp
		self._app = TorrentUIApp(self.env)
		self.add_task(self._app.run_async(
			headless=False,
			inline=False,
			inline_no_clear=False,
			mouse=True,
			size=None,
			auto_pilot=None
		))

	def close(self):
		if self._app:
			self._app.stop()
		super().close()
