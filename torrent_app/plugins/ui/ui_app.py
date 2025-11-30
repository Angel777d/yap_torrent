from asyncio import AbstractEventLoop, Task
from typing import Optional

from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Footer

from torrent_app import Env
from torrent_app.plugins.ui.screens.add_magnet import AddMagnetDialog
from torrent_app.plugins.ui.screens.bsod import BSOD
from torrent_app.plugins.ui.screens.torrents_list import TorrentsList


class Root(Screen):
	BINDINGS = [
		("b", "app.push_screen('bsod')", "BSOD"),
		("c", "app.push_screen('torrent_list')", "Torrents")
	]

	def compose(self) -> ComposeResult:
		yield Footer()


class TorrentUIApp(App):
	CSS_PATH = "default.tcss"
	SCREENS = {
		"bsod": BSOD,
		"torrent_list": TorrentsList,
		"magnet_dialog": AddMagnetDialog,
	}
	BINDINGS = [
		("m", "app.push_screen('magnet_dialog')", "Add magnet"),
		("ctrl+c", "help_quit"),
	]

	def __init__(self):
		super(TorrentUIApp, self).__init__()
		self.__task: Optional[Task] = None
		self.__env: Optional[Env] = None

	def action_help_quit(self) -> None:
		env = self.env
		env.close_event.set()

	@property
	def env(self) -> Optional[Env]:
		return self.__env

	def on_mount(self) -> None:
		self.push_screen(Root())

	def start(self, loop: AbstractEventLoop, env: Env):
		self.__env = env
		self.__task = loop.create_task(self.run_async(
			headless=False,
			inline=False,
			inline_no_clear=False,
			mouse=True,
			size=None,
			auto_pilot=None
		))

	def stop(self):
		if self.__task:
			self.__task.cancel()
