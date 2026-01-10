from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Footer

from plugins.ui.src.yap_torrent_ui.screens.add_magnet import AddMagnetDialog
from plugins.ui.src.yap_torrent_ui.screens.bsod import BSOD
from plugins.ui.src.yap_torrent_ui.screens.torrents_list import TorrentsList
from yap_torrent import Env


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

	def __init__(self, env: Env):
		super(TorrentUIApp, self).__init__()
		# self.__task: Optional[Task] = None
		self.env: Env = env

	# override default implementation
	def action_help_quit(self) -> None:
		self.env.close_event.set()

	# override default implementation
	def action_quit(self) -> None:
		self.env.close_event.set()

	def on_mount(self) -> None:
		self.push_screen(Root())

	def stop(self):
		self.exit()
