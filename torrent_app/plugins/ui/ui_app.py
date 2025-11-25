from asyncio import AbstractEventLoop, Task
from typing import Optional

from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.screen import Screen, ModalScreen
from textual.widgets import Static, Footer, ListView, ListItem, Label, Input, Button

from angelovichcore.DataStorage import Entity
from torrent_app import Env
from torrent_app.components.torrent_ec import TorrentHashEC, TorrentInfoEC
from torrent_app.plugins.ui.bsod import BSOD


def get_torrent_name(entity: Optional[Entity], default_text="No Data") -> str:
	if entity:
		if entity.has_component(TorrentInfoEC):
			return entity.get_component(TorrentInfoEC).info.name
		else:
			return entity.get_component(TorrentHashEC).info_hash.hex()
	else:
		return default_text


class TorrentInfo(Static):
	def __init__(self, entity: Optional[Entity] = None):
		super(TorrentInfo, self).__init__(get_torrent_name(entity))
		self.__entity: Optional[Entity] = entity

	def update_entity(self, entity: Optional[Entity]):
		self.__entity = entity
		self.update(get_torrent_name(entity))


class TorrentListItem(ListItem):
	def __init__(self, entity: Entity):
		super().__init__()
		self.entity = entity

	def compose(self) -> ComposeResult:
		yield Label(get_torrent_name(self.entity))


class TorrentsList(Screen):
	CSS = """
	TorrentsList {
	    # align: center middle;
	    # background: blue;
	    # color: white;
        layout: horizontal;
	}

	Static {
	    width: 40;
	}	
	"""
	BINDINGS = [("escape", "app.pop_screen", "Back")]

	def __init__(self):
		super().__init__()
		self.collection = self.app.env.data_storage.get_collection(TorrentHashEC)
		self.selected_index = 0

	def get_selected_entity(self) -> Optional[Entity]:
		items = self.collection.entities
		if items:
			return items[self.selected_index]
		return None

	def compose(self) -> ComposeResult:
		yield ListView(
			*[TorrentListItem(entity) for entity in self.collection.entities],
			initial_index=self.selected_index,
		)

		yield TorrentInfo(self.get_selected_entity())
		yield Footer()

	def render(self):
		return super().render()

	def on_list_view_selected(self, event: ListView.Selected) -> None:
		self.selected_index = event.index
		self.query_one(TorrentInfo).update_entity(self.get_selected_entity())


class Root(Screen):
	BINDINGS = [
		("b", "app.push_screen('bsod')", "BSOD"),
		("c", "app.push_screen('torrent_list')", "Torrents")
	]

	def compose(self) -> ComposeResult:
		yield Footer()


class AddMagnetDialog(ModalScreen):
	CSS = """
	AddMagnetDialog
	{
        align: center middle;
	}
	#dialog
	{
	    padding: 0 1;
	    width: 60;
	    border: thick $background 80%;
	    background: $surface;
	}
	Label {
	    width: 100%;
	    content-align: center middle;
	}
	Input {
	    width: 100%;
	    content-align: center middle;
	}
	Button {
	    width: 100%;
	}
	"""
	BINDINGS = [("escape", "app.pop_screen", "Close")]

	def compose(self) -> ComposeResult:
		yield Vertical(
			Label("Add magnet link:", id="label"),
			Input(),
			Button("Add magnet", variant="primary", id="add_magnet_button"),
			id="dialog"
		)

	def on_button_pressed(self, event: Button.Pressed) -> None:
		# TODO: dispatch event to event bus
		self.app.pop_screen()


class TorrentUIApp(App):
	CSS_PATH = "default.tcss"
	SCREENS = {
		"bsod": BSOD,
		"torrent_list": TorrentsList,
		"magnet_dialog": AddMagnetDialog,
	}
	BINDINGS = [
		("m", "app.push_screen('magnet_dialog')", "Add magnet"),
	]

	def __init__(self):
		super(TorrentUIApp, self).__init__()
		self.__task: Optional[Task] = None
		self.__env: Optional[Env] = None

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
