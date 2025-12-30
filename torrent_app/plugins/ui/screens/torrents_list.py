from typing import Optional

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.screen import Screen
from textual.timer import Timer
from textual.widget import Widget
from textual.widgets import ListView, Footer, ListItem, Label, Button

from angelovichcore.DataStorage import Entity
from torrent_app import Env
from torrent_app.components.torrent_ec import TorrentHashEC
from torrent_app.plugins.ui.utils import get_torrent_name
from torrent_app.systems import calculate_downloaded


class TorrentInfo(Widget):
	CSS = """
		TorrentInfo {
	        layout: vertical;
		}
	"""

	def __init__(self, entity: Optional[Entity] = None):
		super(TorrentInfo, self).__init__()
		self._entity: Optional[Entity] = entity
		self._update_timer: Timer = self.set_interval(5, self.update_time, pause=True)

	def update_entity(self, entity: Optional[Entity]):
		self._entity = entity
		self.update()

	def compose(self) -> ComposeResult:
		yield Label(id="torrent-name")
		yield Label(id="torrent-downloaded")
		with Horizontal():
			yield Button("+peers", id="add-peers-button")
			yield Button("Button2", id="button2")

	def on_mount(self):
		self.update()

	def update(self):
		self.query_one("#torrent-name", expect_type=Label).update(get_torrent_name(self._entity))
		for button in self.query(Button):
			button.disabled = not bool(self._entity)

		if self._entity:
			self._update_timer.resume()
			self.update_time()
		else:
			self._update_timer.pause()

	def update_time(self):
		value = ""
		if self._entity:
			value = calculate_downloaded(self._entity)
		self.query_one("#torrent-downloaded", expect_type=Label).update(f"Complete: {value:.2%}")

	@on(Button.Pressed, "#add-peers-button")
	def add_peers(self):
		env: Env = self.app.env
		env.event_bus.dispatch("request.dht.more_peers", self._entity.get_component(TorrentHashEC).info_hash)


class TorrentsList(Screen):
	CSS = """
	TorrentsList {
	    # align: center middle;
	    # background: blue;
	    # color: white;
        layout: horizontal;
	}

	TorrentInfo {
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


class TorrentListItem(ListItem):
	def __init__(self, entity: Entity):
		super().__init__()
		self.entity = entity

	def compose(self) -> ComposeResult:
		yield Label(get_torrent_name(self.entity))
