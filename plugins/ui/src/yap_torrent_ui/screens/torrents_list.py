from typing import Optional

from angelovich.core.DataStorage import Entity
from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.events import ScreenResume, ScreenSuspend
from textual.screen import Screen
from textual.timer import Timer
from textual.widget import Widget
from textual.widgets import ListView, Footer, ListItem, Label, Button

from yap_torrent.components.torrent_ec import TorrentHashEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.systems import calculate_downloaded
from ..utils import get_torrent_name


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
		yield Label(id="torrent-completed")
		yield Label(id="torrent-downloaded")
		yield Label(id="torrent-uploaded")
		with Horizontal():
			yield Button("+peers", id="add-peers-button")
			yield Button("Check", id="check-torrent-button")

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
		downloaded = uploaded = completed = ""
		if self._entity:
			completed = calculate_downloaded(self._entity)
			downloaded = self._entity.get_component(TorrentStatsEC).downloaded
			uploaded = self._entity.get_component(TorrentStatsEC).uploaded

		self.query_one("#torrent-completed", expect_type=Label).update(f"Complete: {completed:.2%}")
		self.query_one("#torrent-downloaded", expect_type=Label).update(f"Downloaded: {downloaded:,} bytes")
		self.query_one("#torrent-uploaded", expect_type=Label).update(f"Uploaded: {uploaded:,} bytes")

	@on(Button.Pressed, "#add-peers-button")
	def add_peers(self):
		env: Env = self.app.env
		env.event_bus.dispatch("request.dht.more_peers", self._entity.get_component(TorrentHashEC).info_hash)

	@on(Button.Pressed, "#check-torrent-button")
	def invalidate(self):
		env: Env = self.app.env
		env.event_bus.dispatch("request.torrent.invalidate", self._entity.get_component(TorrentHashEC).info_hash)


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
		env: Env = self.app.env
		self.collection = env.data_storage.get_collection(TorrentHashEC)
		self.selected_index = 0

	def get_selected_entity(self) -> Optional[Entity]:
		items = self.collection.entities
		if items:
			return items[self.selected_index]
		return None

	def compose(self) -> ComposeResult:
		yield ListView(
			*(TorrentListItem(entity) for entity in self.collection.entities),
			initial_index=self.selected_index,
		)

		yield TorrentInfo(self.get_selected_entity())
		yield Footer()

	@on(ScreenResume)
	def on_resume(self, event: ScreenResume) -> None:
		self.collection.add_listener(self.collection.EVENT_ADDED, self._on_collection_changed, scope=self)
		self.collection.add_listener(self.collection.EVENT_REMOVED, self._on_collection_changed, scope=self)
		self.refresh(recompose=True)

	@on(ScreenSuspend)
	def on_suspend(self, event: ScreenSuspend) -> None:
		self.collection.remove_all_listeners(scope=self)

	async def _on_collection_changed(self, *_):
		self.refresh(recompose=True)

	def render(self):
		return super().render()

	@on(ListView.Selected)
	def on_selected(self, event: ListView.Selected) -> None:
		self.selected_index = event.index
		self.query_one(TorrentInfo).update_entity(self.get_selected_entity())


class TorrentListItem(ListItem):
	def __init__(self, entity: Entity):
		super().__init__()
		self.entity = entity

	def compose(self) -> ComposeResult:
		yield Label(get_torrent_name(self.entity))
