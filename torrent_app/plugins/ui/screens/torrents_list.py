from typing import Optional

from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import ListView, Footer, ListItem, Label, Static

from angelovichcore.DataStorage import Entity
from torrent_app.components.torrent_ec import TorrentHashEC
from torrent_app.plugins.ui.utils import get_torrent_name


class TorrentInfo(Static):
	def __init__(self, entity: Optional[Entity] = None):
		super(TorrentInfo, self).__init__(get_torrent_name(entity))
		self.__entity: Optional[Entity] = entity

	def update_entity(self, entity: Optional[Entity]):
		self.__entity = entity
		self.update(get_torrent_name(entity))


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


class TorrentListItem(ListItem):
	def __init__(self, entity: Entity):
		super().__init__()
		self.entity = entity

	def compose(self) -> ComposeResult:
		yield Label(get_torrent_name(self.entity))
