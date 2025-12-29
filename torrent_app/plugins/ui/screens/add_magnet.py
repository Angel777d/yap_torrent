from textual import on
from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Label, Input, Button

from torrent_app import Env


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
	#add_magnet_button {
	    width: 70%;
	}
	#cancel_button {
	    width: 30%;
	}
	"""
	BINDINGS = [("escape", "app.pop_screen", "Close")]

	def compose(self) -> ComposeResult:
		with Vertical(id="dialog"):
			yield Label("Add magnet link:", id="label")
			yield Input()
			with Horizontal():
				yield Button("Cancel", id="cancel_button", variant="error")
				yield Button("Add", id="add_magnet_button", variant="primary")

	@on(Button.Pressed, "#add_magnet_button")
	def add_magnet(self):
		value = self.query_one(Input).value
		env: Env = self.app.env
		env.event_bus.dispatch("request.magnet.add", value=value)
		self.app.pop_screen()

	@on(Button.Pressed, "#cancel_button")
	def add_magnet(self):
		self.app.pop_screen()
