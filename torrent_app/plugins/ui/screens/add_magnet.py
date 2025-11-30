import asyncio

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, Input, Button

from angelovichcore.Dispatcher import Dispatcher


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
		value = self.query_one(Input).value
		event_bus: Dispatcher = self.app.env.event_bus
		asyncio.create_task(event_bus.dispatch("magnet.add", value=value)).result()
		self.app.pop_screen()
