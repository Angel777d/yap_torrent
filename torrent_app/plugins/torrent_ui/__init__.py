import asyncio
import logging
from asyncio import AbstractEventLoop
from datetime import datetime

from textual.app import App, ComposeResult
from textual.widgets import Digits

from torrent_app import Env

logger = logging.getLogger(__name__)


class ClockApp(App):
	CSS = """
    Screen { align: center middle; }
    Digits { width: auto; }
    """

	def __init__(self):
		super().__init__()
		self.__task = None

	def compose(self) -> ComposeResult:
		yield Digits("")

	def on_ready(self) -> None:
		self.update_clock()
		self.set_interval(1, self.update_clock)

	def update_clock(self) -> None:
		clock = datetime.now().time()
		self.query_one(Digits).update(f"{clock:%T}")

	def start(self, loop: AbstractEventLoop):
		self.__task = loop.create_task(app.run_async(
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


app = ClockApp()


# torrent_app.plugins.torrent_ui
async def start(env: Env):
	loop = asyncio.get_running_loop()
	app.start(loop)


async def update(delta_time: float):
	pass


def close():
	app.stop()


logger.info(f"Torrent App UI plugin imported")
