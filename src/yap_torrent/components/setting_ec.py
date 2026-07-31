from typing import Callable, Any, Hashable

from angelovich.core.DataStorage import EntityHashComponent


class SettingEC(EntityHashComponent):
	"""One config property a plugin has chosen to expose, keyed by that property.

	One entity per property: registering a key again replaces what is there, so the
	interface that registered last decides how a value for it is read. Core creates none
	of these — they exist only in answer to `request.setting.register`.
	"""

	def __init__(self, key: str, cast: Callable[[Any], Any], note: str = ""):
		super().__init__()
		self.key = key
		self.cast = cast
		self.note = note

	@staticmethod
	def make_hash(key: str) -> Hashable:
		return key

	def __hash__(self):
		return hash(self.make_hash(self.key))
