from yap_torrent import Env


class State:
	def __init__(self):
		self.env: Env = None
		self.server = None


state = State()


def get_env() -> Env:
	return state.env
