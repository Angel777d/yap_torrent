import asyncio


class TerminalApp:
	def __init__(self):
		self.wait_for_input = False

	async def interaction(self):
		while True:
			loop = asyncio.get_event_loop()
			result = await loop.run_in_executor(None, input, "what?: ")
			print(result)

	async def run(self):
		asyncio.create_task(self.interaction())

	def stop(self):
		pass
