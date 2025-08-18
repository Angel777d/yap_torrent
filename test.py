import asyncio

progress = True
async def main():
	print("start")

	while progress:
		await asyncio.sleep(1)
		print("tick")

	print("end")

try:
	asyncio.run(main())
except KeyboardInterrupt as e:
	print("Caught keyboard interrupt. Canceling tasks...")
	progress = False
