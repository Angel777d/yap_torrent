import asyncio
from os import system

from angelovich.core.DataStorage import Entity

from yap_torrent.components.bitfield_ec import BitfieldEC
from yap_torrent.components.torrent_ec import TorrentHashEC, TorrentInfoEC
from yap_torrent.env import Env
from yap_torrent.systems import get_torrent_name


def _cls():
	system("clear||cls")


def _torrent_list(env: Env, loop: asyncio.AbstractEventLoop):
	_cls()
	torrents = env.data_storage.get_collection(TorrentHashEC).entities
	for index, torrent_entity in enumerate(torrents):
		print(f"{index + 1}. {get_torrent_name(torrent_entity)}")
	print(f"0. Exit")

	value = input("Select torrent: ")
	if value.isdigit():
		index = int(value)
		if index == 0:
			env.close_event.set()
			return
		if index == 100:
			leftovers = asyncio.all_tasks(loop=loop)
			for task in leftovers:
				print(task)
			input("Press enter to exit")
		index = index - 1
		if index < len(torrents):
			loop.run_in_executor(None, _torrent, env, loop, torrents[index])
			return

	# refresh page
	loop.run_in_executor(None, _torrent_list, env, loop)


def _torrent(env: Env, loop: asyncio.AbstractEventLoop, torrent_entity: Entity):
	_cls()
	info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
	if torrent_entity.has_component(TorrentInfoEC):
		info = torrent_entity.get_component(TorrentInfoEC).info
		print(info.name)
		print(f"Complete: {info.calculate_downloaded(torrent_entity.get_component(BitfieldEC).have_num):.2%}")
	else:
		print(info_hash.hex())

	print("1. Stop\r\n2. Start\r\n3. Invalidate\r\n4. Delete\r\n0. Back")
	action = input("Select action: ")

	match action:
		case "1":
			loop.create_task(send_event(env, "request.torrent.stop", info_hash))
		case "2":
			loop.create_task(send_event(env, "request.torrent.start", info_hash))
		case "3":
			loop.create_task(send_event(env, "request.torrent.invalidate", info_hash))
		case "4":
			loop.create_task(send_event(env, "request.torrent.remove", info_hash))

	loop.run_in_executor(None, _torrent_list, env, loop)


async def send_event(env, action, *args):
	env.event_bus.dispatch(action, *args)


def root(env: Env, loop: asyncio.AbstractEventLoop):
	loop.run_in_executor(None, _torrent_list, env, loop)
