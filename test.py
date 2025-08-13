import asyncio
import math
from pathlib import Path

import upnp
from torrent import load_torrent_file
from torrent.structures import FileInfo, PieceData
from torrent.tracker import make_announce

port = 6889

internal_ip = upnp.get_my_ip()
service = upnp.discover(internal_ip)
open_res = "error open port"
if service:
	open_res = upnp.open_port(service, port, internal_ip)
external_ip = upnp.get_my_ext_ip()

print(f"port: {port}, external_ip: {external_ip}, internal_ip: {internal_ip}, open port result: {open_res}")


class FileData:
	def __init__(self, info: FileInfo, start: int, piece_length: int):
		self.start = start
		self.info = info
		self.first_index = math.floor(start / piece_length)
		self.last_index = math.ceil(start + info.length / piece_length)

	def add_piece(self, piece: PieceData):
		if self.first_index <= piece.index <= self.last_index:
			# TODO: save to file
			pass


async def load_torrent():
	pass


async def main():
	peer_id = b'-PY0001-111111111111'
	file_path = Path("data/watch/ubuntu-24.10-desktop-amd64.iso.torrent")
	print(file_path.exists(), file_path.absolute())
	torrent_info = load_torrent_file(file_path)

	print(torrent_info)

	left = torrent_info.size
	event = "started"  # "started", "completed", "stopped"

	print(f"make announce to: {torrent_info.announce}")
	result = make_announce(
		torrent_info.announce,
		torrent_info.info_hash,
		peer_id=peer_id,
		left=left,
		port=port,
		ip=external_ip,
		event=event,
		compact=1
	)

	peer_task = None
	for peer in result.peers:
		pass


asyncio.run(main())
