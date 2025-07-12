import asyncio
import os
import random
import struct
from datetime import datetime, timedelta
from ipaddress import ip_address
from urllib.parse import urlparse


class ServerError(Exception):
	pass


class UdpTrackerClientProto(asyncio.Protocol):
	def __init__(self, client):
		self.client = client

		self.transport = None
		self.received_msg = None

		self.sent_msgs = {}
		self.connection_lost_received = asyncio.Event()

	@classmethod
	async def start(cls, client, loop):
		transport, proto = await loop.create_datagram_endpoint(
			lambda: cls(client),
			remote_addr=client.server_addr)
		return proto

	async def close(self):
		self.transport.close()
		await self.connection_lost_received.wait()

	def connection_made(self, transport):
		self.transport = transport

	def connection_lost(self, exc):
		self.connection_lost_received.set()

	def data_received(self, data):
		if len(data) < 8:
			print('Invalid datagram received.')
			return

		action, tid = struct.unpack('!II', data[:8])
		if tid in self.sent_msgs:
			self.received_msg = (action, tid, data[8:])
			self.sent_msgs[tid].set()
		else:
			print('Invalid transaction ID received.')

	# def error_received(self, exc):
	# 	print('UDP client transmission error: {exc}')

	def get_tid(self):
		tid = random.randint(0, 0xffffffff)
		while tid in self.sent_msgs:
			tid = random.randint(0, 0xffffffff)
		self.sent_msgs[tid] = asyncio.Event()
		return tid

	async def send_msg(self, msg, tid):
		n = 0
		timeout = 15

		for i in range(self.client.max_retransmissions):
			try:
				self.transport.sendto(msg)
				await asyncio.wait_for(
					self.sent_msgs[tid].wait(),
					timeout=timeout)

				del self.sent_msgs[tid]
			except asyncio.TimeoutError:
				if n >= self.client.max_retransmissions - 1:
					del self.sent_msgs[tid]
					raise TimeoutError('Tracker server timeout.')

				action = int.from_bytes(msg[8:12], byteorder='big')
				if action != 0:  # if not CONNECT
					delta = timedelta(seconds=self.client.connection_id_valid_period)
					if self.client.connection_id_timestamp < datetime.now() - delta:
						await self.connect()

				n += 1
				timeout = 15 * 2 ** n

				print(f'Request timeout. Retransmitting. (try #{n}, next timeout {timeout} seconds)')
			else:
				return

	async def connect(self):
		print('Sending connect message.')
		tid = self.get_tid()
		msg = struct.pack('!QII', 0x41727101980, 0, tid)
		await self.send_msg(msg, tid)
		if self.received_msg:
			action, tid, data = self.received_msg
			if action == 3:
				print(f'An error was received in reply to connect: {data.decode()}')
				self.client.connection_id = None
				raise ServerError(f'An error was received in reply to connect: {data.decode()}')
			else:
				self.client.callback('connected')
				self.client.connection_id = int.from_bytes(data, byteorder='big')
				self.client.connection_id_timestamp = datetime.now()

			self.received_msg = None
		else:
			print('No reply received.')

	async def announce(self, info_hash, port, num_want, downloaded, left, uploaded, event=0, ip=0):
		if not self.client.interval or not self.client.connection_id or \
				datetime.now() > self.client.connection_id_timestamp + \
				timedelta(seconds=self.client.connection_id_valid_period):
			# get a connection id first
			await self.connect()

			if not self.client.connection_id:
				print('No reply to connect message.')
				return

		print('Sending announce message.')
		action = 1
		tid = self.get_tid()
		key = random.randint(0, 0xffffffff)
		ip = int.from_bytes(ip_address(ip).packed, byteorder='big')
		msg = struct.pack(
			'!QII20s20sQQQIIIIH', self.client.connection_id, action, tid,
			info_hash, self.client.peer_id, downloaded, left,
			uploaded, event, ip, key, num_want, port)
		await self.send_msg(msg, tid)
		if self.received_msg:
			action, tid, data = self.received_msg
			if action == 3:
				print(f'An error was received in reply to announce: {data.decode()}')
				raise ServerError(
					'An error was received in reply to announce: {}'
					.format(data.decode()))
			else:
				if len(data) < 12:
					print('Invalid announce reply received. Too short.')
					return None
				self.client.interval, leeches, seeders = struct.unpack('!III', data[:12])

			self.received_msg = None

			data = data[12:]
			if len(data) % 6 != 0:
				print('Invalid announce reply received. Invalid length.')
				return None

			peers = [data[i:i + 6] for i in range(0, len(data), 6)]
			peers = [(str(ip_address(p[:4])), int.from_bytes(p[4:], byteorder='big')) for p in peers]

			self.client.callback('announced', info_hash, peers)
		else:
			peers = None
			print('No reply received to announce message.')

		return peers


class TrackerClient:
	def __init__(self, announce_uri, max_retransmissions=8):
		scheme, netloc, _, _, _, _ = urlparse(announce_uri)
		if scheme != 'udp':
			raise ValueError(f'Tracker scheme not supported: {scheme}')
		if ':' not in netloc:
			print('Port not specified in announce URI. Assuming 80.')
			tracker_host, tracker_port = netloc, 80
		else:
			tracker_host, tracker_port = netloc.split(':')
			tracker_port = int(tracker_port)

		self.server_addr = tracker_host, tracker_port
		self.max_retransmissions = max_retransmissions

		self.connection_id_valid_period = 60
		self.connection_id = None
		self.connection_id_timestamp = None
		self.interval = None
		self.peer_id = os.urandom(20)

# def setup_logging(args):
# 	import sys
# 	logger = logging.getLogger(__name__)
# 	formatter = logging.Formatter(
# 		'%(asctime) -15s - %(levelname) -8s - %(message)s')
# 	level = {
# 		'debug': logging.DEBUG,
# 		'info': logging.INFO,
# 		'warning': logging.WARNING,
# 		'error': logging.ERROR,
# 		'critical': logging.CRITICAL
# 	}[args.log_level]
#
# 	if args.log_to_stdout:
# 		handler = logging.StreamHandler(sys.stdout)
# 		handler.setFormatter(formatter)
# 		handler.setLevel(level)
# 		logger.addHandler(handler)
#
# 	if args.log_file:
# 		handler = logging.FileHandler(args.log_file)
# 		handler.setFormatter(formatter)
# 		handler.setLevel(level)
# 		logger.addHandler(handler)
#
# 	logger.setLevel(level)
