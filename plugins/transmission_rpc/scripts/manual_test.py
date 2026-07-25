#!/usr/bin/env python3
"""Standalone smoke test for a *running* yap_torrent Transmission RPC server.

Unlike the pytest suite (which drives the RPC layer in-process), this script
talks real HTTP to a live instance, so you can verify it end-to-end and confirm
the CSRF handshake works the way real Transmission clients expect.

Usage:
    # start yap_torrent with the plugin installed, then:
    python plugins/transmission_rpc/scripts/manual_test.py \
        --url http://127.0.0.1:9091/transmission/rpc

    # optionally add a magnet:
    python .../manual_test.py --magnet "magnet:?xt=urn:btih:..."

Only the Python standard library is used.
"""
import argparse
import json
import sys
import urllib.error
import urllib.request

CSRF_HEADER = "X-Transmission-Session-Id"


class Client:
	def __init__(self, url: str):
		self.url = url
		self.session_id = ""

	def call(self, method: str, arguments: dict | None = None) -> dict:
		payload = json.dumps({"method": method, "arguments": arguments or {}}).encode("utf-8")
		for _ in range(2):  # one retry to pick up a fresh CSRF id
			req = urllib.request.Request(self.url, data=payload, method="POST")
			req.add_header("Content-Type", "application/json")
			if self.session_id:
				req.add_header(CSRF_HEADER, self.session_id)
			try:
				with urllib.request.urlopen(req) as resp:
					return json.loads(resp.read().decode("utf-8"))
			except urllib.error.HTTPError as err:
				if err.code == 409 and CSRF_HEADER in err.headers:
					# Expected first-contact handshake: grab the id and retry.
					self.session_id = err.headers[CSRF_HEADER]
					continue
				raise
		raise RuntimeError("CSRF handshake did not settle after retry")


def main() -> int:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--url", default="http://127.0.0.1:9091/transmission/rpc")
	parser.add_argument("--magnet", help="optional magnet link to add via torrent-add")
	args = parser.parse_args()

	client = Client(args.url)

	print(f"-> connecting to {args.url}")
	session = client.call("session-get")
	assert session["result"] == "success", session
	sargs = session["arguments"]
	print(f"   connected. server version: {sargs.get('version')} (rpc {sargs.get('rpc-version')})")
	print(f"   session id after handshake: {client.session_id[:12]}...")

	stats = client.call("session-stats")["arguments"]
	print(f"-> session-stats: {stats.get('torrentCount')} torrents "
	      f"({stats.get('activeTorrentCount')} active)")

	if args.magnet:
		added = client.call("torrent-add", {"filename": args.magnet})
		print(f"-> torrent-add: {json.dumps(added['arguments'])}")

	fields = ["id", "hashString", "name", "status", "percentDone"]
	torrents = client.call("torrent-get", {"fields": fields})["arguments"]["torrents"]
	print(f"-> torrent-get: {len(torrents)} torrent(s)")
	for t in torrents:
		print(f"   [{t['id']}] {t['name']}  status={t['status']}  {t['percentDone'] * 100:.1f}%")

	# Confirm an unimplemented method is recognised (not an unknown-method error).
	unimpl = client.call("torrent-set", {"ids": [1]})
	print(f"-> torrent-set (expected recognised-but-unimplemented): {unimpl['result']!r}")

	print("OK")
	return 0


if __name__ == "__main__":
	sys.exit(main())
