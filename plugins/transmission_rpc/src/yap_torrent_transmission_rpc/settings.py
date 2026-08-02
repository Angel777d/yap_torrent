from pathlib import Path
from typing import Tuple, Dict

from yap_torrent.config import as_bool
from yap_torrent.settings import Setting


# Transmission session key -> core config key. Anything absent here is either
# plugin-owned or something core has no notion of; session-set ignores those rather than
# failing the call, which is what Transmission does with unknown args.
SESSION_SETTINGS: Dict[str, str] = {
	"download-dir": "download_folder",
	"incomplete-dir": "incomplete_folder",
	"incomplete-dir-enabled": "incomplete_folder_enabled",
	"seedRatioLimit": "seed_ratio_limit",
	"seedRatioLimited": "seed_ratio_limited",
	"download-queue-enabled": "download_queue_enabled",
	"download-queue-size": "download_queue_size",
	"seed-queue-enabled": "seed_queue_enabled",
	"seed-queue-size": "seed_queue_size",
	"peer-limit-global": "max_connections",
	"peer-limit-per-torrent": "peer_limit_per_torrent",
	"blocklist-enabled": "blocklist_enabled",
	"blocklist-url": "blocklist_url",
	"start-added-torrents": "start_added_torrents",
	"dht-enabled": "dht_enabled",
	"peer-port": "port",
}

# The config properties this plugin offers its clients, with the cast each needs to read a
# JSON value, and — where nothing acts on the value yet — why. Core holds the properties
# and knows none of this: what is worth exposing, and what a client is allowed to send for
# it, is ours. Registered at start-up by `RpcServer`.
_NOT_ENFORCED_BANDWIDTH = "bandwidth limiting is not implemented; the value is stored and reported only"
_NOT_ENFORCED_QUEUE = "there is no active-torrent queue; the value is stored and reported only"
_NOT_ENFORCED_PEERS = "connection admission is driven by the queue limits; the value is stored and reported only"
_NOT_ENFORCED_RATIO = "seeding is never stopped on ratio; the value is stored and reported only"
_NOT_ENFORCED_INCOMPLETE = "downloads are written straight to download_folder"
_NOT_ENFORCED_BLOCKLIST = "no blocklist subsystem; no peer is ever filtered"


CORE_SETTINGS: Tuple[Setting, ...] = (
	Setting("download_folder", Path),
	Setting("port", int),
	Setting("dht_enabled", as_bool),
	Setting("start_added_torrents", as_bool),

	Setting("incomplete_folder", Path, note=_NOT_ENFORCED_INCOMPLETE),
	Setting("incomplete_folder_enabled", as_bool, note=_NOT_ENFORCED_INCOMPLETE),
	Setting("speed_limit_down", int, note=_NOT_ENFORCED_BANDWIDTH),
	Setting("speed_limit_up", int, note=_NOT_ENFORCED_BANDWIDTH),
	Setting("seed_ratio_limit", float, note=_NOT_ENFORCED_RATIO),
	Setting("seed_ratio_limited", as_bool, note=_NOT_ENFORCED_RATIO),
	Setting("download_queue_enabled", as_bool, note=_NOT_ENFORCED_QUEUE),
	Setting("download_queue_size", int, note=_NOT_ENFORCED_QUEUE),
	Setting("seed_queue_enabled", as_bool, note=_NOT_ENFORCED_QUEUE),
	Setting("seed_queue_size", int, note=_NOT_ENFORCED_QUEUE),
	Setting("max_connections", int, note=_NOT_ENFORCED_PEERS),
	Setting("peer_limit_per_torrent", int, note=_NOT_ENFORCED_PEERS),
	Setting("blocklist_enabled", as_bool, note=_NOT_ENFORCED_BLOCKLIST),
	Setting("blocklist_url", str, note=_NOT_ENFORCED_BLOCKLIST),
)
