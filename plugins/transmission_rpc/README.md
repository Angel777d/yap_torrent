# yap_torrent Transmission RPC

A [yap_torrent](https://github.com/Angel777d/yap_torrent) plugin that exposes a
[Transmission RPC](https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md)
compatible server, so existing Transmission remote clients (Transmission Remote GUI,
`transmission-remote`, the `transmission-rpc` Python library, mobile remotes, …) can
drive yap_torrent.

## Install

```bash
pip install -e plugins/transmission_rpc
```

The plugin is discovered automatically through the `yap_torrent.plugins` entry point.
Disable it by adding `yap_torrent_transmission_rpc` to `disabled_plugins` in `config.json`.

## Configuration

Add a `yap_torrent_transmission_rpc` block to `config.json`:

```json
{
  "yap_torrent_transmission_rpc": {
    "host": "0.0.0.0",
    "port": 9091,
    "path": "/transmission/rpc",
    "username": null,
    "password": null
  }
}
```

- `host` / `port` — where the RPC server binds (Transmission's default port is `9091`).
- `path` — RPC endpoint path (Transmission's default is `/transmission/rpc`).
- `username` / `password` — **reserved for a future HTTP Basic auth implementation.**
  They are read but not enforced yet; the endpoint is currently unauthenticated.

Point your client at `http://<host>:9091/transmission/rpc`.

## CSRF handshake

Per the spec, the server issues an `X-Transmission-Session-Id`. The first request (or one
with a stale id) receives an HTTP `409` carrying the current id in its headers; well-behaved
clients transparently retry with it. This is handled automatically for real clients.

## Supported methods

Implemented: `torrent-add`, `torrent-remove`, `torrent-start`, `torrent-start-now`,
`torrent-stop`, `torrent-verify`, `torrent-get`, `session-get`, `session-stats`,
`free-space`, `port-test`.

Every other spec method is recognised but returns an explanatory error string (it is *not*
treated as an unknown method). See `UNIMPLEMENTED` in `methods.py` for the list and the notes
on what each one needs. `torrent-add` accepts a magnet link or `.torrent` path/URL via the
`filename` field, or base64 `.torrent` content via `metainfo`.

## Tests

```bash
# only the test runner is needed; yap_torrent / angelovich.core are imported
# from source by conftest.py (no package install required)
pip install pytest pytest-aiohttp
pytest plugins/transmission_rpc

# manual smoke test against a running instance:
python plugins/transmission_rpc/scripts/manual_test.py --url http://127.0.0.1:9091/transmission/rpc
```
