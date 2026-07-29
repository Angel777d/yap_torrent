# Yet Another Python torrent

Pure python implementation of BitTorrent protocol.
It's aimed to be simple, easy to use and extend.

## Implemented features:
* Standard protocol implementation [BEP:3](https://bittorrent.org/beps/bep_0003.html)
* DHT protocol [BEP:5](https://bittorrent.org/beps/bep_0005.html)
* Extensions protocol [BEP:10](https://bittorrent.org/beps/bep_0010.html)
* Metadata extension, magnet URI support [BEP:9](https://bittorrent.org/beps/bep_0009.html)
* Endgame algorithm
* Rarest first algorithm
* Choke algorithm (tit-for-tat, client-wide upload budget)
* Download queue ordered by torrent priority
* Plugins system
* Headless mode
* SimpleUI (plugin)
* Web UI (plugin)
* Transmission RPC server (plugin) — drive it from existing Transmission remotes

---

### Planned features:

* move torrent validations to post-initialize
* Config editor
* Per-file download order and priorities
* Bandwidth limits and transfer-rate reporting
