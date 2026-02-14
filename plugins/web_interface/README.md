# YAP Torrent Web Interface Plugin

A web interface plugin for YAP Torrent built with aiohttp.

## Features

- Web server using aiohttp
- Web dashboard with status and torrent information
- Selectable torrent list with click-to-select functionality
- Torrent control actions (start, stop, invalidate, remove, DHT peer discovery)
- RESTful API endpoints
- Auto-refresh functionality
- Separate HTML/CSS/JS files for easy customization

## Installation

```bash
pip install yap_torrent_web
```

## Usage

The plugin starts automatically when loaded. By default, it runs on `http://0.0.0.0:8080`.

Access the web interface at: `http://localhost:8080`

## Web Interface

### Status Section
- Displays current status: peer ID, IP, external IP
- Refresh button to update status

### Torrents Section
- Click on any torrent to select it
- Control buttons apply to the selected torrent:
  - **Start** – Start the selected torrent
  - **Stop** – Stop the selected torrent
  - **Invalidate** – Invalidate torrent data
  - **Remove** – Remove the selected torrent
  - **DHT Ask Peers** – Request peers via DHT

## API Endpoints

### GET Endpoints
- `GET /` - Web dashboard (HTML interface)
- `GET /api/status` - Get current status (JSON)
- `GET /api/torrents` - Get list of torrents (JSON)
- `GET /static/*` - Static files (CSS, JS)

### POST Endpoints
- `POST /api/torrent/action` - Execute torrent action
  - Body: `{"action": "start|stop|invalidate|remove|dht_ask_peers", "hash": "torrent_hash"}`
  - Returns: `{"success": true, "action": "...", "hash": "..."}`

## Configuration

The server host and port can be configured by modifying the `WebServer` initialization in `__init__.py`:

```python
self.server = WebServer(env, host='0.0.0.0', port=8080)
```

## File Structure

```
plugins/web_interface/
├── src/yap_torrent_web_interface/
│   ├── __init__.py          # Plugin initialization
│   ├── server.py            # Web server implementation
│   └── static/              # Static web files
│       ├── index.html       # Main HTML page
│       ├── style.css        # Styles
│       └── app.js           # JavaScript logic
├── pyproject.toml
├── README.md
└── LICENSE
```

## Dependencies

- aiohttp - HTTP server and client library
- yap_torrent - Core torrent functionality
