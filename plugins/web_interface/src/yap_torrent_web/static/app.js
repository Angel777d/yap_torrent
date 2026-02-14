let selectedTorrentHash = null;

async function loadStatus() {
	try {
		const response = await fetch('/api/status');
		const data = await response.json();

		document.getElementById('status-running').textContent = data.status || '-';
		document.getElementById('status-peer-id').textContent = data.peer_id || '-';
		document.getElementById('status-ip').textContent = data.ip || '-';
		document.getElementById('status-external-ip').textContent = data.external_ip || '-';
	} catch (e) {
		document.getElementById('status').innerHTML = '<p class="error">Error: ' + e.message + '</p>';
	}
}

async function loadTorrents() {
	try {
		const response = await fetch('/api/torrents');
		const data = await response.json();
		const container = document.getElementById('torrents-list');

		if (Array.isArray(data) && data.length > 0) {
			container.innerHTML = data.map(t =>
				`<div class="torrent-item" data-hash="${t.hash}" onclick="selectTorrent('${t.hash}')">
					${t.name || 'Unknown'}
				</div>`
			).join('');
		} else {
			container.innerHTML = '<p>No torrents found</p>';
		}
	} catch (e) {
		document.getElementById('torrents-list').innerHTML = '<p class="error">Error: ' + e.message + '</p>';
	}
}

function selectTorrent(hash) {
	// Remove previous selection
	document.querySelectorAll('.torrent-item').forEach(item => {
		item.classList.remove('selected');
	});

	// Add selection to clicked item
	const item = document.querySelector(`[data-hash="${hash}"]`);
	if (item) {
		item.classList.add('selected');
		selectedTorrentHash = hash;
	}
}

async function torrentActionSelected(action) {
	if (!selectedTorrentHash) {
		alert('Please select a torrent first');
		return;
	}

	await torrentAction(action, selectedTorrentHash);
}

async function torrentAction(action, hash) {
	try {
		const response = await fetch('/api/torrent/action', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({ action, hash })
		});

		const result = await response.json();

		if (response.ok && result.success) {
			// Reload torrents to show updated state
			await loadTorrents();
		} else {
			alert('Error: ' + (result.error || 'Unknown error'));
		}
	} catch (e) {
		alert('Error: ' + e.message);
	}
}

async function addMagnetLink() {
	const input = document.getElementById('magnet-input');
	const magnetLink = input.value.trim();

	if (!magnetLink) {
		alert('Please enter a magnet link');
		return;
	}

	if (!magnetLink.startsWith('magnet:')) {
		alert('Invalid magnet link format. Must start with "magnet:"');
		return;
	}

	try {
		const response = await fetch('/api/magnet/add', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({ magnet: magnetLink })
		});

		const result = await response.json();

		if (response.ok && result.success) {
			input.value = ''; // Clear input
			alert('Magnet link added successfully!');
			// Reload torrents to show new torrent
			await loadTorrents();
		} else {
			alert('Error: ' + (result.error || 'Unknown error'));
		}
	} catch (e) {
		alert('Error: ' + e.message);
	}
}

// Auto-load on page load
window.onload = () => {
	loadStatus();
	loadTorrents();
};
