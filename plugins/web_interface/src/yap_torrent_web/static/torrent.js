let torrentHash = null;

async function loadTorrentInfo() {
	const pathParts = window.location.pathname.split('/');
	torrentHash = pathParts[pathParts.length - 1];

	if (!torrentHash) {
		document.getElementById('torrent-info').innerHTML = '<p class="error">No torrent hash provided</p>';
		return;
	}

	try {
		const response = await fetch(`/api/torrent/${torrentHash}`);

		if (!response.ok) {
			throw new Error('Torrent not found');
		}

		const data = await response.json();
		displayTorrentInfo(data);
	} catch (e) {
		document.getElementById('torrent-info').innerHTML = '<p class="error">Error: ' + e.message + '</p>';
	}
}

function displayTorrentInfo(torrent) {
	const progress = (torrent.complete * 100).toFixed(2);

	const html = `
		<div class="info-section">
			<h2>${torrent.name}</h2>
		</div>

		<div class="info-section">
			<div class="info-item">
				<strong>Hash:</strong> <span class="hash-text">${torrent.hash}</span>
			</div>
		</div>

		<div class="info-section">
			<h3>Progress</h3>
			<div class="progress-bar">
				<div class="progress-fill" style="width: ${progress}%"></div>
				<div class="progress-text">${progress}%</div>
			</div>
		</div>

		<div class="info-section">
			<h3>Statistics</h3>
			<div class="info-item">
				<strong>Downloaded:</strong> ${formatBytes(torrent.downloaded)}
			</div>
			<div class="info-item">
				<strong>Uploaded:</strong> ${formatBytes(torrent.uploaded)}
			</div>
		</div>
	`;

	document.getElementById('torrent-info').innerHTML = html;
}

function formatBytes(bytes) {
	if (bytes === 0) return '0 Bytes';
	const k = 1024;
	const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
	const i = Math.floor(Math.log(bytes) / Math.log(k));
	return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

async function torrentAction(action) {
	if (!torrentHash) {
		alert('No torrent hash available');
		return;
	}

	try {
		const response = await fetch('/api/torrent/action', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({ action, hash: torrentHash })
		});

		const result = await response.json();

		if (response.ok && result.success) {
			alert(`Action "${action}" executed successfully`);
			// Reload torrent info to show updated state
			await loadTorrentInfo();
		} else {
			alert('Error: ' + (result.error || 'Unknown error'));
		}
	} catch (e) {
		alert('Error: ' + e.message);
	}
}

// Auto-load on page load
window.onload = () => {
	loadTorrentInfo();
	// Auto-refresh every 5 seconds
	setInterval(loadTorrentInfo, 5000);
};
