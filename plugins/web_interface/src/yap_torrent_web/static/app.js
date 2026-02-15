let selectedTorrentHash = null;

// Status popup functions
function showStatusPopup() {
    loadStatus();
    document.getElementById('status-popup').style.display = 'flex';
}

function hideStatusPopup() {
    document.getElementById('status-popup').style.display = 'none';
}

async function loadStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();

        document.getElementById('status-running').textContent = data.status || '-';
        document.getElementById('status-peer-id').textContent = data.peer_id || '-';
        document.getElementById('status-ip').textContent = data.ip || '-';
        document.getElementById('status-external-ip').textContent = data.external_ip || '-';
    } catch (e) {
        console.error('Error loading status:', e);
    }
}

async function loadTorrents() {
    try {
        const response = await fetch('/api/torrents');
        const data = await response.json();
        const container = document.getElementById('torrents-list');

        if (Array.isArray(data) && data.length > 0) {
            container.innerHTML = data.map(t => {
                const progress = (t.complete * 100).toFixed(2);
                return `<div class="torrent-item" data-hash="${t.hash}" onclick="selectTorrent('${t.hash}')">
					<div class="torrent-name">${t.name || 'Unknown'}</div>
					<div class="progress-bar">
						<div class="progress-fill" style="width: ${progress}%"></div>
						<div class="progress-text">${progress}%</div>
					</div>
				</div>`;
            }).join('');
        } else {
            container.innerHTML = '<p>No torrents found</p>';
        }
    } catch (e) {
        document.getElementById('torrents-list').innerHTML = '<p class="error">Error: ' + e.message + '</p>';
    }
}

async function selectTorrent(hash) {
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

    // Load torrent info
    await loadTorrentInfo(hash);
}

async function loadTorrentInfo(hash) {
    try {
        const response = await fetch(`/api/torrent/${hash}`);
        if (!response.ok) {
            throw new Error('Torrent not found');
        }

        const torrent = await response.json();
        displayTorrentInfo(torrent);
        updateButtonStates(torrent.isStarted);

        // Show controls
        document.getElementById('torrent-controls').style.display = 'block';
    } catch (e) {
        document.getElementById('torrent-info').innerHTML = '<p class="error">Error: ' + e.message + '</p>';
        document.getElementById('torrent-controls').style.display = 'none';
    }
}

function updateButtonStates(isStarted) {
    const startButtons = document.querySelectorAll('[onclick*="start"]');
    const stopButtons = document.querySelectorAll('[onclick*="stop"]');

    startButtons.forEach(btn => {
        if (btn.textContent.trim() === 'Start') {
            btn.disabled = isStarted;
        }
    });

    stopButtons.forEach(btn => {
        if (btn.textContent.trim() === 'Stop') {
            btn.disabled = !isStarted;
        }
    });
}

function displayTorrentInfo(torrent) {
    const progress = (torrent.complete * 100).toFixed(2);

    let filesHtml = '';
    if (torrent.files && torrent.files.length > 0) {
        filesHtml = `
        <div class="info-section">
            <h3 class="collapsible-header" onclick="toggleFilesSection()">
                <span class="collapse-icon">▶</span> Files (${torrent.files.length})
            </h3>
            <ul class="files-list collapsed" id="files-list">
                ${torrent.files.map(file => `<li>${file}</li>`).join('')}
            </ul>
        </div>
        `;
    }

    const html = `
        <div class="info-section">
            <h2>${torrent.name}</h2>
        </div>

        <div class="info-section">
            <div class="info-item">
                <strong>Hash:</strong> <span class="hash-text">${torrent.hash}</span>
            </div>
            <div class="info-item">
                <strong>Download Path:</strong>
                <span class="path-text">${torrent.downloadPath || 'N/A'}</span>
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

        ${filesHtml}
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

function toggleFilesSection() {
    const filesList = document.getElementById('files-list');
    const icon = document.querySelector('.collapse-icon');

    if (filesList.classList.contains('collapsed')) {
        filesList.classList.remove('collapsed');
        icon.textContent = '▼';
    } else {
        filesList.classList.add('collapsed');
        icon.textContent = '▶';
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
            body: JSON.stringify({action, hash})
        });

        const result = await response.json();

        if (response.ok && result.success) {
            // Reload torrents and torrent info
            await loadTorrents();
            if (selectedTorrentHash) {
                await loadTorrentInfo(selectedTorrentHash);
            }
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
            body: JSON.stringify({magnet: magnetLink})
        });

        if (response.ok) {
            const result = await response.json();
            if (result.success) {
                input.value = ''; // Clear input
                alert('Magnet link added successfully!');
                // Reload torrents to show new torrent
                await loadTorrents();
            }
        } else {
            alert('Error: ' + (response.statusText || 'Unknown error'));
        }
    } catch (e) {
        alert('Error: ' + e.message);
    }
}

// Auto-load on page load
window.onload = () => {
    loadTorrents();
};
