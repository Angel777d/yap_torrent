/*
 * The whole UI is a Transmission RPC client. It keeps no private endpoint and no server
 * state of its own: everything it shows comes from torrent-get / session-get, and
 * everything it does goes out as a spec method. That is the point of it living beside the
 * RPC — a change made here works for any Transmission remote too, and vice versa.
 */
"use strict";

const RPC_PATH = document.querySelector('meta[name="rpc-path"]').content;
const CSRF_HEADER = "X-Transmission-Session-Id";
const REFRESH_MS = 2000;

// Fields the list needs. Asking for only these keeps the per-poll payload small on a
// session with many torrents; the detail panel asks for the rest of what it shows.
const LIST_FIELDS = [
	"id", "hashString", "name", "status", "percentDone", "percentComplete", "totalSize",
	"sizeWhenDone", "rateDownload", "rateUpload", "peersConnected", "eta", "error",
	"errorString", "queuePosition", "isFinished", "isStalled", "metadataPercentComplete",
];

const DETAIL_FIELDS = LIST_FIELDS.concat([
	"downloadDir", "downloadedEver", "uploadedEver", "uploadRatio", "haveValid",
	"leftUntilDone", "pieceCount", "pieceSize", "addedDate", "doneDate", "startDate",
	"activityDate", "magnetLink", "labels", "peersSendingToUs", "peersGettingFromUs",
	"files", "fileStats", "trackerStats",
]);

// tr_stat status codes (rpc-spec 3.3)
const STATUS = {
	0: "Stopped",
	1: "Queued for verify",
	2: "Verifying",
	3: "Queued to download",
	4: "Downloading",
	5: "Queued to seed",
	6: "Seeding",
};

const PRIORITIES = [[1, "High"], [0, "Normal"], [-1, "Low"]];

const state = {
	sessionId: null,
	torrents: [],
	// ids, not hashes: torrent-get returns both, but every method takes ids and the
	// integer is what queue moves compare against
	selected: new Set(),
	detail: null,
	collapsed: {files: false, trackers: true},
	polling: null,
	// while a modal is up, polling keeps running but must not redraw what is being edited
	modal: null,
	settings: null,
};

// --- transport -------------------------------------------------------------

/**
 * One RPC call. The 409 CSRF handshake is transparent: a call made without a valid
 * session id comes back as 409 carrying the current one, and is retried once with it.
 */
async function rpc(method, args = {}, retry = true) {
	const headers = {"Content-Type": "application/json"};
	if (state.sessionId) headers[CSRF_HEADER] = state.sessionId;

	let response;
	try {
		response = await fetch(RPC_PATH, {
			method: "POST",
			headers,
			body: JSON.stringify({method, arguments: args}),
		});
	} catch (e) {
		throw new Error(`cannot reach the client: ${e.message}`);
	}

	if (response.status === 409) {
		const id = response.headers.get(CSRF_HEADER);
		if (!id || !retry) throw new Error("session id rejected");
		state.sessionId = id;
		return rpc(method, args, false);
	}
	if (!response.ok) throw new Error(`${method}: HTTP ${response.status}`);

	const body = await response.json();
	if (body.result !== "success") throw new Error(`${method}: ${body.result}`);
	return body.arguments || {};
}

function setError(message) {
	const box = document.getElementById("status-error");
	box.textContent = message || "";
	box.title = message || "";
}

/** Run an action, surface whatever it says, and refresh so the result is visible at once. */
async function run(fn) {
	try {
		await fn();
		setError("");
		await refresh();
	} catch (e) {
		setError(e.message);
	}
}

// --- formatting ------------------------------------------------------------

function esc(value) {
	return String(value ?? "").replace(/[&<>"']/g, c => (
		{"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]
	));
}

function formatBytes(bytes) {
	const value = Number(bytes) || 0;
	if (value <= 0) return "0 B";
	const units = ["B", "KiB", "MiB", "GiB", "TiB"];
	const index = Math.min(units.length - 1, Math.floor(Math.log(value) / Math.log(1024)));
	const scaled = value / Math.pow(1024, index);
	return `${index === 0 ? scaled : scaled.toFixed(scaled < 10 ? 2 : 1)} ${units[index]}`;
}

function formatRate(bytes) {
	return `${formatBytes(bytes)}/s`;
}

function formatPercent(fraction) {
	return `${(Math.max(0, Math.min(1, Number(fraction) || 0)) * 100).toFixed(1)}%`;
}

function formatEta(seconds) {
	// the spec's two sentinels: -1 unknown, -2 not applicable
	if (seconds === -2) return "—";
	if (seconds < 0) return "∞";
	if (seconds < 60) return `${seconds}s`;
	if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
	if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
	return `${Math.floor(seconds / 86400)}d ${Math.floor((seconds % 86400) / 3600)}h`;
}

function formatDate(seconds) {
	return seconds > 0 ? new Date(seconds * 1000).toLocaleString() : "—";
}

function formatRatio(ratio) {
	// TR_RATIO_NA = -1, TR_RATIO_INF = -2
	if (ratio === -2) return "∞";
	if (ratio < 0) return "—";
	return ratio.toFixed(2);
}

function statusText(torrent) {
	const text = STATUS[torrent.status] ?? `Status ${torrent.status}`;
	if (torrent.error) return `${text} — ${torrent.errorString || "error"}`;
	if (torrent.status === 4 && torrent.metadataPercentComplete < 1) return "Fetching metadata";
	if (torrent.isStalled) return `${text} (stalled)`;
	return text;
}

// --- torrent list ----------------------------------------------------------

function renderList() {
	const container = document.getElementById("torrents-list");

	if (!state.torrents.length) {
		container.innerHTML = '<p class="no-selection">No torrents yet — add a magnet link or a .torrent file</p>';
		return;
	}

	container.innerHTML = state.torrents.map(t => {
		const done = formatPercent(t.percentDone);
		const selected = state.selected.has(t.id) ? " selected" : "";
		const error = t.error ? " has-error" : "";
		return `<div class="torrent-item${selected}${error}" data-id="${t.id}">
			<div class="torrent-name" title="${esc(t.name)}">${esc(t.name)}</div>
			<div class="progress-bar">
				<div class="progress-fill${t.isFinished ? " complete" : ""}" style="width: ${done}"></div>
				<div class="progress-text">${done}</div>
			</div>
			<div class="torrent-meta">
				<span class="status">${esc(statusText(t))}</span>
				<span>${formatBytes(t.sizeWhenDone)}</span>
				<span>↓ ${formatRate(t.rateDownload)}</span>
				<span>↑ ${formatRate(t.rateUpload)}</span>
				<span>${t.peersConnected} peers</span>
				<span>${formatEta(t.eta)}</span>
			</div>
		</div>`;
	}).join("");
}

function renderSelectionState() {
	const count = state.selected.size;
	document.getElementById("selection-count").textContent = count ? `${count} selected` : "";
	// every toolbar action needs at least one torrent; nothing there is meaningful otherwise
	document.querySelectorAll(".toolbar button[data-action]").forEach(button => {
		button.disabled = count === 0;
	});
}

// --- detail panel ----------------------------------------------------------

function isBeingEdited(element) {
	return element.tagName === "SELECT"
		|| (element.tagName === "INPUT" && element.type !== "checkbox");
}

function renderDetail() {
	const panel = document.getElementById("torrent-info");

	if (state.selected.size !== 1 || !state.detail) {
		panel.innerHTML = state.selected.size > 1
			? `<p class="no-selection">${state.selected.size} torrents selected</p>`
			: '<p class="no-selection">Select a torrent to view details</p>';
		return;
	}

	// A half-typed label or an open priority <select> would be thrown away by a redraw, so
	// hold off while one of those has focus. A checkbox is deliberately not covered: its
	// click is already finished, and skipping for it would freeze the panel's numbers for
	// as long as it kept focus.
	if (panel.contains(document.activeElement) && isBeingEdited(document.activeElement)) return;

	const t = state.detail;
	const scroll = panel.scrollTop;

	panel.innerHTML = `
		<div class="info-section">
			<h2 title="${esc(t.name)}">${esc(t.name)}</h2>
			<div class="progress-bar">
				<div class="progress-fill${t.isFinished ? " complete" : ""}" style="width: ${formatPercent(t.percentDone)}"></div>
				<div class="progress-text">${formatPercent(t.percentDone)}</div>
			</div>
			<div class="info-grid">
				<span>Status</span><span>${esc(statusText(t))}</span>
				<span>Queue position</span><span>${t.queuePosition}</span>
				<span>Have</span><span>${formatBytes(t.haveValid)} of ${formatBytes(t.sizeWhenDone)} (${formatBytes(t.leftUntilDone)} left)</span>
				<span>Speed</span><span>↓ ${formatRate(t.rateDownload)} &nbsp; ↑ ${formatRate(t.rateUpload)}</span>
				<span>Peers</span><span>${t.peersConnected} connected, ${t.peersSendingToUs} sending, ${t.peersGettingFromUs} receiving</span>
				<span>Transferred</span><span>↓ ${formatBytes(t.downloadedEver)} &nbsp; ↑ ${formatBytes(t.uploadedEver)} &nbsp; ratio ${formatRatio(t.uploadRatio)}</span>
				<span>Pieces</span><span>${t.pieceCount} × ${formatBytes(t.pieceSize)}</span>
				<span>Added</span><span>${formatDate(t.addedDate)}</span>
				<span>Completed</span><span>${formatDate(t.doneDate)}</span>
				<span>Last activity</span><span>${formatDate(t.activityDate)}</span>
				<span>Location</span><span class="mono">${esc(t.downloadDir)}</span>
				<span>Hash</span><span class="mono">${esc(t.hashString)}</span>
			</div>
		</div>

		<div class="info-section">
			<h3>Labels</h3>
			<div class="labels-row">
				<label class="sr-only" for="labels-input">Comma-separated labels</label>
				<input type="text" id="labels-input" placeholder="comma, separated"
				       value="${esc((t.labels || []).join(", "))}">
				<button data-action="save-labels">Save</button>
			</div>
		</div>

		${renderFiles(t)}
		${renderTrackers(t)}
	`;

	panel.scrollTop = scroll;
}

function renderFiles(t) {
	const files = t.files || [];
	const stats = t.fileStats || [];
	if (!files.length) {
		return '<div class="info-section"><h3>Files</h3><p class="muted">No metadata yet</p></div>';
	}

	const rows = files.map((file, index) => {
		const stat = stats[index] || {wanted: true, priority: 0, bytesCompleted: 0};
		const percent = file.length ? formatPercent(stat.bytesCompleted / file.length) : "—";
		const options = PRIORITIES.map(([value, label]) =>
			`<option value="${value}"${Number(stat.priority) === value ? " selected" : ""}>${label}</option>`
		).join("");
		return `<tr>
			<td><input type="checkbox" data-file-wanted="${index}"${stat.wanted ? " checked" : ""}></td>
			<td class="file-name" title="${esc(file.name)}">${esc(file.name)}</td>
			<td class="numeric">${formatBytes(file.length)}</td>
			<td class="numeric">${percent}</td>
			<td><select data-file-priority="${index}">${options}</select></td>
		</tr>`;
	}).join("");

	return `<div class="info-section">
		<h3 class="collapsible-header" data-action="toggle-files">
			<span class="collapse-icon">${state.collapsed.files ? "▶" : "▼"}</span> Files (${files.length})
		</h3>
		<div class="table-scroll${state.collapsed.files ? " collapsed" : ""}">
			<table class="files-table">
				<thead><tr><th></th><th>Name</th><th class="numeric">Size</th><th class="numeric">Done</th><th>Priority</th></tr></thead>
				<tbody>${rows}</tbody>
			</table>
		</div>
	</div>`;
}

function renderTrackers(t) {
	const trackers = t.trackerStats || [];
	if (!trackers.length) {
		return '<div class="info-section"><h3>Trackers</h3><p class="muted">None</p></div>';
	}

	const rows = trackers.map(tracker => `<tr>
		<td class="file-name" title="${esc(tracker.announce)}">${esc(tracker.announce)}</td>
		<td>${tracker.lastAnnounceSucceeded ? "OK" : "Failed"}</td>
		<td>${esc(tracker.lastAnnounceResult || "—")}</td>
		<td>${formatDate(tracker.nextAnnounceTime)}</td>
	</tr>`).join("");

	return `<div class="info-section">
		<h3 class="collapsible-header" data-action="toggle-trackers">
			<span class="collapse-icon">${state.collapsed.trackers ? "▶" : "▼"}</span> Trackers (${trackers.length})
		</h3>
		<div class="table-scroll${state.collapsed.trackers ? " collapsed" : ""}">
			<table class="files-table">
				<thead><tr><th>Announce</th><th>State</th><th>Result</th><th>Next</th></tr></thead>
				<tbody>${rows}</tbody>
			</table>
		</div>
	</div>`;
}

// --- polling ---------------------------------------------------------------

async function refresh() {
	const {torrents} = await rpc("torrent-get", {fields: LIST_FIELDS});
	// queuePosition is the order core actually serves them in, so show that rather than
	// whatever order the collection happens to iterate in
	torrents.sort((a, b) => a.queuePosition - b.queuePosition);
	state.torrents = torrents;

	// drop selections for torrents that are gone (removed here or by another client)
	const live = new Set(torrents.map(t => t.id));
	for (const id of [...state.selected]) if (!live.has(id)) state.selected.delete(id);

	if (state.selected.size === 1) {
		const detail = await rpc("torrent-get", {ids: [...state.selected], fields: DETAIL_FIELDS});
		state.detail = detail.torrents[0] || null;
	} else {
		state.detail = null;
	}

	const stats = await rpc("session-stats");
	document.getElementById("session-rates").textContent =
		`↓ ${formatRate(stats.downloadSpeed)} ↑ ${formatRate(stats.uploadSpeed)}`;
	document.getElementById("status-summary").textContent =
		`${stats.torrentCount} torrents — ${stats.activeTorrentCount} active, ${stats.pausedTorrentCount} paused` +
		` — session ↓ ${formatBytes(stats["current-stats"].downloadedBytes)}` +
		` ↑ ${formatBytes(stats["current-stats"].uploadedBytes)}`;

	renderList();
	renderSelectionState();
	renderDetail();
}

async function poll() {
	try {
		await refresh();
		setError("");
	} catch (e) {
		setError(e.message);
	}
}

// --- actions ---------------------------------------------------------------

function selectedIds() {
	return [...state.selected];
}

/** Run an action and refresh, but only if something is selected. */
function withSelection(fn) {
	return () => {
		if (!state.selected.size) return;
		return run(fn);
	};
}

async function addByUrl() {
	const input = document.getElementById("add-input");
	const value = input.value.trim();
	if (!value) {
		setError("Enter a magnet link or a torrent URL first");
		return;
	}
	// torrent-add's "filename" takes a magnet, an http(s) URL, or a path the *server* can
	// read — fetching it is its job, not ours
	await run(async () => {
		const result = await rpc("torrent-add", {filename: value});
		input.value = "";
		if (result["torrent-duplicate"]) setError(`Already added: ${result["torrent-duplicate"].name}`);
	});
}

async function addFiles(files) {
	await run(async () => {
		for (const file of files) {
			const metainfo = await readAsBase64(file);
			const result = await rpc("torrent-add", {metainfo});
			if (result["torrent-duplicate"]) setError(`Already added: ${result["torrent-duplicate"].name}`);
		}
	});
}

function readAsBase64(file) {
	return new Promise((resolve, reject) => {
		const reader = new FileReader();
		// the data: URL's payload after the comma is exactly the base64 torrent-add wants
		reader.onload = () => resolve(String(reader.result).split(",", 2)[1]);
		reader.onerror = () => reject(new Error(`cannot read ${file.name}`));
		reader.readAsDataURL(file);
	});
}

function applyFileSelection(index, wanted) {
	return withSelection(() => rpc("torrent-set", {
		ids: selectedIds(),
		[wanted ? "files-wanted" : "files-unwanted"]: [index],
	}))();
}

function applyFilePriority(index, priority) {
	const key = {"1": "priority-high", "0": "priority-normal", "-1": "priority-low"}[String(priority)];
	if (!key) return;
	return withSelection(() => rpc("torrent-set", {ids: selectedIds(), [key]: [index]}))();
}

function saveLabels() {
	const raw = document.getElementById("labels-input").value;
	const labels = raw.split(",").map(s => s.trim()).filter(Boolean);
	return withSelection(() => rpc("torrent-set", {ids: selectedIds(), labels}))();
}

function openRemoveModal() {
	if (!state.selected.size) return;
	const names = state.torrents.filter(t => state.selected.has(t.id)).map(t => t.name);
	document.getElementById("remove-modal-text").textContent = names.length === 1
		? `Remove "${names[0]}"?`
		: `Remove ${names.length} torrents?`;
	document.getElementById("remove-delete-data").checked = false;
	openModal("remove-modal");
}

async function confirmRemove() {
	const deleteData = document.getElementById("remove-delete-data").checked;
	const ids = selectedIds();
	closeModal();
	if (!ids.length) return;
	await run(() => rpc("torrent-remove", {ids, "delete-local-data": deleteData}));
}

// --- session settings ------------------------------------------------------

// What the panel offers, in order. Each is one session-get/session-set key; "depends"
// greys a number out while its own on/off switch is off, the way the limits pair up.
const SETTINGS_FIELDS = [
	{key: "download-dir", label: "Download folder", type: "text"},
	{key: "incomplete-dir-enabled", label: "Use an incomplete folder", type: "bool"},
	{key: "incomplete-dir", label: "Incomplete folder", type: "text", depends: "incomplete-dir-enabled"},
	{key: "start-added-torrents", label: "Start torrents when added", type: "bool"},
	{key: "peer-port", label: "Peer port", type: "number"},
	{key: "dht-enabled", label: "DHT enabled", type: "bool"},
	{key: "speed-limit-down-enabled", label: "Limit download speed", type: "bool"},
	{key: "speed-limit-down", label: "Download limit (kB/s)", type: "number", depends: "speed-limit-down-enabled"},
	{key: "speed-limit-up-enabled", label: "Limit upload speed", type: "bool"},
	{key: "speed-limit-up", label: "Upload limit (kB/s)", type: "number", depends: "speed-limit-up-enabled"},
	{key: "alt-speed-enabled", label: "Alternative speed limits", type: "bool"},
	{key: "alt-speed-down", label: "Alternative download (kB/s)", type: "number", depends: "alt-speed-enabled"},
	{key: "alt-speed-up", label: "Alternative upload (kB/s)", type: "number", depends: "alt-speed-enabled"},
	{key: "seedRatioLimited", label: "Stop seeding at a ratio", type: "bool"},
	{key: "seedRatioLimit", label: "Seed ratio", type: "number", step: "0.1", depends: "seedRatioLimited"},
	{key: "peer-limit-global", label: "Global peer limit", type: "number"},
	{key: "peer-limit-per-torrent", label: "Peer limit per torrent", type: "number"},
	{key: "download-queue-enabled", label: "Limit active downloads", type: "bool"},
	{key: "download-queue-size", label: "Active downloads", type: "number", depends: "download-queue-enabled"},
	{key: "seed-queue-enabled", label: "Limit active seeds", type: "bool"},
	{key: "seed-queue-size", label: "Active seeds", type: "number", depends: "seed-queue-enabled"},
];

async function openSettings() {
	try {
		state.settings = await rpc("session-get");
	} catch (e) {
		setError(e.message);
		return;
	}

	document.getElementById("settings-grid").innerHTML = SETTINGS_FIELDS.map(field => {
		const value = state.settings[field.key];
		const input = field.type === "bool"
			? `<input type="checkbox" id="set-${field.key}" data-setting="${field.key}"${value ? " checked" : ""}>`
			: `<input type="${field.type}" id="set-${field.key}" data-setting="${field.key}"`
			+ `${field.step ? ` step="${field.step}"` : ""} value="${esc(value)}">`;
		return `<label for="set-${field.key}">${esc(field.label)}</label><div>${input}</div>`;
	}).join("");

	// Three different fates, and a client that showed them all as "saved" would be lying:
	// most take effect on the next tick, the two start-up-only ones reach the config file
	// but not the running client, and the limits are stored and reported by nothing that
	// acts on them. session-set answers "success" either way, so this has to be said here.
	document.getElementById("settings-note").textContent =
		"Peer port and DHT are read once at start-up: they are saved, but the running client"
		+ " keeps its current values until it is restarted. Speed, queue, ratio and peer limits"
		+ " are stored and reported back, but nothing enforces them yet.";

	updateSettingsDependencies();
	openModal("settings-modal");
}

function updateSettingsDependencies() {
	for (const field of SETTINGS_FIELDS) {
		if (!field.depends) continue;
		const master = document.querySelector(`[data-setting="${field.depends}"]`);
		const dependant = document.querySelector(`[data-setting="${field.key}"]`);
		if (master && dependant) dependant.disabled = !master.checked;
	}
}

async function saveSettings() {
	const args = {};
	for (const field of SETTINGS_FIELDS) {
		const input = document.querySelector(`[data-setting="${field.key}"]`);
		if (!input) continue;

		let value;
		if (field.type === "bool") value = input.checked;
		else if (field.type === "number") value = Number(input.value);
		else value = input.value;

		// only send what moved: applying a setting announces it and may write config, so a
		// full block would re-announce every property on every save
		if (value !== state.settings[field.key]) args[field.key] = value;
	}

	closeModal();
	if (!Object.keys(args).length) return;
	await run(() => rpc("session-set", args));
}

// --- modals ----------------------------------------------------------------

function openModal(id) {
	state.modal = id;
	document.getElementById(id).hidden = false;
}

function closeModal() {
	if (!state.modal) return;
	document.getElementById(state.modal).hidden = true;
	state.modal = null;
}

// --- wiring ----------------------------------------------------------------

const ACTIONS = {
	"add-url": addByUrl,
	"pick-file": () => document.getElementById("add-file").click(),
	"open-settings": openSettings,
	"settings-cancel": closeModal,
	"settings-save": saveSettings,
	"remove": openRemoveModal,
	"remove-cancel": closeModal,
	"remove-confirm": confirmRemove,
	"save-labels": saveLabels,
	"toggle-files": () => {
		state.collapsed.files = !state.collapsed.files;
		renderDetail();
	},
	"toggle-trackers": () => {
		state.collapsed.trackers = !state.collapsed.trackers;
		renderDetail();
	},
};

// every method that takes nothing but ids goes the same way
for (const method of ["torrent-start", "torrent-stop", "torrent-verify", "torrent-reannounce",
	"queue-move-top", "queue-move-up", "queue-move-down", "queue-move-bottom"]) {
	ACTIONS[method] = withSelection(() => rpc(method, {ids: selectedIds()}));
}

function onTorrentClick(event) {
	const item = event.target.closest(".torrent-item");
	if (!item) return;
	const id = Number(item.dataset.id);

	if (event.ctrlKey || event.metaKey) {
		if (state.selected.has(id)) state.selected.delete(id);
		else state.selected.add(id);
	} else {
		state.selected.clear();
		state.selected.add(id);
	}

	// paint the new selection at once, then let the poll fill the detail panel in
	renderList();
	renderSelectionState();
	poll();
}

document.addEventListener("click", event => {
	const trigger = event.target.closest("[data-action]");
	if (trigger) {
		const action = ACTIONS[trigger.dataset.action];
		if (action) {
			event.preventDefault();
			action();
		}
		return;
	}
	// clicking the dimmed area outside a modal dismisses it
	if (event.target.classList.contains("modal-overlay")) closeModal();
});

document.addEventListener("change", event => {
	const target = event.target;
	if (target.dataset.fileWanted !== undefined) {
		applyFileSelection(Number(target.dataset.fileWanted), target.checked);
	} else if (target.dataset.filePriority !== undefined) {
		applyFilePriority(Number(target.dataset.filePriority), Number(target.value));
	} else if (target.dataset.setting !== undefined) {
		updateSettingsDependencies();
	} else if (target.id === "add-file") {
		const files = [...target.files];
		target.value = "";  // so picking the same file twice still fires a change
		addFiles(files);
	}
});

document.addEventListener("keydown", event => {
	if (event.key === "Escape") closeModal();
	if (event.key !== "Enter") return;
	if (event.target.id === "add-input") addByUrl();
	if (event.target.id === "labels-input") saveLabels();
});

document.getElementById("torrents-list").addEventListener("click", onTorrentClick);

poll();
state.polling = setInterval(poll, REFRESH_MS);
