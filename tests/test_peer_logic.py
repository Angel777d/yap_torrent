"""Tests for the pure peer logic (Part C): state machine + choke selection."""
from yap_torrent.components.peer_ec import PeerState
from yap_torrent.systems.peer_logic import (
	ChokeCandidate,
	MAX_QUESTIONABLE_FAILS,
	can_connect,
	next_state_on_failure,
	retry_delay,
	select_unchoked,
	should_attempt,
)


# --- state machine ---------------------------------------------------------
def test_unknown_fails_to_questionable():
	assert next_state_on_failure(PeerState.Unknown, 0) == (PeerState.Questionable, 1)


def test_questionable_escalates_to_no_connection_after_cap():
	state, fails = PeerState.Questionable, 1
	# keep failing; after MAX_QUESTIONABLE_FAILS it drops to NoConnection
	for _ in range(MAX_QUESTIONABLE_FAILS):
		state, fails = next_state_on_failure(state, fails)
	assert state == PeerState.NoConnection


def test_no_connection_is_unlimited():
	state, fails = next_state_on_failure(PeerState.NoConnection, 99)
	assert state == PeerState.NoConnection and fails == 100


def test_good_demotes_to_questionable():
	assert next_state_on_failure(PeerState.Good, 0) == (PeerState.Questionable, 1)


def test_retry_delays():
	assert retry_delay(PeerState.Unknown) == 0.0
	assert retry_delay(PeerState.Questionable) == 60.0
	assert retry_delay(PeerState.NoConnection) == 600.0


def test_suspicious_never_connects():
	assert can_connect(PeerState.Suspicious) is False
	assert should_attempt(PeerState.Suspicious, last_attempt=0, now=10_000) is False


def test_should_attempt_respects_cooldown():
	# questionable: not yet (30s < 60s), then yes (90s >= 60s)
	assert should_attempt(PeerState.Questionable, last_attempt=0, now=30) is False
	assert should_attempt(PeerState.Questionable, last_attempt=0, now=90) is True
	# unknown connects immediately
	assert should_attempt(PeerState.Unknown, last_attempt=0, now=0) is True


# --- choke selection -------------------------------------------------------
def _c(key, interested=True, reciprocated=True, rate=0.0):
	return ChokeCandidate(key=key, interested=interested, reciprocated=reciprocated, rate=rate)


def test_under_limit_keeps_everyone():
	peers = [_c("a"), _c("b")]
	assert select_unchoked(peers, limit=5, seeding=False) == {"a", "b"}


def test_not_interested_choked_first():
	peers = [_c("keep", interested=True, rate=1), _c("drop", interested=False, rate=100)]
	assert select_unchoked(peers, limit=1, seeding=False) == {"keep"}


def test_non_reciprocators_choked_before_reciprocators():
	peers = [
		_c("recip", interested=True, reciprocated=True, rate=1),
		_c("leech", interested=True, reciprocated=False, rate=100),
	]
	# not seeding: reciprocator kept despite lower rate
	assert select_unchoked(peers, limit=1, seeding=False) == {"recip"}
	# seeding: reciprocation ignored -> higher rate kept
	assert select_unchoked(peers, limit=1, seeding=True) == {"leech"}


def test_pure_downloaders_ordered_by_rate_smallest_choked_first():
	peers = [
		_c("fast", interested=True, reciprocated=False, rate=100),
		_c("slow", interested=True, reciprocated=False, rate=1),
	]
	# not seeding, neither reciprocates -> keep the faster one, choke the slowest
	assert select_unchoked(peers, limit=1, seeding=False) == {"fast"}


def test_limit_zero():
	assert select_unchoked([_c("a")], limit=0, seeding=False) == set()
