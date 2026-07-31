"""Tests for the pure peer logic (Part C): state machine + choke selection."""
from yap_torrent.components.peer_ec import PeerState
from yap_torrent.systems.logic.peer import (
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
# Field meanings are from the PEER's side: took = bytes it took from us, gave = bytes it
# gave us.
def _c(key, interested=True, took=0, gave=0):
	return ChokeCandidate(key=key, interested=interested, took=took, gave=gave)


def test_under_limit_keeps_everyone():
	peers = [_c("a"), _c("b")]
	assert select_unchoked(peers, limit=5, seeding=False) == {"a", "b"}


def test_limit_zero():
	assert select_unchoked([_c("a")], limit=0, seeding=False) == set()


def test_not_interested_choked_first():
	# interest outranks every balance term: serving a peer that wants nothing is wasted
	peers = [_c("keep", interested=True), _c("drop", interested=False, gave=10_000)]
	assert select_unchoked(peers, limit=1, seeding=False) == {"keep"}
	assert select_unchoked(peers, limit=1, seeding=True) == {"keep"}


def test_leeching_prefers_the_better_net_balance():
	# tit-for-tat: rank on what a peer gave us minus what it took
	peers = [
		_c("generous", gave=10_000, took=1_000),  # net +9000
		_c("freeloader", gave=0, took=10_000),  # net -10000
	]
	assert select_unchoked(peers, limit=1, seeding=False) == {"generous"}


def test_leeching_counts_the_balance_not_the_raw_total():
	# a peer that gave us a lot but took even more ranks below a modest net contributor
	peers = [
		_c("big_but_even", gave=100_000, took=100_000),  # net 0
		_c("small_surplus", gave=5, took=0),  # net +5
	]
	assert select_unchoked(peers, limit=1, seeding=False) == {"small_surplus"}


def test_seeding_shares_out_by_who_has_taken_least():
	# complete: nobody can reciprocate, so rank on least served to spread upload around
	peers = [
		_c("already_fed", took=10_000),
		_c("barely_fed", took=1),
	]
	assert select_unchoked(peers, limit=1, seeding=True) == {"barely_fed"}


def test_seeding_ignores_what_a_peer_gave_us():
	# the leeching balance would keep "gave_us_lots"; seeding must not care
	peers = [
		_c("gave_us_lots", gave=10_000, took=10_000),
		_c("gave_nothing", gave=0, took=0),
	]
	assert select_unchoked(peers, limit=1, seeding=False) == {"gave_us_lots"}
	assert select_unchoked(peers, limit=1, seeding=True) == {"gave_nothing"}


def test_ties_break_deterministically_on_key():
	# identical peers must not depend on iteration order
	peers = [_c("a"), _c("b"), _c("c")]
	first = select_unchoked(peers, limit=2, seeding=False)
	assert first == select_unchoked(list(reversed(peers)), limit=2, seeding=False)
	assert len(first) == 2
