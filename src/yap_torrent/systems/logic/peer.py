"""Pure, network-free peer logic — unit-tested in isolation.

Two concerns live here so systems stay thin and the tricky rules are testable:
- the connection-attempt state machine (`PeerState` transitions + retry schedule);
- the choke selection (who to keep unchoked when over the upload limit).
"""
from dataclasses import dataclass
from typing import Hashable, Iterable, Set, Tuple

from yap_torrent.components.peer_ec import PeerState

# retry schedule (seconds) and the questionable attempt cap
QUESTIONABLE_RETRY = 60.0
NO_CONNECTION_RETRY = 600.0
MAX_QUESTIONABLE_FAILS = 5


def can_connect(state: PeerState) -> bool:
	"""Never connect to Suspicious peers."""
	return state != PeerState.Suspicious


def retry_delay(state: PeerState) -> float:
	"""Minimum seconds between connection attempts for a given state."""
	if state == PeerState.Questionable:
		return QUESTIONABLE_RETRY
	if state == PeerState.NoConnection:
		return NO_CONNECTION_RETRY
	return 0.0  # Unknown / Good -> connect immediately


def should_attempt(state: PeerState, last_attempt: float, now: float) -> bool:
	"""Whether we may (re)connect to this peer now."""
	if not can_connect(state):
		return False
	return (now - last_attempt) >= retry_delay(state)


def next_state_on_failure(state: PeerState, fail_count: int) -> Tuple[PeerState, int]:
	"""Advance the state machine after a failed connection attempt.

	Unknown -> Questionable; Questionable escalates and after MAX_QUESTIONABLE_FAILS
	becomes NoConnection; NoConnection stays (unlimited retries); Good demotes to
	Questionable. Returns (new_state, new_fail_count).
	"""
	if state == PeerState.Unknown:
		return PeerState.Questionable, 1
	if state == PeerState.Good:
		return PeerState.Questionable, 1
	if state == PeerState.Questionable:
		fail_count += 1
		if fail_count >= MAX_QUESTIONABLE_FAILS:
			return PeerState.NoConnection, fail_count
		return PeerState.Questionable, fail_count
	if state == PeerState.NoConnection:
		return PeerState.NoConnection, fail_count + 1
	return state, fail_count


@dataclass(frozen=True)
class ChokeCandidate:
	key: Hashable  # peer identity (orderable for deterministic ties)
	interested: bool  # the peer is interested in our pieces
	took: int  # download from us
	gave: int  # uploaded to us


def select_unchoked(candidates: Iterable[ChokeCandidate], limit: int, seeding: bool) -> Set[Hashable]:
	"""Return the peers to keep UNCHOKED (at most ``limit``); everyone else is choked.	"""
	if limit <= 0:
		return set()

	ordered = sorted(candidates,
	                 key=lambda c: ((c.interested, -c.took, c.key) if seeding
	                                else (c.interested, c.gave - c.took, c.key)),
	                 reverse=True)
	return {c.key for c in ordered[:limit]}
