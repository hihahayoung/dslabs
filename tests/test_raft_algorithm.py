"""Unit tests describing the expected Raft algorithm behavior.

These tests exercise the public surface of ``dslabs.algorithms.raft.Raft``.
They intentionally fail with the current stub implementation, providing a
roadmap for a student to fill in the algorithm.

We use light-weight fakes for scheduling and transport to keep the tests
fully deterministic and synchronous: we can trigger timers immediately
without spinning up a real event loop in dslabs/scheduler.py. The fake
transport below records outbound Raft RPCs in-memory, letting the checks
assert against the exact payloads without running the network stack in
dslabs/network.py.
''''

"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import pytest

from dslabs.algorithms.raft import LogEntry, Raft, RaftState


@dataclass
class ScheduledCall:
    """Represents a scheduled callback and its metadata."""

    ms: int
    cb: Callable[[], None]
    timer_id: int
    active: bool = True


class FakeCancel:
    """Cancellation handle returned by ``FakeScheduler.call_later``."""

    def __init__(self, scheduler: "FakeScheduler", timer_id: int) -> None:
        """Record owning scheduler and timer identifier for cancellation."""
        self.scheduler = scheduler
        self.timer_id = timer_id

    def __call__(self) -> None:
        """Invoke cancellation on the parent scheduler."""
        self.scheduler.cancel(self.timer_id)


class FakeScheduler:
    """Deterministic scheduler used to capture timer behavior."""

    def __init__(self) -> None:
        """Initialize timer bookkeeping state."""
        self._now = 0
        self._timers: List[ScheduledCall] = []
        self._next_id = 0

    def call_later(self, ms: int, cb: Callable[[], None]) -> FakeCancel:
        """Schedule callback ``cb`` after ``ms`` milliseconds."""
        timer = ScheduledCall(ms=ms, cb=cb, timer_id=self._next_id)
        self._timers.append(timer)
        self._next_id += 1
        return FakeCancel(self, timer.timer_id)

    def now_ms(self) -> int:
        """Return the current simulated time in milliseconds."""
        return self._now

    def cancel(self, timer_id: int) -> None:
        """Mark timer ``timer_id`` inactive so it does not fire."""
        for timer in self._timers:
            if timer.timer_id == timer_id:
                timer.active = False
                return

    def advance(self, ms: int = 0) -> None:
        """Advance simulated time by ``ms`` milliseconds."""
        self._now += ms

    def has_active_timer(self) -> bool:
        """Return whether any timers are still active."""
        return any(timer.active for timer in self._timers)

    def pop_next_callback(self) -> Callable[[], None]:
        """Return and deactivate the next active scheduled callback."""
        for timer in self._timers:
            if timer.active:
                timer.active = False
                return timer.cb
        raise AssertionError("No active timers to trigger")


class FakeTransport:
    """Captures outbound Raft protocol messages."""

    def __init__(self) -> None:
        """Create storage for registered handlers and sent messages."""
        self.handlers: Dict[str, Callable[[Dict[str, Any]], None]] = {}
        self.sent_messages: List[Dict[str, Any]] = []

    def register(self, node_id: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Record message handler for ``node_id``."""
        self.handlers[node_id] = handler

    def send(self, to: str, msg: Dict[str, Any]) -> None:
        """Capture an outbound message destined for ``to``."""
        envelope = {"to": to, "msg": msg}
        self.sent_messages.append(envelope)

    def by_type(self, msg_type: str) -> List[Dict[str, Any]]:
        """Return sent messages whose payload ``type`` matches ``msg_type``."""
        return [env for env in self.sent_messages if env["msg"].get("type") == msg_type]

    def clear(self) -> None:
        """Reset the log of sent messages."""
        self.sent_messages.clear()


@pytest.fixture
def raft_cluster() -> Dict[str, Any]:
    """Construct a Raft node under test with fake transport and scheduler."""
    peers = ["n1", "n2", "n3"]
    transport = FakeTransport()
    scheduler = FakeScheduler()
    applied: List[tuple[int, Any]] = []

    def apply(command: Any, index: int) -> None:
        """Record application of ``command`` at ``index`` for inspection."""
        applied.append((index, command))

    raft = Raft(
        node_id="n1",
        peers=peers,
        transport=transport,
        scheduler=scheduler,
        apply=apply,
    )
    return {
        "raft": raft,
        "transport": transport,
        "scheduler": scheduler,
        "applied": applied,
    }


def _trigger_election_timeout(raft: Raft, scheduler: FakeScheduler) -> None:
    """Fire the pending election timeout for ``raft`` using ``scheduler``."""
    assert (
        scheduler.has_active_timer()
    ), "Raft.start() should schedule an election timeout"
    cb = scheduler.pop_next_callback()
    cb()


def test_election_timeout_moves_follower_to_candidate(
    raft_cluster: Dict[str, Any]
) -> None:
    """Follower should start election and request votes once timeout fires."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)

    assert raft.state == RaftState.CANDIDATE
    assert raft.current_term == 1
    assert raft.voted_for == raft.node_id

    request_votes = transport.by_type("request_vote")
    destinations = {env["to"] for env in request_votes}
    assert destinations == {"n2", "n3"}
    for env in request_votes:
        payload = env["msg"]
        assert payload["candidate_id"] == raft.node_id
        assert payload["term"] == raft.current_term
        assert "prev_log_index" in payload
        assert "prev_log_term" in payload


def test_candidate_becomes_leader_after_majority_votes(
    raft_cluster: Dict[str, Any]
) -> None:
    """Candidate should become leader once it collects a majority."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)

    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n2",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )
    assert raft.state == RaftState.CANDIDATE

    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n3",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )

    assert raft.state == RaftState.LEADER
    assert raft.leader_id == raft.node_id

    heartbeats = transport.by_type("append_entries")
    destinations = {env["to"] for env in heartbeats}
    assert destinations == {"n2", "n3"}
    for env in heartbeats:
        payload = env["msg"]
        assert payload["entries"] == []
        assert payload["term"] == raft.current_term
        assert payload["leader_commit"] == raft.commit_index
        assert payload["leader_id"] == raft.node_id
        assert "prev_log_index" in payload
        assert "prev_log_term" in payload

    for peer in ("n2", "n3"):
        assert raft.next_index.get(peer) == len(raft.log)
        assert raft.match_index.get(peer, -1) == raft.commit_index


def test_leader_commits_entry_after_majority_ack(raft_cluster: Dict[str, Any]) -> None:
    """Leader should commit and apply client entries after majority replication."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]
    applied: List[tuple[int, Any]] = raft_cluster["applied"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)
    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n2",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )
    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n3",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )

    transport.clear()

    command = {"op": "set", "key": "x", "value": 1}
    raft.client_append(command)

    assert raft.log
    entry = raft.log[-1]
    assert isinstance(entry, LogEntry)
    assert entry.command == command
    assert entry.term == raft.current_term

    append_msgs = transport.by_type("append_entries")
    assert append_msgs, "Leader should send AppendEntries with the new log entry"
    for env in append_msgs:
        payload = env["msg"]
        assert payload["entries"], "AppendEntries should carry the newly appended entry"
        commands = [item["command"] for item in payload["entries"]]
        assert command in commands
        assert payload["leader_commit"] == raft.commit_index

    raft.on_message(
        {
            "type": "append_entries_response",
            "from": "n2",
            "term": raft.current_term,
            "success": True,
            "match_index": len(raft.log) - 1,
        }
    )
    raft.on_message(
        {
            "type": "append_entries_response",
            "from": "n3",
            "term": raft.current_term,
            "success": True,
            "match_index": len(raft.log) - 1,
        }
    )

    assert raft.commit_index == len(raft.log) - 1
    assert applied == [(raft.commit_index, command)]
    for peer in ("n2", "n3"):
        assert raft.match_index.get(peer) == len(raft.log) - 1
        assert raft.next_index.get(peer) == len(raft.log)


def test_follower_rejects_old_term_vote_request(raft_cluster: Dict[str, Any]) -> None:
    """Follower should reject vote requests with old term."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]

    raft.start()
    raft.current_term = 5

    raft.on_message(
        {
            "type": "request_vote",
            "term": 3,
            "candidate_id": "n2",
            "last_log_index": -1,
            "last_log_term": 0,
        }
    )

    responses = transport.by_type("request_vote_response")
    assert len(responses) == 1
    assert responses[0]["msg"]["vote_granted"] is False
    assert raft.current_term == 5


def test_candidate_steps_down_on_higher_term(raft_cluster: Dict[str, Any]) -> None:
    """Candidate should step down if it sees a higher term."""
    raft: Raft = raft_cluster["raft"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)

    assert raft.state == RaftState.CANDIDATE
    assert raft.current_term == 1

    raft.on_message(
        {
            "type": "append_entries",
            "term": 5,
            "leader_id": "n2",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": -1,
        }
    )

    assert raft.state == RaftState.FOLLOWER
    assert raft.current_term == 5
    assert raft.leader_id == "n2"


def test_leader_steps_down_on_higher_term(raft_cluster: Dict[str, Any]) -> None:
    """Leader should step down if it sees a higher term."""
    raft: Raft = raft_cluster["raft"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)
    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n2",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )
    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n3",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )

    assert raft.state == RaftState.LEADER

    raft.on_message(
        {
            "type": "request_vote",
            "term": 10,
            "candidate_id": "n2",
            "last_log_index": -1,
            "last_log_term": 0,
        }
    )

    assert raft.state == RaftState.FOLLOWER
    assert raft.current_term == 10


def test_split_vote_no_leader_elected(raft_cluster: Dict[str, Any]) -> None:
    """No leader should be elected with only one vote."""
    raft: Raft = raft_cluster["raft"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)

    assert raft.state == RaftState.CANDIDATE

    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n2",
            "term": raft.current_term,
            "vote_granted": False,
        }
    )

    assert raft.state == RaftState.CANDIDATE


def test_log_inconsistency_rejected(raft_cluster: Dict[str, Any]) -> None:
    """Follower should reject AppendEntries with inconsistent log."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]

    raft.start()
    raft.log.append(LogEntry(term=1, command={"op": "set", "key": "x", "value": 1}))

    raft.on_message(
        {
            "type": "append_entries",
            "term": 2,
            "leader_id": "n2",
            "prev_log_index": 1,
            "prev_log_term": 2,
            "entries": [],
            "leader_commit": -1,
        }
    )

    responses = transport.by_type("append_entries_response")
    assert len(responses) == 1
    assert responses[0]["msg"]["success"] is False


def test_multiple_entries_committed_in_batch(raft_cluster: Dict[str, Any]) -> None:
    """Leader should commit multiple entries when replicated."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]
    applied: List[tuple[int, Any]] = raft_cluster["applied"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)
    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n2",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )
    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n3",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )

    transport.clear()

    cmd1 = {"op": "set", "key": "x", "value": 1}
    cmd2 = {"op": "set", "key": "y", "value": 2}
    cmd3 = {"op": "set", "key": "z", "value": 3}

    raft.client_append(cmd1)
    raft.client_append(cmd2)
    raft.client_append(cmd3)

    assert len(raft.log) == 3

    for i in range(3):
        raft.on_message(
            {
                "type": "append_entries_response",
                "from": "n2",
                "term": raft.current_term,
                "success": True,
                "match_index": i,
            }
        )
        raft.on_message(
            {
                "type": "append_entries_response",
                "from": "n3",
                "term": raft.current_term,
                "success": True,
                "match_index": i,
            }
        )

    assert raft.commit_index == 2
    assert len(applied) == 3
    assert applied[0] == (0, cmd1)
    assert applied[1] == (1, cmd2)
    assert applied[2] == (2, cmd3)


def test_follower_log_replication(raft_cluster: Dict[str, Any]) -> None:
    """Follower should replicate log entries from leader."""
    raft: Raft = raft_cluster["raft"]
    applied: List[tuple[int, Any]] = raft_cluster["applied"]

    raft.start()

    cmd1 = {"op": "set", "key": "x", "value": 1}
    cmd2 = {"op": "set", "key": "y", "value": 2}

    raft.on_message(
        {
            "type": "append_entries",
            "term": 1,
            "leader_id": "n2",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [{"term": 1, "command": cmd1}, {"term": 1, "command": cmd2}],
            "leader_commit": -1,
        }
    )

    assert len(raft.log) == 2
    assert raft.log[0].command == cmd1
    assert raft.log[1].command == cmd2
    assert raft.leader_id == "n2"

    raft.on_message(
        {
            "type": "append_entries",
            "term": 1,
            "leader_id": "n2",
            "prev_log_index": 1,
            "prev_log_term": 1,
            "entries": [],
            "leader_commit": 1,
        }
    )

    assert raft.commit_index == 1
    assert len(applied) == 2


def test_conflicting_log_entries_overwritten(raft_cluster: Dict[str, Any]) -> None:
    """Follower should overwrite conflicting log entries."""
    raft: Raft = raft_cluster["raft"]

    raft.start()
    raft.log.append(LogEntry(term=1, command={"op": "set", "key": "x", "value": 1}))
    raft.log.append(LogEntry(term=1, command={"op": "set", "key": "y", "value": 2}))
    raft.log.append(LogEntry(term=2, command={"op": "set", "key": "z", "value": 3}))

    cmd_new = {"op": "set", "key": "a", "value": 10}

    raft.on_message(
        {
            "type": "append_entries",
            "term": 3,
            "leader_id": "n2",
            "prev_log_index": 0,
            "prev_log_term": 1,
            "entries": [{"term": 3, "command": cmd_new}],
            "leader_commit": -1,
        }
    )

    assert len(raft.log) == 2
    assert raft.log[0].command == {"op": "set", "key": "x", "value": 1}
    assert raft.log[1].command == cmd_new
    assert raft.log[1].term == 3


def test_vote_restriction_outdated_log(raft_cluster: Dict[str, Any]) -> None:
    """Follower should reject vote if candidate's log is outdated."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]

    raft.start()
    raft.current_term = 3
    raft.log.append(LogEntry(term=2, command={"op": "set", "key": "x", "value": 1}))
    raft.log.append(LogEntry(term=3, command={"op": "set", "key": "y", "value": 2}))

    raft.on_message(
        {
            "type": "request_vote",
            "term": 4,
            "candidate_id": "n2",
            "last_log_index": 0,
            "last_log_term": 1,
        }
    )

    responses = transport.by_type("request_vote_response")
    assert len(responses) == 1
    assert responses[0]["msg"]["vote_granted"] is False


def test_client_append_fails_on_follower(raft_cluster: Dict[str, Any]) -> None:
    """Follower should reject client append requests."""
    raft: Raft = raft_cluster["raft"]

    raft.start()

    with pytest.raises(RuntimeError):
        raft.client_append({"op": "set", "key": "x", "value": 1})
