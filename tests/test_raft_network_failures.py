"""Tests for Raft under network failures and node crashes."""

from typing import Any, Dict, List
import pytest

from dslabs.algorithms.raft import LogEntry, Raft, RaftState
from tests.test_raft_algorithm import (
    FakeScheduler,
    FakeTransport,
    _trigger_election_timeout,
    raft_cluster,
)


@pytest.fixture
def raft_cluster_5() -> Dict[str, Any]:
    """Construct a 5-node Raft cluster for testing."""
    peers = ["n1", "n2", "n3", "n4", "n5"]
    rafts = {}
    transports = {}
    schedulers = {}
    applied = {}

    for node_id in peers:
        transport = FakeTransport()
        scheduler = FakeScheduler()
        node_applied: List[tuple[int, Any]] = []

        def make_apply(node_applied_list: List[tuple[int, Any]]) -> Any:
            def apply(command: Any, index: int) -> None:
                node_applied_list.append((index, command))

            return apply

        raft = Raft(
            node_id=node_id,
            peers=peers,
            transport=transport,
            scheduler=scheduler,
            apply=make_apply(node_applied),
        )

        rafts[node_id] = raft
        transports[node_id] = transport
        schedulers[node_id] = scheduler
        applied[node_id] = node_applied

    return {
        "rafts": rafts,
        "transports": transports,
        "schedulers": schedulers,
        "applied": applied,
    }


def test_leader_crash_and_recovery(raft_cluster: Dict[str, Any]) -> None:
    """Test that cluster can recover when leader crashes."""
    raft: Raft = raft_cluster["raft"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]

    raft.start()
    _trigger_election_timeout(raft, scheduler)

    # Get votes and become leader
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

    # Simulate crash by stopping
    old_term = raft.current_term
    raft.stop()

    # Restart and become follower
    raft.start()
    assert raft.state == RaftState.FOLLOWER

    # Should see higher term from new leader
    raft.on_message(
        {
            "type": "append_entries",
            "term": old_term + 1,
            "leader_id": "n2",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": -1,
        }
    )

    assert raft.state == RaftState.FOLLOWER
    assert raft.current_term == old_term + 1
    assert raft.leader_id == "n2"


def test_message_loss_during_replication(raft_cluster: Dict[str, Any]) -> None:
    """Test that leader retries when AppendEntries response is lost."""
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
    raft.on_message(
        {
            "type": "request_vote_response",
            "from": "n3",
            "term": raft.current_term,
            "vote_granted": True,
        }
    )

    transport.clear()

    # Append command
    cmd = {"op": "set", "key": "x", "value": 1}
    raft.client_append(cmd)

    # Simulate n2 acknowledges but n3's response is lost
    raft.on_message(
        {
            "type": "append_entries_response",
            "from": "n2",
            "term": raft.current_term,
            "success": True,
            "match_index": 0,
        }
    )

    # Not committed yet (need majority)
    assert raft.commit_index == -1

    # Simulate heartbeat timeout and retry
    transport.clear()
    cb = scheduler.pop_next_callback()
    cb()

    # Check that AppendEntries sent again
    append_msgs = transport.by_type("append_entries")
    assert len(append_msgs) > 0

    # Now n3 acknowledges
    raft.on_message(
        {
            "type": "append_entries_response",
            "from": "n3",
            "term": raft.current_term,
            "success": True,
            "match_index": 0,
        }
    )

    # Now committed
    assert raft.commit_index == 0


def test_network_partition_heals(raft_cluster: Dict[str, Any]) -> None:
    """Test that node catches up after network partition heals."""
    raft: Raft = raft_cluster["raft"]
    scheduler: FakeScheduler = raft_cluster["scheduler"]
    applied: List[tuple[int, Any]] = raft_cluster["applied"]

    raft.start()

    # Simulate being partitioned - don't hear from leader for a while
    # Node should try to become candidate
    _trigger_election_timeout(raft, scheduler)

    assert raft.state == RaftState.CANDIDATE

    # Network heals - receive AppendEntries from real leader with higher term
    cmd1 = {"op": "set", "key": "x", "value": 1}
    cmd2 = {"op": "set", "key": "y", "value": 2}

    raft.on_message(
        {
            "type": "append_entries",
            "term": 5,
            "leader_id": "n2",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [{"term": 5, "command": cmd1}, {"term": 5, "command": cmd2}],
            "leader_commit": 1,
        }
    )

    # Should step down and accept log entries
    assert raft.state == RaftState.FOLLOWER
    assert len(raft.log) == 2
    assert raft.commit_index == 1
    assert len(applied) == 2


def test_follower_falls_behind_and_catches_up(raft_cluster: Dict[str, Any]) -> None:
    """Test that follower can catch up when it falls behind."""
    raft: Raft = raft_cluster["raft"]

    raft.start()

    # Receive initial entries
    raft.on_message(
        {
            "type": "append_entries",
            "term": 1,
            "leader_id": "n2",
            "prev_log_index": -1,
            "prev_log_term": 0,
            "entries": [{"term": 1, "command": {"op": "set", "key": "a", "value": 1}}],
            "leader_commit": -1,
        }
    )

    assert len(raft.log) == 1

    # Simulate being offline - miss several entries
    # Leader now has entries 0, 1, 2, 3 but we only have 0

    # Receive AppendEntries that assumes we have entry 3
    raft.on_message(
        {
            "type": "append_entries",
            "term": 1,
            "leader_id": "n2",
            "prev_log_index": 3,
            "prev_log_term": 1,
            "entries": [{"term": 1, "command": {"op": "set", "key": "e", "value": 5}}],
            "leader_commit": 3,
        }
    )

    # Should reject due to log inconsistency
    assert len(raft.log) == 1

    # Leader backs up and sends missing entries
    raft.on_message(
        {
            "type": "append_entries",
            "term": 1,
            "leader_id": "n2",
            "prev_log_index": 0,
            "prev_log_term": 1,
            "entries": [
                {"term": 1, "command": {"op": "set", "key": "b", "value": 2}},
                {"term": 1, "command": {"op": "set", "key": "c", "value": 3}},
                {"term": 1, "command": {"op": "set", "key": "d", "value": 4}},
            ],
            "leader_commit": 3,
        }
    )

    # Should now have caught up
    assert len(raft.log) == 4
    assert raft.commit_index == 3


def test_conflicting_logs_resolved(raft_cluster: Dict[str, Any]) -> None:
    """Test that conflicting logs from old term are overwritten."""
    raft: Raft = raft_cluster["raft"]

    raft.start()
    raft.current_term = 2

    # Add some entries from old term (simulating old leader)
    raft.log.append(LogEntry(term=1, command={"op": "set", "key": "x", "value": 1}))
    raft.log.append(LogEntry(term=2, command={"op": "set", "key": "y", "value": 2}))
    raft.log.append(
        LogEntry(term=2, command={"op": "set", "key": "z", "value": 999})
    )  # Conflicting

    # Receive AppendEntries from new leader with different entry at index 2
    raft.on_message(
        {
            "type": "append_entries",
            "term": 3,
            "leader_id": "n2",
            "prev_log_index": 1,
            "prev_log_term": 2,
            "entries": [
                {"term": 3, "command": {"op": "set", "key": "z", "value": 3}},
                {"term": 3, "command": {"op": "set", "key": "w", "value": 4}},
            ],
            "leader_commit": -1,
        }
    )

    # Conflicting entry should be overwritten
    assert len(raft.log) == 4
    assert raft.log[2].command == {"op": "set", "key": "z", "value": 3}
    assert raft.log[2].term == 3
    assert raft.log[3].command == {"op": "set", "key": "w", "value": 4}


def test_restart_preserves_log(raft_cluster: Dict[str, Any]) -> None:
    """Test that log is preserved across restarts (in memory)."""
    raft: Raft = raft_cluster["raft"]

    raft.start()

    # Add entries to log
    raft.log.append(LogEntry(term=1, command={"op": "set", "key": "x", "value": 1}))
    raft.log.append(LogEntry(term=1, command={"op": "set", "key": "y", "value": 2}))
    raft.commit_index = 1

    # Stop and restart
    raft.stop()
    raft.start()

    # Log should still be there (in-memory simulation)
    assert len(raft.log) == 2
    assert raft.commit_index == 1


def test_votes_not_granted_twice_same_term(raft_cluster: Dict[str, Any]) -> None:
    """Test that a node doesn't grant votes to multiple candidates in same term."""
    raft: Raft = raft_cluster["raft"]
    transport: FakeTransport = raft_cluster["transport"]

    raft.start()
    raft.current_term = 5

    # Receive vote request from n2
    raft.on_message(
        {
            "type": "request_vote",
            "term": 5,
            "candidate_id": "n2",
            "last_log_index": -1,
            "last_log_term": 0,
        }
    )

    responses = transport.by_type("request_vote_response")
    assert len(responses) == 1
    assert responses[0]["msg"]["vote_granted"] is True
    assert raft.voted_for == "n2"

    transport.clear()

    # Receive vote request from n3 in same term
    raft.on_message(
        {
            "type": "request_vote",
            "term": 5,
            "candidate_id": "n3",
            "last_log_index": -1,
            "last_log_term": 0,
        }
    )

    responses = transport.by_type("request_vote_response")
    assert len(responses) == 1
    assert responses[0]["msg"]["vote_granted"] is False
    assert raft.voted_for == "n2"  # Still voted for n2
