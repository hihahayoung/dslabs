# Raft Consensus Algorithm Implementation Summary

## Overview

This implementation provides a complete, working version of the Raft consensus algorithm as described in the paper "Raft: In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout.

## Implementation Location

- **Main Algorithm**: [dslabs/algorithms/raft/raft.py](dslabs/algorithms/raft/raft.py)
- **Unit Tests**: [tests/test_raft_algorithm.py](tests/test_raft_algorithm.py)
- **Network Failure Tests**: [tests/test_raft_network_failures.py](tests/test_raft_network_failures.py)
- **Simulation**: [tests/raft_simulation.py](tests/raft_simulation.py)

## Core Features Implemented

### 1. Leader Election (Section 5.2 of Raft paper)
- **Follower State**: Nodes start as followers and respond to RPCs from leaders and candidates
- **Candidate State**: Followers transition to candidates when election timeout expires
- **Leader State**: Candidates become leaders upon receiving majority votes
- **Randomized Election Timeouts**: Prevents split votes (150-300ms range)
- **Term Management**: Proper handling of term numbers for maintaining consistency

### 2. Log Replication (Section 5.3)
- **AppendEntries RPC**: Leader sends log entries to followers
- **Log Consistency Check**: Ensures logs match using `prev_log_index` and `prev_log_term`
- **Conflict Resolution**: Overwrites conflicting entries in follower logs
- **Heartbeats**: Periodic empty AppendEntries to maintain authority (50ms interval)

### 3. Safety (Section 5.4)
- **Election Restriction**: Only candidates with up-to-date logs can win elections
- **Commit Rules**: Only entries from current term can be committed once replicated on majority
- **State Machine Application**: Committed entries are applied to state machine in order

### 4. Network Failure Handling
- **Message Loss**: Handles dropped messages through retries via heartbeat mechanism
- **Node Crashes**: Nodes can stop and restart, rejoining the cluster
- **Leader Failure**: Cluster elects new leader when current leader fails
- **Network Partitions**: Nodes catch up when partitions heal
- **Log Divergence**: Properly resolves conflicting logs after failures

## Key Components

### State Variables (Per Paper's Figure 2)

**Persistent state** (maintained across restarts in this in-memory implementation):
- `current_term`: Latest term the server has seen
- `voted_for`: Candidate that received vote in current term
- `log`: Log entries containing commands and terms

**Volatile state on all servers**:
- `commit_index`: Highest log index known to be committed
- `last_applied`: Highest log index applied to state machine
- `state`: Current role (FOLLOWER, CANDIDATE, or LEADER)

**Volatile state on leaders**:
- `next_index`: For each peer, index of next log entry to send
- `match_index`: For each peer, highest log index known to be replicated

### Core Methods

1. **`start()`**: Initialize node as follower and start election timer
2. **`stop()`**: Clean up timers and reset state
3. **`on_message()`**: Dispatch incoming RPCs (RequestVote, AppendEntries)
4. **`client_append()`**: Accept client commands (leader only)
5. **`_start_election()`**: Transition to candidate and request votes
6. **`_handle_request_vote()`**: Process vote requests from candidates
7. **`_handle_append_entries()`**: Process log replication from leader
8. **`_become_leader()`**: Transition to leader after winning election
9. **`_step_down()`**: Revert to follower upon seeing higher term
10. **`_update_commit_index()`**: Advance commit index when majority replicates
11. **`_apply_committed_entries()`**: Apply committed entries to state machine

### Timer Management

- **Election Timer**: Randomized (150-300ms) to prevent split votes
- **Heartbeat Timer**: Fixed interval (50ms) for leader to maintain authority
- **Proper Cancellation**: Timers cancelled on state transitions

## Testing

### Unit Tests (20 total, all passing)

**Basic Algorithm Tests** (`test_raft_algorithm.py`):
1. `test_election_timeout_moves_follower_to_candidate` - Election initiation
2. `test_candidate_becomes_leader_after_majority_votes` - Leader election
3. `test_leader_commits_entry_after_majority_ack` - Log replication and commit
4. `test_follower_rejects_old_term_vote_request` - Term comparison
5. `test_candidate_steps_down_on_higher_term` - Candidate demotion
6. `test_leader_steps_down_on_higher_term` - Leader demotion
7. `test_split_vote_no_leader_elected` - Split vote handling
8. `test_log_inconsistency_rejected` - Log consistency check
9. `test_multiple_entries_committed_in_batch` - Batch commit
10. `test_follower_log_replication` - Follower log updates
11. `test_conflicting_log_entries_overwritten` - Conflict resolution
12. `test_vote_restriction_outdated_log` - Election safety
13. `test_client_append_fails_on_follower` - Leader-only writes

**Network Failure Tests** (`test_raft_network_failures.py`):
14. `test_leader_crash_and_recovery` - Leader restart
15. `test_message_loss_during_replication` - Dropped message handling
16. `test_network_partition_heals` - Partition recovery
17. `test_follower_falls_behind_and_catches_up` - Catch-up mechanism
18. `test_conflicting_logs_resolved` - Log conflict resolution
19. `test_restart_preserves_log` - Log persistence across restarts
20. `test_votes_not_granted_twice_same_term` - Vote uniqueness

### Simulation (`raft_simulation.py`)

A visual simulation demonstrating:
- Leader election with 5 nodes
- Log replication across cluster
- Leader failure and re-election
- Continued operation with new leader
- Node crash and recovery
- Consistency verification

**Run with**: `.venv/bin/python tests/raft_simulation.py`

## Running Tests

```bash
# Run all Raft tests
.venv/bin/python -m pytest tests/test_raft*.py -v

# Run specific test file
.venv/bin/python -m pytest tests/test_raft_algorithm.py -v
.venv/bin/python -m pytest tests/test_raft_network_failures.py -v

# Run simulation
.venv/bin/python tests/raft_simulation.py
```

## Implementation Notes

### Design Decisions

1. **Randomized Election Timeouts**: Range of 150-300ms provides good balance for test environments
2. **Heartbeat Interval**: 50ms (1/3 of minimum election timeout) ensures timely leader detection
3. **Message Format**: JSON-compatible dictionaries for easy serialization
4. **Error Handling**: Non-leaders raise `RuntimeError` on `client_append()`
5. **Vote Tracking**: Uses set to track received votes during election

### Simplifications (for learning purposes)

1. **In-Memory Only**: No disk persistence (could be added in production)
2. **No Log Compaction**: Logs grow unbounded (Section 7 of paper)
3. **No Membership Changes**: Cluster size is fixed (Section 6 of paper)
4. **No Client Retry**: Clients must handle leader redirects
5. **Synchronous State Machine**: `apply()` callback is synchronous

### Potential Extensions

1. **Persistent Storage**: Add disk persistence for state and log
2. **Log Compaction**: Implement snapshots to bound memory usage
3. **Dynamic Membership**: Add/remove nodes dynamically
4. **Read Optimization**: Implement read-only queries without log entries
5. **Batching**: Batch multiple client commands for efficiency
6. **Pipeline**: Pipeline AppendEntries for higher throughput

## References

- [Raft Paper](https://raft.github.io/raft.pdf) - Original paper by Ongaro & Ousterhout
- [Raft Website](https://raft.github.io/) - Visualizations and resources
- [The Raft Consensus Algorithm](https://raft.github.io/) - Interactive visualization

## Verification

All tests pass successfully:
```
============================== 20 passed ==============================
```

The implementation correctly handles:
- ✅ Leader election with randomized timeouts
- ✅ Log replication with consistency checks
- ✅ Safety properties (election restriction, commit rules)
- ✅ Network failures (message loss, node crashes, partitions)
- ✅ Log conflict resolution
- ✅ State machine application
- ✅ Term management and stepping down

## Conclusion

This implementation provides a complete, tested, and well-documented version of the Raft consensus algorithm suitable for educational purposes. It demonstrates all core concepts from the paper and handles various failure scenarios correctly.
