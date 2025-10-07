"""Raft simulation to visualize log replication and leader election.

This simulation demonstrates how the Raft consensus algorithm works across
multiple nodes with realistic network delays and message passing.
"""

import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from dslabs.algorithms.raft import LogEntry, Raft, RaftState


@dataclass
class Message:
    """Message in transit between nodes."""

    sender: str
    receiver: str
    payload: Dict[str, Any]
    deliver_at: float


class SimulatedTransport:
    """Transport with realistic network delays."""

    def __init__(self, simulation: "RaftSimulation") -> None:
        self.simulation = simulation
        self.handlers: Dict[str, Callable[[Dict[str, Any]], None]] = {}

    def register(self, node_id: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        self.handlers[node_id] = handler

    def send(self, to: str, msg: Dict[str, Any]) -> None:
        # Add sender info to message
        sender = None
        for node_id in self.handlers:
            if self.handlers[node_id].__self__.node_id == to:  # type: ignore
                for other_id in self.handlers:
                    handler = self.handlers[other_id]
                    if hasattr(handler, "__self__"):
                        raft_node = handler.__self__  # type: ignore
                        if raft_node.transport == self:
                            for peer in raft_node.peers:
                                if peer == to:
                                    sender = raft_node.node_id
                                    break

        msg_copy = msg.copy()
        if sender:
            msg_copy["from"] = sender

        # Simulate network delay (1-5ms)
        delay = random.uniform(0.001, 0.005)

        # Simulate message loss (2% chance)
        if random.random() < 0.02:
            return  # Drop message

        deliver_at = self.simulation.current_time + delay
        self.simulation.message_queue.append(
            Message(sender="", receiver=to, payload=msg_copy, deliver_at=deliver_at)
        )


class SimulatedScheduler:
    """Scheduler for simulation."""

    def __init__(self, simulation: "RaftSimulation") -> None:
        self.simulation = simulation
        self.timers: List[Dict[str, Any]] = []
        self.next_timer_id = 0

    def call_later(self, ms: int, cb: Callable[[], None]) -> Callable[[], None]:
        timer_id = self.next_timer_id
        self.next_timer_id += 1

        timer = {
            "id": timer_id,
            "fire_at": self.simulation.current_time + ms / 1000.0,
            "callback": cb,
            "active": True,
        }
        self.timers.append(timer)

        def cancel() -> None:
            for t in self.timers:
                if t["id"] == timer_id:
                    t["active"] = False

        return cancel

    def now_ms(self) -> int:
        return int(self.simulation.current_time * 1000)

    def fire_ready_timers(self) -> None:
        """Fire all timers that are ready."""
        for timer in self.timers[:]:
            if timer["active"] and timer["fire_at"] <= self.simulation.current_time:
                timer["active"] = False
                timer["callback"]()


@dataclass
class RaftSimulation:
    """Simulates a Raft cluster with visualization."""

    num_nodes: int = 5
    current_time: float = field(default=0.0, init=False)
    nodes: Dict[str, Raft] = field(default_factory=dict, init=False)
    transports: Dict[str, SimulatedTransport] = field(default_factory=dict, init=False)
    schedulers: Dict[str, SimulatedScheduler] = field(default_factory=dict, init=False)
    message_queue: List[Message] = field(default_factory=list, init=False)
    state_machines: Dict[str, List[Any]] = field(default_factory=dict, init=False)
    crashed_nodes: Set[str] = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        """Initialize the cluster."""
        node_ids = [f"n{i}" for i in range(self.num_nodes)]

        # Create nodes
        for node_id in node_ids:
            transport = SimulatedTransport(self)
            scheduler = SimulatedScheduler(self)
            self.transports[node_id] = transport
            self.schedulers[node_id] = scheduler
            self.state_machines[node_id] = []

            def make_apply(nid: str) -> Callable[[Any, int], None]:
                def apply(command: Any, index: int) -> None:
                    self.state_machines[nid].append((index, command))

                return apply

            node = Raft(
                node_id=node_id,
                peers=node_ids,
                transport=transport,
                scheduler=scheduler,
                apply=make_apply(node_id),
            )
            self.nodes[node_id] = node
            node.start()

    def step(self, duration: float = 0.01) -> None:
        """Advance simulation by duration seconds."""
        self.current_time += duration

        # Deliver ready messages
        ready = [m for m in self.message_queue if m.deliver_at <= self.current_time]
        self.message_queue = [
            m for m in self.message_queue if m.deliver_at > self.current_time
        ]

        for msg in ready:
            if msg.receiver not in self.crashed_nodes:
                handler = self.transports[msg.receiver].handlers.get(msg.receiver)
                if handler:
                    handler(msg.payload)

        # Fire ready timers
        for node_id, scheduler in self.schedulers.items():
            if node_id not in self.crashed_nodes:
                scheduler.fire_ready_timers()

    def print_state(self) -> None:
        """Print current state of all nodes."""
        print(f"\n{'='*80}")
        print(f"Time: {self.current_time:.2f}s")
        print(f"{'='*80}")

        for node_id, node in sorted(self.nodes.items()):
            status = "CRASHED" if node_id in self.crashed_nodes else node.state.value
            leader_marker = " [LEADER]" if node.state == RaftState.LEADER else ""

            print(f"\n{node_id} - {status.upper()}{leader_marker}")
            print(f"  Term: {node.current_term}")
            print(f"  Voted for: {node.voted_for}")
            print(f"  Commit index: {node.commit_index}")
            print(f"  Log: [", end="")

            for i, entry in enumerate(node.log):
                prefix = " " if i > 0 else ""
                committed = "✓" if i <= node.commit_index else " "
                cmd_str = str(entry.command).replace("'", '"')
                if len(cmd_str) > 30:
                    cmd_str = cmd_str[:27] + "..."
                print(f"{prefix}({entry.term}:{committed}:{cmd_str})", end="")

            print("]")

            if node.state == RaftState.LEADER:
                print(f"  Next index: {node.next_index}")
                print(f"  Match index: {node.match_index}")

            print(f"  Applied: {len(self.state_machines[node_id])} entries")

    def submit_command(self, command: Any) -> bool:
        """Submit a command to the leader."""
        for node in self.nodes.values():
            if node.state == RaftState.LEADER:
                try:
                    node.client_append(command)
                    return True
                except Exception:
                    pass
        return False

    def crash_node(self, node_id: str) -> None:
        """Simulate node crash."""
        if node_id in self.nodes:
            self.crashed_nodes.add(node_id)
            self.nodes[node_id].stop()
            print(f"\n💥 Node {node_id} crashed!")

    def restart_node(self, node_id: str) -> None:
        """Restart a crashed node."""
        if node_id in self.crashed_nodes:
            self.crashed_nodes.remove(node_id)
            self.nodes[node_id].start()
            print(f"\n🔄 Node {node_id} restarted!")

    def run_scenario(self) -> None:
        """Run a demonstration scenario."""
        print("\n" + "="*80)
        print("RAFT CONSENSUS ALGORITHM SIMULATION")
        print("="*80)

        # Wait for leader election
        print("\n📊 Phase 1: Leader Election")
        for _ in range(200):
            self.step(0.002)
        self.print_state()

        # Submit some commands
        print("\n📊 Phase 2: Log Replication")
        time.sleep(1)
        commands = [
            {"op": "set", "key": "x", "value": 1},
            {"op": "set", "key": "y", "value": 2},
            {"op": "set", "key": "z", "value": 3},
        ]

        for cmd in commands:
            if self.submit_command(cmd):
                print(f"✅ Submitted: {cmd}")

        # Wait for replication
        for _ in range(300):
            self.step(0.002)
        self.print_state()

        # Crash the leader
        print("\n📊 Phase 3: Leader Failure & Re-election")
        time.sleep(1)
        leader_id = None
        for node_id, node in self.nodes.items():
            if node.state == RaftState.LEADER:
                leader_id = node_id
                break

        if leader_id:
            self.crash_node(leader_id)

        # Wait for new election
        for _ in range(300):
            self.step(0.002)
        self.print_state()

        # Submit more commands with new leader
        print("\n📊 Phase 4: Recovery & Continued Operation")
        time.sleep(1)
        new_commands = [
            {"op": "set", "key": "a", "value": 10},
            {"op": "set", "key": "b", "value": 20},
        ]

        for cmd in new_commands:
            if self.submit_command(cmd):
                print(f"✅ Submitted: {cmd}")

        for _ in range(300):
            self.step(0.002)
        self.print_state()

        # Restart crashed node
        if leader_id:
            self.restart_node(leader_id)

        # Wait for catch-up
        for _ in range(300):
            self.step(0.002)

        print("\n📊 Phase 5: Final State After Node Recovery")
        self.print_state()

        # Verify consistency
        print("\n" + "="*80)
        print("CONSISTENCY CHECK")
        print("="*80)

        logs = {}
        for node_id, node in self.nodes.items():
            if node_id not in self.crashed_nodes:
                log_str = str([e.command for e in node.log[: node.commit_index + 1]])
                logs[node_id] = log_str

        if len(set(logs.values())) == 1:
            print("✅ All nodes have consistent committed logs!")
        else:
            print("❌ Log inconsistency detected!")
            for node_id, log in logs.items():
                print(f"  {node_id}: {log}")


if __name__ == "__main__":
    sim = RaftSimulation(num_nodes=5)
    sim.run_scenario()
