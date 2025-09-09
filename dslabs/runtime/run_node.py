"""Entry point for running a node as a separate process (or in Docker).

This file wires a `RaftNode` to the HTTP transport and an asyncio-backed
timer adapter, then serves FastAPI endpoints for messages and state.

Environment variables (see docker-compose.yml):
- NODE_ID: this node's ID (e.g., n1)
- MY_URL: base URL for this node (e.g., http://n1:8000)
- PEERS: comma-separated `id:url` entries for the cluster
"""

import os, uvicorn, asyncio
from typing import Dict
from dslabs.raft.node import RaftNode
from dslabs.raft.storage import MemStore
from dslabs.runtime.http_transport import HttpTransport


def parse_peers(env: str) -> Dict[str, str]:
    """Parse `PEERS` env var (e.g., "n1:http://n1:8000,n2:http://n2:8000")."""
    peers = {}
    for kv in env.split(","):
        nid, url = kv.split(":", 1)
        peers[nid] = url
    return peers


def main():
    """Boot a node, attach transport/timers, and run the HTTP server."""
    my_id = os.environ.get("NODE_ID", "n1")
    my_url = os.environ.get("MY_URL", f"http://{my_id}:8000")
    peers_env = os.environ.get(
        "PEERS", "n1:http://n1:8000,n2:http://n2:8000,n3:http://n3:8000"
    )
    peers = parse_peers(peers_env)
    store = MemStore()
    node = RaftNode(
        my_id, list(peers.keys()), transport=None, timer_api=None, storage=store
    )
    transport = HttpTransport(my_id, my_url, peers, node.on_message)
    node.transport = transport

    class LoopTimer:
        """Tiny adapter exposing `now_ms` and `call_later` atop asyncio loop."""

        def now_ms(self):
            return int(asyncio.get_event_loop().time() * 1000)

        def call_later(self, ms, cb):
            handle = asyncio.get_event_loop().call_later(ms / 1000.0, cb)

            def cancel():
                handle.cancel()

            return cancel

    node.timers = LoopTimer()
    app = transport.app
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


if __name__ == "__main__":
    main()
