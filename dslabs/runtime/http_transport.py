"""Minimal HTTP transport for multi-process runs.

This module exposes two endpoints via FastAPI:
- POST /msg: deliver a message dict to the node's handler.
- GET /state: fetch the node's brief state for dashboards/inspection.

It is intentionally simple: fire-and-forget sends and in-memory state.
Use the simulator for determinism; use this runtime to see behavior across
separate processes or containers.
"""

from fastapi import FastAPI
import asyncio, aiohttp
from typing import Dict, Any, Callable


class HttpTransport:
    """HTTP-based transport used by `run_node`.

    Parameters:
    - my_id/my_url: this node's identity and base URL.
    - peers: mapping of node_id -> base URL for other nodes.
    - handler: callable to invoke for inbound messages.
    """

    def __init__(
        self,
        my_id: str,
        my_url: str,
        peers: Dict[str, str],
        handler: Callable[[Dict[str, Any]], None],
    ):
        self.app = FastAPI()
        self.handler = handler
        self.my_id = my_id
        self.my_url = my_url
        self.peers = peers
        self.client = aiohttp.ClientSession()

        @self.app.post("/msg")
        async def rx(msg: Dict[str, Any]):
            """Receive a message and hand it to the node."""
            self.handler(msg)
            return {"ok": True}

        @self.app.get("/state")
        async def state():
            st = handler_owner_state(handler)
            return st

    def send(self, to: str, msg: Dict[str, Any]) -> None:
        """Asynchronously POST a message to the peer's /msg endpoint."""
        asyncio.create_task(self.client.post(self.peers[to] + "/msg", json=msg))


def handler_owner_state(handler):
    """Introspect the bound method's owner to surface `brief_state()`.

    This lets dashboards call /state without coupling to node internals.
    """
    owner = getattr(handler, "__self__", None)
    if owner and hasattr(owner, "brief_state"):
        return owner.brief_state()
    return {"error": "no_state"}
