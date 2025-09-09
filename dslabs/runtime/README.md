# Runtime Overview

HTTP-based runtime to run nodes as separate processes or containers.

Endpoints

- `POST /msg`: deliver a message (JSON dict) to the node.
- `GET /state`: expose `brief_state()` for dashboards and debugging.

Environment

- `NODE_ID`: node name (e.g., `n1`).
- `MY_URL`: this node's base URL (e.g., `http://n1:8000`).
- `PEERS`: comma-separated `id:url` list for the cluster.

Run locally (Docker)

```bash
docker compose -f dslabs/runtime/docker-compose.yml up --build
```

Notes

- Sending is fire-and-forget; reliability is the algorithm's job.
- Prefer the simulator for deterministic tests; use this runtime to observe behavior with real concurrency and I/O.
