# Dashboard Overview

Minimal Streamlit app that polls each node's `/state` endpoint and shows the
JSON. Useful for quick visual checks during labs.

Run locally

```bash
streamlit run dslabs/dashboard/app.py
```

Tips

- Edit the peers text field to match your cluster URLs.
- Lower the refresh interval for smoother updates; increase if requests time out.
- The app uses simple polling; it will show errors if nodes are unreachable.
