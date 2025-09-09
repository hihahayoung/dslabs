"""Simple Streamlit dashboard to peek at node state.

The app polls each peer's `/state` endpoint and renders the JSON, making it
useful for quick visual inspection during labs. Adjust peers and refresh
interval above to experiment with different cluster sizes/layouts.
"""

import streamlit as st
import requests, time

st.set_page_config(page_title="DS Labs Dashboard", layout="wide")
peers_text = st.text_input(
    "Peers (comma-separated id:url)",
    "n1:http://localhost:8000,n2:http://localhost:8001,n3:http://localhost:8002",
)
peers = dict(kv.split(":", 1) for kv in peers_text.split(","))
interval = st.slider("Refresh interval (sec)", 0.2, 2.0, 0.5)
while True:
    cols = st.columns(len(peers))
    for i, (nid, url) in enumerate(sorted(peers.items())):
        with cols[i]:
            st.subheader(nid)
            try:
                r = requests.get(url + "/state", timeout=0.3)
                st.json(r.json())
            except Exception as e:
                st.error(str(e))
    time.sleep(interval)
