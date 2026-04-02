#!/usr/bin/env python3
"""
NSE Screener — Entry Point
NSE Stock Screener + Portfolio Manager with Angel One Live Feed
"""

import os
import errno

# Monkeypatch os.makedirs to be more resilient (fixes issues in some environments)
_orig_makedirs = os.makedirs
def _patched_makedirs(name, mode=0o777, exist_ok=False):
    try:
        _orig_makedirs(name, mode, exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
os.makedirs = _patched_makedirs

import time
import logging
import threading


from flask import Flask

import live_feed
from scanner import scan_cache, load_disk_cache, run_full_scan
from routes.pages import pages_bp
from routes.api import api_bp
from routes.portfolio import portfolio_bp

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("screener")

# ---------------------------------------------------------------------------
# Flask App
# ---------------------------------------------------------------------------
app = Flask(__name__)

# Register blueprints
app.register_blueprint(pages_bp)
app.register_blueprint(api_bp)
app.register_blueprint(portfolio_bp)

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
log.info("NSE Screener | NSE Stock Screener + Portfolio Manager")

# Start Angel One WebSocket for live prices
try:
    live_feed.start_websocket()
    log.info("Angel One WebSocket started")
except Exception as exc:
    log.warning("WebSocket start failed (will use REST fallback): %s", exc)

# Load cached data or start fresh scan
if not load_disk_cache():
    log.info("No valid cache. Starting first scan...")
    threading.Thread(target=run_full_scan, daemon=True).start()
else:
    log.info("Cache loaded. Background refresh in 60s...")
    cached_syms = [r["symbol"] for r in scan_cache["results"]]
    if cached_syms:
        live_feed.subscribe(cached_syms)
        log.info("Subscribed %d cached stocks to live feed", len(cached_syms))
    threading.Thread(target=lambda: (time.sleep(300), run_full_scan()), daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(debug=False, host="0.0.0.0", port=port)

