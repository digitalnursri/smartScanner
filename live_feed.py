"""
Angel One SmartAPI WebSocket Live Feed
Real-time tick data for NSE Screener
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from datetime import datetime, date, timedelta

import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

log = logging.getLogger("live_feed")

# ---------------------------------------------------------------------------
# Config from .env
# ---------------------------------------------------------------------------
ENV_FILE = Path(__file__).parent / ".env"

def _load_env():
    """Load .env file into os.environ."""
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

_load_env()

API_KEY = os.environ.get("ANGEL_API_KEY", "")
SECRET_KEY = os.environ.get("ANGEL_SECRET_KEY", "")
CLIENT_ID = os.environ.get("ANGEL_CLIENT_ID", "")
MPIN = os.environ.get("ANGEL_MPIN", "")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET", "")

TOKEN_FILE = Path(__file__).parent / "cache" / "angel_tokens.json"

# ---------------------------------------------------------------------------
# Token mapping
# ---------------------------------------------------------------------------

_token_map = {}   # symbol -> token string
_reverse_map = {} # token string -> symbol


def load_token_map():
    global _token_map, _reverse_map
    if TOKEN_FILE.exists():
        _token_map = json.loads(TOKEN_FILE.read_text())
        _reverse_map = {v: k for k, v in _token_map.items()}
        log.info("Loaded %d symbol tokens", len(_token_map))
    else:
        log.warning("Token file not found: %s", TOKEN_FILE)
        _refresh_token_map()


def _refresh_token_map():
    """Download fresh instrument list from Angel One."""
    global _token_map, _reverse_map
    try:
        import requests
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        data = requests.get(url, timeout=30).json()
        nse_eq = [d for d in data if d.get("exch_seg") == "NSE" and d.get("symbol", "").endswith("-EQ")]
        _token_map = {d["symbol"].replace("-EQ", ""): d["token"] for d in nse_eq}
        _reverse_map = {v: k for k, v in _token_map.items()}
        TOKEN_FILE.parent.mkdir(exist_ok=True)
        TOKEN_FILE.write_text(json.dumps(_token_map))
        log.info("Refreshed %d symbol tokens", len(_token_map))
    except Exception as exc:
        log.error("Failed to refresh token map: %s", exc)


def get_token(symbol: str) -> str | None:
    return _token_map.get(symbol.upper().replace(".NS", ""))


def get_symbol(token: str) -> str | None:
    return _reverse_map.get(token)


# ---------------------------------------------------------------------------
# SmartAPI Session
# ---------------------------------------------------------------------------

_smart_api = None
_auth_token = None
_feed_token = None
_session_lock = threading.Lock()
_last_login = 0


def _login():
    global _smart_api, _auth_token, _feed_token, _last_login

    if not all([API_KEY, CLIENT_ID, MPIN, TOTP_SECRET]):
        log.error("Angel One credentials not configured in .env")
        return False

    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        obj = SmartConnect(api_key=API_KEY)
        data = obj.generateSession(CLIENT_ID, MPIN, totp)

        if not data.get("status"):
            log.error("Login failed: %s", data.get("message"))
            return False

        _smart_api = obj
        _auth_token = data["data"]["jwtToken"]
        _feed_token = obj.getfeedToken()
        _last_login = time.time()
        log.info("Angel One login successful")
        return True
    except Exception as exc:
        log.error("Login error: %s", exc)
        return False


def ensure_session():
    """Ensure we have a valid session, re-login if needed."""
    with _session_lock:
        # Re-login every 6 hours (tokens expire in ~24h)
        if _smart_api is None or (time.time() - _last_login) > 6 * 3600:
            return _login()
        return True


# ---------------------------------------------------------------------------
# Live Price Store
# ---------------------------------------------------------------------------

_live_prices = {}  # symbol -> {ltp, open, high, low, close, change, change_pct, volume, last_update}
_prices_lock = threading.Lock()
_subscribers = set()  # set of symbols being tracked
_ws_thread = None
_ws_running = False


def get_live_prices(symbols: list[str] | None = None) -> dict:
    """Get current live prices. If symbols is None, return all."""
    with _prices_lock:
        if symbols:
            return {s: _live_prices[s].copy() for s in symbols if s in _live_prices}
        return {s: d.copy() for s, d in _live_prices.items()}


def get_live_price(symbol: str) -> dict | None:
    with _prices_lock:
        data = _live_prices.get(symbol)
        return data.copy() if data else None


# ---------------------------------------------------------------------------
# WebSocket Feed
# ---------------------------------------------------------------------------

_sws = None


def _on_data(wsapp, message, *args):
    """Handle incoming tick data."""
    try:
        if not isinstance(message, dict):
            return

        token = str(message.get("token", ""))
        symbol = get_symbol(token)
        if not symbol:
            return

        ltp = message.get("last_traded_price", 0) / 100  # Angel sends price * 100
        close_price = message.get("closed_price", 0) / 100
        open_price = message.get("open_price_of_the_day", 0) / 100
        high_price = message.get("high_price_of_the_day", 0) / 100
        low_price = message.get("low_price_of_the_day", 0) / 100
        volume = message.get("volume_trade_for_the_day", 0)

        change = ltp - close_price if close_price > 0 else 0
        change_pct = round((change / close_price) * 100, 2) if close_price > 0 else 0

        with _prices_lock:
            _live_prices[symbol] = {
                "ltp": round(ltp, 2),
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "change": round(change, 2),
                "change_pct": change_pct,
                "volume": volume,
                "last_update": datetime.now().strftime("%H:%M:%S"),
            }
    except Exception as exc:
        log.debug("Tick parse error: %s", exc)


def _on_open(wsapp, *args):
    log.info("WebSocket connected")
    _subscribe_current()


def _on_error(wsapp, error, *args):
    log.warning("WebSocket error: %s", error)


def _on_close(wsapp, *args):
    log.info("WebSocket closed")
    global _ws_running
    _ws_running = False


def _subscribe_current():
    """Subscribe to all current symbols."""
    global _sws
    if not _sws or not _subscribers:
        return

    tokens = []
    for sym in _subscribers:
        t = get_token(sym)
        if t:
            tokens.append({"exchangeType": 1, "tokens": [t]})  # 1 = NSE

    if not tokens:
        return

    # Angel One allows subscribing in batches
    # Mode: 1=LTP, 2=Quote, 3=Snap Quote
    try:
        # Subscribe in chunks of 50
        for i in range(0, len(tokens), 50):
            batch = tokens[i:i+50]
            token_list = [t["tokens"][0] for t in batch]
            _sws.subscribe("abc123", 2, [{"exchangeType": 1, "tokens": token_list}])
        log.info("Subscribed to %d symbols", len(tokens))
    except Exception as exc:
        log.error("Subscribe error: %s", exc)


def subscribe(symbols: list[str]):
    """Add symbols to the subscription list."""
    global _subscribers
    new_syms = set()
    for s in symbols:
        clean = s.upper().replace(".NS", "")
        if clean not in _subscribers and get_token(clean):
            _subscribers.add(clean)
            new_syms.add(clean)

    # If WebSocket is running, subscribe to new symbols
    if _ws_running and new_syms and _sws:
        tokens = [get_token(s) for s in new_syms if get_token(s)]
        if tokens:
            try:
                _sws.subscribe("abc123", 2, [{"exchangeType": 1, "tokens": tokens}])
                log.info("Subscribed %d new symbols", len(tokens))
            except Exception as exc:
                log.error("Subscribe error: %s", exc)


def start_websocket():
    """Start the WebSocket connection in a background thread."""
    global _ws_thread, _ws_running, _sws

    if _ws_running:
        return

    if not ensure_session():
        log.error("Cannot start WebSocket: login failed")
        return

    load_token_map()

    def _run():
        global _sws, _ws_running
        _ws_running = True

        while _ws_running:
            try:
                _sws = SmartWebSocketV2(
                    _auth_token, API_KEY, CLIENT_ID, _feed_token,
                    max_retry_attempt=5
                )
                _sws.on_data = _on_data
                _sws.on_open = _on_open
                _sws.on_error = _on_error
                _sws.on_close = _on_close

                log.info("Starting WebSocket connection...")
                _sws.connect()
            except Exception as exc:
                log.error("WebSocket crashed: %s — retrying in 10s", exc)
                time.sleep(10)

            if _ws_running:
                log.info("WebSocket reconnecting in 5s...")
                time.sleep(5)

    _ws_thread = threading.Thread(target=_run, daemon=True)
    _ws_thread.start()
    log.info("WebSocket thread started")


def stop_websocket():
    global _ws_running, _sws
    _ws_running = False
    if _sws:
        try:
            _sws.close_connection()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Bulk LTP fallback (REST API, for when WebSocket hasn't caught up)
# ---------------------------------------------------------------------------

def fetch_ltp_bulk(symbols: list[str]) -> dict:
    """Fetch LTP via REST API for symbols not yet in WebSocket cache."""
    if not ensure_session():
        return {}

    results = {}
    # Angel One supports batch LTP
    for sym in symbols:
        token = get_token(sym)
        if not token:
            continue
        try:
            data = _smart_api.ltpData("NSE", f"{sym}-EQ", token)
            if data.get("status") and data.get("data"):
                d = data["data"]
                ltp = float(d.get("ltp", 0))
                close_price = float(d.get("close", 0))
                change = ltp - close_price if close_price else 0
                change_pct = round((change / close_price) * 100, 2) if close_price else 0
                results[sym] = {
                    "ltp": ltp,
                    "open": float(d.get("open", 0)),
                    "high": float(d.get("high", 0)),
                    "low": float(d.get("low", 0)),
                    "close": close_price,
                    "change": round(change, 2),
                    "change_pct": change_pct,
                    "last_update": datetime.now().strftime("%H:%M:%S"),
                }
        except Exception as exc:
            log.debug("LTP fetch failed for %s: %s", sym, exc)

    return results


def is_market_open() -> bool:
    """Check if NSE market is currently open."""
    now = datetime.now()
    if now.weekday() >= 5:  # Saturday/Sunday
        return False
    h, m = now.hour, now.minute
    mins = h * 60 + m
    return 555 <= mins <= 930  # 9:15 AM to 3:30 PM


# ---------------------------------------------------------------------------
# Historical Candle Data (replaces jugaad_data)
# ---------------------------------------------------------------------------

_hist_lock = threading.Lock()
_hist_last_call = 0
HIST_RATE_LIMIT = 0.5  # seconds between API calls


def fetch_historical(symbol: str, days: int = 365) -> "pd.DataFrame | None":
    """
    Fetch historical OHLCV candle data from Angel One.
    Returns a DataFrame with columns: DATE, OPEN, HIGH, LOW, CLOSE, VOLUME
    Compatible with the old jugaad_data format.
    """
    import pandas as pd
    global _hist_last_call

    if not ensure_session():
        return None

    clean = symbol.upper().replace(".NS", "")
    token = get_token(clean)
    if not token:
        log.debug("No token for %s", clean)
        return None

    # Rate limiting — wait if calling too fast
    with _hist_lock:
        now = time.time()
        elapsed = now - _hist_last_call
        if elapsed < HIST_RATE_LIMIT:
            time.sleep(HIST_RATE_LIMIT - elapsed)
        _hist_last_call = time.time()

    try:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)

        params = {
            "exchange": "NSE",
            "symboltoken": token,
            "interval": "ONE_DAY",
            "fromdate": start_dt.strftime("%Y-%m-%d 09:15"),
            "todate": end_dt.strftime("%Y-%m-%d 15:30"),
        }

        result = _smart_api.getCandleData(params)

        # Retry once on rate limit
        if result and result.get("errorcode") == "AB1019":
            time.sleep(1.5)
            with _hist_lock:
                _hist_last_call = time.time()
            result = _smart_api.getCandleData(params)

        if not result or not result.get("status") or not result.get("data"):
            msg = result.get("message", "None") if result else "No response"
            ec = result.get("errorcode", "") if result else ""
            if ec:
                log.warning("Candle API error for %s: %s (%s)", clean, msg, ec)
            else:
                log.debug("No candle data for %s: %s", clean, msg)
            return None

        candles = result["data"]
        # Format: [timestamp, open, high, low, close, volume]
        rows = []
        for c in candles:
            rows.append({
                "DATE": pd.Timestamp(c[0]),
                "OPEN": float(c[1]),
                "HIGH": float(c[2]),
                "LOW": float(c[3]),
                "CLOSE": float(c[4]),
                "VOLUME": int(c[5]),
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return None

        return df

    except Exception as exc:
        log.debug("Historical fetch failed for %s: %s", clean, exc)
        return None
