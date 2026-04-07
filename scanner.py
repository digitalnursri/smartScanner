"""
Batch scanner — Phase 1 (jugaad_data) + Phase 2 (Angel One fallback).
"""

import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from stocks import STOCK_UNIVERSE
from config import MAX_WORKERS, BATCH_SIZE, BATCH_DELAY, DATA_LOOKBACK_DAYS
from analyzer import fetch_and_analyze, get_nifty50_benchmark, apply_sector_strength, generate_ai_summary
import live_feed
import db

log = logging.getLogger("screener")

# ---------------------------------------------------------------------------
# Scan state (shared across modules)
# ---------------------------------------------------------------------------
scan_cache = {
    "results": [],
    "last_scan": None,
    "scanning": False,
    "progress": 0,
    "total": len(STOCK_UNIVERSE),
    "errors": [],
    "nifty50_1m": 0,
    "market_regime": "unknown",
    "heatmap": [],
    "summary": "",
}
progress_lock = threading.Lock()


# ===================================================================
#  CACHE (SQLite backed)
# ===================================================================

def load_disk_cache() -> bool:
    """Load results from SQLite DB (fallback to old JSON cache)."""
    import json
    from pathlib import Path
    from config import CACHE_FILE, CACHE_TTL_HOURS, TOP_N_RESULTS

    db.init_db()
    count = db.get_result_count()
    if count > 0:
        last_scan = db.get_meta("last_scan", "From cache")
        timestamp = db.get_meta("timestamp")
        if timestamp:
            try:
                cached_time = datetime.fromisoformat(timestamp)
                if cached_time.tzinfo is None:
                    cached_time = cached_time.replace(tzinfo=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - cached_time).total_seconds() / 3600
                if age_hours > CACHE_TTL_HOURS:
                    return False
            except (ValueError, TypeError):
                pass

        scan_cache["results"] = db.load_results(TOP_N_RESULTS)
        scan_cache["last_scan"] = last_scan
        scan_cache["market_regime"] = db.get_meta("market_regime", "unknown")
        scan_cache["heatmap"] = db.get_meta("heatmap", [])
        scan_cache["summary"] = db.get_meta("summary", "")
        scan_cache["nifty50_1m"] = db.get_meta("nifty50_1m", 0)
        log.info("Loaded %d results from SQLite DB", len(scan_cache["results"]))
        return True

    # Fallback: old JSON cache
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text())
            cached_time = datetime.fromisoformat(data.get("timestamp", "2000-01-01"))
            if cached_time.tzinfo is None:
                cached_time = cached_time.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - cached_time).total_seconds() / 3600
            if age_hours > CACHE_TTL_HOURS:
                return False
            scan_cache["results"] = data["results"]
            scan_cache["last_scan"] = data.get("last_scan", "From cache")
            scan_cache["market_regime"] = data.get("market_regime", "unknown")
            scan_cache["heatmap"] = data.get("heatmap", [])
            scan_cache["summary"] = data.get("summary", "")
            scan_cache["nifty50_1m"] = data.get("nifty50_1m", 0)
            save_disk_cache()
            log.info("Migrated %d results from JSON to SQLite", len(data["results"]))
            return True
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            log.warning("JSON cache load failed: %s", exc)
    return False


def save_disk_cache():
    """Save scan results to SQLite DB."""
    try:
        db.init_db()
        db.save_results(scan_cache["results"], {
            "last_scan": scan_cache["last_scan"],
            "market_regime": scan_cache["market_regime"],
            "heatmap": scan_cache["heatmap"],
            "summary": scan_cache["summary"],
            "nifty50_1m": scan_cache["nifty50_1m"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        log.warning("DB save failed: %s", exc)


# ===================================================================
#  FULL SCAN
# ===================================================================

def run_full_scan():
    with progress_lock:
        scan_cache["scanning"] = True
        scan_cache["progress"] = 0
        scan_cache["errors"] = []

    results = []
    log.info("v4 HC Scan: %d stocks...", len(STOCK_UNIVERSE))
    start_time = time.time()

    nifty_1m, regime = get_nifty50_benchmark()
    scan_cache["nifty50_1m"] = nifty_1m
    scan_cache["market_regime"] = regime
    log.info("Nifty 1M: %+.2f%% | Regime: %s", nifty_1m, regime.upper())

    # ── PHASE 1: jugaad_data (NSE) — fast bulk scan in batches ──
    custom = [s["symbol"] for s in db.get_custom_stocks()]
    all_symbols = list(STOCK_UNIVERSE) + [s for s in custom if s not in STOCK_UNIVERSE]
    failed_symbols = []
    scored_set = set()

    log.info("Phase 1: jugaad_data scan (%d stocks)...", len(all_symbols))
    for batch_start in range(0, len(all_symbols), BATCH_SIZE):
        batch = all_symbols[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(all_symbols) + BATCH_SIZE - 1) // BATCH_SIZE

        if batch_start > 0:
            time.sleep(BATCH_DELAY)

        scored_before = len(results)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_and_analyze, sym, nifty_1m, regime): sym
                for sym in batch
            }
            for future in as_completed(futures):
                sym = futures[future]
                clean = sym.replace(".NS", "")
                with progress_lock:
                    scan_cache["progress"] += 1
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        scored_set.add(clean)
                    else:
                        failed_symbols.append(clean)
                except Exception:
                    failed_symbols.append(clean)

        new_scored = len(results) - scored_before
        done = scan_cache["progress"]
        
        # Update global cache with partial results (sorted by score)
        sorted_partial = sorted(results, key=lambda x: x.get("score", 0), reverse=True)
        scan_cache["results"] = sorted_partial
        
        log.info("Phase 1 batch %d/%d — +%d, total %d scored",
                 batch_num, total_batches, new_scored, len(results))

    # ── PHASE 2: Angel One fallback for failed stocks ──
    if failed_symbols:
        log.info("Phase 2: Angel One fallback for %d failed stocks...", len(failed_symbols))
        time.sleep(5)

        angel_scored = 0
        for i, sym in enumerate(failed_symbols):
            if sym in scored_set:
                continue
            try:
                df = live_feed.fetch_historical(sym, days=DATA_LOOKBACK_DAYS)
                if df is not None and not df.empty and len(df) >= 50:
                    r = fetch_and_analyze(sym, nifty_1m, regime, ext_df=df)
                    if r:
                        results.append(r)
                        scored_set.add(sym)
                        angel_scored += 1
            except Exception:
                pass
            with progress_lock:
                scan_cache["progress"] = len(all_symbols) + i + 1
            if (i + 1) % 50 == 0:
                log.info("Phase 2: %d/%d done, +%d scored", i + 1, len(failed_symbols), angel_scored)

        log.info("Phase 2 done: +%d scored from Angel One", angel_scored)

    heatmap = apply_sector_strength(results)
    summary = generate_ai_summary(results, regime)

    elapsed = time.time() - start_time
    hc_count = sum(1 for r in results if r.get("high_conviction"))

    scan_cache["results"] = results
    scan_cache["heatmap"] = heatmap
    scan_cache["summary"] = summary
    ist = timezone(timedelta(hours=5, minutes=30))
    scan_cache["last_scan"] = datetime.now(ist).strftime("%Y-%m-%d %H:%M IST")
    scan_cache["scanning"] = False
    save_disk_cache()
    log.info("Done in %.0fs! %d scored, %d high conviction, %d errors",
             elapsed, len(results), hc_count, len(scan_cache["errors"]))

    # Auto-subscribe all scored stocks to WebSocket for live prices
    all_syms = [r["symbol"] for r in results]
    live_feed.subscribe(all_syms)
    log.info("Subscribed %d stocks to live feed", len(all_syms))
