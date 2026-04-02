"""API routes — scan, results, live prices, stock detail, export."""

import csv
import io
import threading
from datetime import date, timedelta

import pandas as pd
from flask import Blueprint, jsonify, request, Response
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD, EMAIndicator, SMAIndicator, ADXIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator
from jugaad_data.nse import stock_df

from config import TOP_N_RESULTS, DATA_LOOKBACK_DAYS, CACHE_FILE
from stocks import STOCK_UNIVERSE, SECTORS
from scanner import scan_cache, run_full_scan
from analyzer import fetch_and_analyze
import live_feed
import db

api_bp = Blueprint("api", __name__)


@api_bp.route("/api/scan", methods=["POST"])
def start_scan():
    if scan_cache["scanning"]:
        return jsonify({"status": "already_scanning"})
    threading.Thread(target=run_full_scan, daemon=True).start()
    return jsonify({"status": "started"})


@api_bp.route("/api/force-scan", methods=["POST"])
def force_scan():
    if scan_cache["scanning"]:
        return jsonify({"status": "already_scanning"})
    if CACHE_FILE.exists():
        CACHE_FILE.unlink()
    scan_cache["results"] = []
    threading.Thread(target=run_full_scan, daemon=True).start()
    return jsonify({"status": "force_started"})


@api_bp.route("/api/status")
def scan_status():
    return jsonify({
        "scanning": scan_cache["scanning"],
        "progress": scan_cache["progress"],
        "total": scan_cache["total"],
        "last_scan": scan_cache["last_scan"],
        "market_regime": scan_cache.get("market_regime", "unknown"),
    })


@api_bp.route("/api/results")
def get_results():
    sort_by = request.args.get("sort", "score")
    order = request.args.get("order", "desc")

    results = scan_cache["results"][:TOP_N_RESULTS]
    if not results:
        results = db.load_results(TOP_N_RESULTS)
        if results:
            scan_cache["results"] = results

    valid_sorts = [
        "score", "price", "rsi", "adx", "volume_ratio", "pct_1w", "pct_1m",
        "delivery_pct", "risk_score", "rs_vs_nifty", "risk_reward", "target_pct",
        "atr_pct", "stoch_k", "bb_position",
    ]
    if sort_by in valid_sorts:
        results = sorted(results, key=lambda x: x.get(sort_by, 0), reverse=(order == "desc"))

    return jsonify({
        "results": results,
        "total_analyzed": len(scan_cache["results"]),
        "last_scan": scan_cache["last_scan"],
        "errors": len(scan_cache["errors"]),
        "nifty50_1m": scan_cache.get("nifty50_1m", 0),
        "summary": scan_cache.get("summary", ""),
        "heatmap": scan_cache.get("heatmap", []),
        "market_regime": scan_cache.get("market_regime", "unknown"),
    })


@api_bp.route("/api/export/csv")
def export_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    headers = [
        "Rank", "Symbol", "Sector", "Score", "High Conviction", "Price",
        "Target", "Target%", "StopLoss(ATR)", "SL%", "R:R",
        "RSI", "ADX", "MACD", "Volume", "Weekly Trend",
        "1W%", "2W%", "1M%", "Delivery%", "Fib Level",
        "Risk Score", "RS vs Nifty", "Breakout", "Accumulation",
        "Support S1", "Resistance R1",
    ]
    writer.writerow(headers)
    for i, r in enumerate(scan_cache["results"][:TOP_N_RESULTS]):
        sr = r.get("support_resistance", {})
        writer.writerow([
            i + 1, r["symbol"], r["sector"], r["score"],
            "YES" if r.get("high_conviction") else "",
            r["price"], r["target_price"], r.get("target_pct", ""),
            r["stop_loss"], r.get("stop_loss_pct", ""), r.get("risk_reward", ""),
            r["rsi"], r.get("adx", ""), r["macd_signal"], r["volume_ratio"],
            r.get("weekly_trend", ""),
            r["pct_1w"], r["pct_2w"], r["pct_1m"], r.get("delivery_pct", ""),
            r.get("fib_level", ""), r.get("risk_score", ""), r.get("rs_vs_nifty", ""),
            "YES" if r.get("is_breakout") else "", "YES" if r.get("vp_divergence") else "",
            sr.get("s1", ""), sr.get("r1", ""),
        ])
    output.seek(0)
    return Response(
        output.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment;filename=nifty250_v4_{date.today()}.csv"},
    )


@api_bp.route("/api/stock/<symbol>")
def stock_data(symbol):
    """Return extended indicator series for the detail page."""
    clean = symbol.upper().replace(".NS", "")
    cached = next((r for r in scan_cache["results"] if r["symbol"] == clean), None)

    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=DATA_LOOKBACK_DAYS)
        df = stock_df(symbol=clean, from_date=start_date, to_date=end_date)
        if df.empty or len(df) < 50:
            return jsonify({"error": "Insufficient data"}), 404

        df = df.sort_values("DATE").reset_index(drop=True)
        close = df["CLOSE"].astype(float)
        high = df["HIGH"].astype(float)
        low = df["LOW"].astype(float)
        volume = df["VOLUME"].astype(float)
        delivery_pct = (df["DELIVERY %"].astype(float)
                        if "DELIVERY %" in df.columns
                        else pd.Series([50.0] * len(df)))

        rsi_series = RSIIndicator(close, window=14).rsi()
        macd_ind = MACD(close)
        macd_line_s = macd_ind.macd()
        macd_sig_s = macd_ind.macd_signal()
        macd_hist_s = macd_ind.macd_diff()
        ema_9_s = EMAIndicator(close, window=9).ema_indicator()
        ema_21_s = EMAIndicator(close, window=21).ema_indicator()
        sma_50_s = SMAIndicator(close, window=50).sma_indicator()
        ema_200_s = EMAIndicator(close, window=min(200, len(close) - 1)).ema_indicator()
        bb = BollingerBands(close, window=20, window_dev=2)
        bb_upper_s = bb.bollinger_hband()
        bb_lower_s = bb.bollinger_lband()
        bb_mid_s = bb.bollinger_mavg()
        stoch = StochasticOscillator(high, low, close)
        stoch_k_s = stoch.stoch()
        stoch_d_s = stoch.stoch_signal()
        adx_s = ADXIndicator(high, low, close, window=14).adx()
        atr_s = AverageTrueRange(high, low, close, window=14).average_true_range()
        obv_s = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        avg_vol_s = volume.rolling(20).mean()

        def safe_list(series):
            return [None if pd.isna(v) else round(float(v), 2) for v in series]

        dates = [
            r["DATE"].strftime("%Y-%m-%d") if hasattr(r["DATE"], "strftime")
            else str(r["DATE"])[:10]
            for _, r in df.iterrows()
        ]

        ohlcv = []
        for _, row in df.iterrows():
            ohlcv.append({
                "date": row["DATE"].strftime("%Y-%m-%d") if hasattr(row["DATE"], "strftime") else str(row["DATE"])[:10],
                "o": round(float(row.get("OPEN", row["CLOSE"])), 2),
                "h": round(float(row["HIGH"]), 2),
                "l": round(float(row["LOW"]), 2),
                "c": round(float(row["CLOSE"]), 2),
                "v": int(row.get("VOLUME", 0)),
            })

        sector = SECTORS.get(clean, "Other")
        current_price = float(close.iloc[-1])

        result = {
            "symbol": clean, "sector": sector,
            "price": round(current_price, 2),
            "high_52w": round(float(high.max()), 2),
            "low_52w": round(float(low.min()), 2),
            "dates": dates, "ohlcv": ohlcv,
            "close": safe_list(close), "volume": [int(v) for v in volume],
            "delivery": safe_list(delivery_pct),
            "rsi": safe_list(rsi_series),
            "macd_line": safe_list(macd_line_s), "macd_signal": safe_list(macd_sig_s),
            "macd_hist": safe_list(macd_hist_s),
            "ema_9": safe_list(ema_9_s), "ema_21": safe_list(ema_21_s),
            "sma_50": safe_list(sma_50_s), "ema_200": safe_list(ema_200_s),
            "bb_upper": safe_list(bb_upper_s), "bb_lower": safe_list(bb_lower_s),
            "bb_mid": safe_list(bb_mid_s),
            "stoch_k": safe_list(stoch_k_s), "stoch_d": safe_list(stoch_d_s),
            "adx": safe_list(adx_s), "atr": safe_list(atr_s),
            "obv": safe_list(obv_s), "avg_volume": safe_list(avg_vol_s),
            "market_regime": scan_cache.get("market_regime", "unknown"),
            "nifty50_1m": scan_cache.get("nifty50_1m", 0),
        }

        if cached:
            result["scan"] = {
                "score": cached["score"], "risk_score": cached["risk_score"],
                "risk_reward": cached["risk_reward"],
                "target_price": cached["target_price"], "target_pct": cached.get("target_pct"),
                "stop_loss": cached["stop_loss"], "stop_loss_pct": cached.get("stop_loss_pct"),
                "signals": cached["signals"],
                "high_conviction": cached.get("high_conviction", False),
                "is_breakout": cached.get("is_breakout", False),
                "weekly_trend": cached.get("weekly_trend", "flat"),
                "below_ema200": cached.get("below_ema200", False),
                "vp_divergence": cached.get("vp_divergence", False),
                "fib_level": cached.get("fib_level"),
                "fib_support": cached.get("fib_support"),
                "fib_resistance": cached.get("fib_resistance"),
                "support_resistance": cached.get("support_resistance", {}),
                "pct_1w": cached.get("pct_1w"), "pct_2w": cached.get("pct_2w"),
                "pct_1m": cached.get("pct_1m"), "rs_vs_nifty": cached.get("rs_vs_nifty"),
                "delivery_pct": cached.get("delivery_pct"),
                "delivery_trend": cached.get("delivery_trend"),
                "bb_position": cached.get("bb_position"),
                "vwap_position": cached.get("vwap_position"),
            }

        return jsonify(result)
    except Exception as exc:
        import logging
        logging.getLogger("screener").warning("Stock detail fetch failed for %s: %s", clean, exc)
        return jsonify({"error": str(exc)}), 500


@api_bp.route("/api/live-prices", methods=["POST"])
def live_prices():
    body = request.get_json(silent=True) or {}
    symbols = body.get("symbols", [])
    if not symbols:
        return jsonify({"error": "No symbols provided"}), 400

    symbols = [s.upper().replace(".NS", "") for s in symbols[:500]]
    live_feed.subscribe(symbols)

    ws_prices = live_feed.get_live_prices(symbols)
    missing = [s for s in symbols if s not in ws_prices]
    if missing:
        rest_prices = live_feed.fetch_ltp_bulk(missing[:20])
        ws_prices.update(rest_prices)

    result = {}
    for sym, data in ws_prices.items():
        price = data.get("ltp", 0)
        if not price:
            continue
        entry = {
            "price": price, "open": data.get("open", 0),
            "high": data.get("high", 0), "low": data.get("low", 0),
            "close": data.get("close", 0), "change": data.get("change", 0),
            "change_pct": data.get("change_pct", 0),
            "volume": data.get("volume", 0), "last_update": data.get("last_update", ""),
        }
        scan_data = next((r for r in scan_cache["results"] if r["symbol"] == sym), None)
        if scan_data:
            entry["scan_price"] = scan_data["price"]
        result[sym] = entry

    return jsonify({
        "prices": result, "source": "angel_one",
        "market_open": live_feed.is_market_open(),
        "ws_connected": live_feed._ws_running,
    })


@api_bp.route("/api/custom-stocks", methods=["GET"])
def get_custom_stocks():
    return jsonify({"stocks": db.get_custom_stocks()})


@api_bp.route("/api/custom-stocks", methods=["POST"])
def add_custom_stock():
    body = request.get_json(silent=True) or {}
    symbol = body.get("symbol", "").upper().replace("NSE:", "").replace(".NS", "").strip()
    if not symbol:
        return jsonify({"error": "Symbol required"}), 400

    db.add_custom_stock(symbol, "NSE", body.get("note", ""))
    try:
        nifty_1m = scan_cache.get("nifty50_1m", 0)
        regime = scan_cache.get("market_regime", "unknown")
        result = fetch_and_analyze(symbol, nifty_1m, regime)
        if result:
            result["custom"] = True
            scan_cache["results"] = [r for r in scan_cache["results"] if r["symbol"] != symbol]
            scan_cache["results"].append(result)
            scan_cache["results"].sort(key=lambda x: x.get("score", 0), reverse=True)
            db.save_results([result])
            live_feed.subscribe([symbol])
            return jsonify({"status": "ok", "symbol": symbol, "score": result["score"], "scanned": True})
        else:
            return jsonify({"status": "ok", "symbol": symbol, "scanned": False, "message": "Added but no data available"})
    except Exception as exc:
        return jsonify({"status": "ok", "symbol": symbol, "scanned": False, "message": str(exc)})


@api_bp.route("/api/custom-stocks/<symbol>", methods=["DELETE"])
def remove_custom_stock(symbol):
    clean = symbol.upper().replace("NSE:", "").replace(".NS", "")
    return jsonify({"status": "ok", "removed": db.remove_custom_stock(clean)})


@api_bp.route("/api/health")
def health():
    try:
        db_info = db.db_stats()
    except Exception:
        db_info = {}
    return jsonify({
        "status": "ok", "version": "v4-hc", "stocks": len(STOCK_UNIVERSE),
        "cached_results": len(scan_cache["results"]),
        "db_results": db_info.get("results", 0),
        "db_size_kb": db_info.get("db_size_kb", 0),
        "scanning": scan_cache["scanning"],
        "market_regime": scan_cache.get("market_regime", "unknown"),
        "ws_connected": live_feed._ws_running,
        "live_symbols": len(live_feed._subscribers),
    })


@api_bp.route("/api/stock/history/<symbol>")
def stock_score_history(symbol):
    return jsonify({"symbol": symbol.upper(), "history": db.get_score_history(symbol.upper(), days=30)})
