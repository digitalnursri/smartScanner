"""Portfolio management API routes."""

from datetime import datetime
from flask import Blueprint, jsonify, request

from scanner import scan_cache
import live_feed
import db

portfolio_bp = Blueprint("portfolio", __name__)


@portfolio_bp.route("/api/portfolios", methods=["GET"])
def api_get_portfolios():
    portfolios = db.get_portfolios()
    for p in portfolios:
        p["summary"] = db.get_portfolio_summary(p["id"])
    return jsonify({"portfolios": portfolios})


@portfolio_bp.route("/api/portfolios", methods=["POST"])
def api_create_portfolio():
    body = request.get_json(silent=True) or {}
    name = body.get("name", "").strip()
    if not name:
        return jsonify({"error": "Name required"}), 400
    try:
        pid = db.create_portfolio(name, body.get("description", ""))
        return jsonify({"status": "ok", "id": pid, "name": name})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@portfolio_bp.route("/api/portfolios/<int:pid>", methods=["PUT"])
def api_update_portfolio(pid):
    body = request.get_json(silent=True) or {}
    db.update_portfolio(pid, name=body.get("name"), description=body.get("description"))
    return jsonify({"status": "ok"})


@portfolio_bp.route("/api/portfolios/<int:pid>", methods=["DELETE"])
def api_delete_portfolio(pid):
    db.delete_portfolio(pid)
    return jsonify({"status": "ok"})


@portfolio_bp.route("/api/portfolios/<int:pid>/positions", methods=["GET"])
def api_get_positions(pid):
    status = request.args.get("status")
    positions = db.get_positions(pid, status)
    scan_lookup = {r["symbol"]: r for r in scan_cache.get("results", [])}

    symbols = list({p["symbol"] for p in positions if p["status"] == "OPEN"})
    if symbols:
        live_feed.subscribe(symbols)
        live_prices = live_feed.get_live_prices(symbols)
        for pos in positions:
            sym = pos["symbol"]
            scan = scan_lookup.get(sym, {})

            if pos["status"] == "OPEN":
                lp = live_prices.get(sym, {})
                current = lp.get("ltp", 0)
                if current:
                    pos["current_price"] = current
                    pos["pnl"] = round((current - pos["buy_price"]) * pos["quantity"], 2)
                    pos["pnl_pct"] = round(((current - pos["buy_price"]) / pos["buy_price"]) * 100, 2)
                    pos["day_change"] = lp.get("change", 0)
                    pos["day_change_pct"] = lp.get("change_pct", 0)

                if scan:
                    pos["auto_sl"] = scan.get("stop_loss")
                    pos["auto_sl_pct"] = scan.get("stop_loss_pct")
                    pos["auto_target"] = scan.get("target_price")
                    pos["auto_target_pct"] = scan.get("target_pct")
                    pos["rsi"] = scan.get("rsi")
                    pos["adx"] = scan.get("adx")
                    pos["macd_signal"] = scan.get("macd_signal")
                    pos["score"] = scan.get("score")
                    pos["risk_score"] = scan.get("risk_score")
                    pos["risk_reward"] = scan.get("risk_reward")
                    pos["signals"] = scan.get("signals", [])
                    pos["sector"] = scan.get("sector", "")
                    pos["weekly_trend"] = scan.get("weekly_trend", "")
                    pos["volume_ratio"] = scan.get("volume_ratio")
                    pos["delivery_pct"] = scan.get("delivery_pct")
                    pos["high_conviction"] = scan.get("high_conviction", False)

                    # Generate HOLD/SELL signal
                    signal = "HOLD"
                    signal_reasons = []
                    cp = current or pos["buy_price"]

                    if scan.get("stop_loss") and cp <= scan["stop_loss"]:
                        signal = "SELL"
                        signal_reasons.append("Stop loss hit")
                    elif scan.get("target_price") and cp >= scan["target_price"]:
                        signal = "BOOK PROFIT"
                        signal_reasons.append("Target reached")
                    elif scan.get("rsi", 0) > 75:
                        signal = "SELL"
                        signal_reasons.append(f"RSI overbought ({scan['rsi']})")
                    elif scan.get("rsi", 0) > 65:
                        signal = "TRAIL SL"
                        signal_reasons.append(f"RSI high ({scan['rsi']}), trail stop loss")

                    if scan.get("macd_signal") == "Bearish":
                        if signal == "HOLD":
                            signal = "CAUTION"
                        signal_reasons.append("MACD bearish crossover")
                    if scan.get("adx", 0) < 15:
                        signal_reasons.append("Weak trend (ADX < 15)")

                    pnl_pct = pos.get("pnl_pct", 0)
                    if pnl_pct < -8:
                        signal = "SELL"
                        signal_reasons.append(f"Loss exceeds 8% ({pnl_pct:.1f}%)")
                    elif pnl_pct > 15:
                        signal = "BOOK PROFIT"
                        signal_reasons.append(f"Profit {pnl_pct:.1f}% — consider booking")

                    pos["signal"] = signal
                    pos["signal_reasons"] = signal_reasons

            elif pos["status"] == "CLOSED" and pos["sell_price"]:
                pos["pnl"] = round((pos["sell_price"] - pos["buy_price"]) * pos["quantity"], 2)
                pos["pnl_pct"] = round(((pos["sell_price"] - pos["buy_price"]) / pos["buy_price"]) * 100, 2)
                if scan:
                    pos["sector"] = scan.get("sector", "")

    summary = db.get_portfolio_summary(pid)
    total_current = 0
    total_invested = 0
    for pos in positions:
        if pos["status"] == "OPEN":
            total_invested += pos["buy_price"] * pos["quantity"]
            total_current += pos.get("current_price", pos["buy_price"]) * pos["quantity"]
    summary["current_value"] = round(total_current, 2)
    summary["unrealized_pnl"] = round(total_current - total_invested, 2)
    summary["unrealized_pnl_pct"] = round(((total_current - total_invested) / total_invested * 100), 2) if total_invested else 0
    summary["total_pnl"] = round(summary["unrealized_pnl"] + summary["realized_pnl"], 2)

    return jsonify({"positions": positions, "summary": summary})


@portfolio_bp.route("/api/portfolios/<int:pid>/positions", methods=["POST"])
def api_add_position(pid):
    body = request.get_json(silent=True) or {}
    symbol = body.get("symbol", "").upper().replace("NSE:", "").replace(".NS", "").strip()
    if not symbol or not body.get("buy_price") or not body.get("quantity"):
        return jsonify({"error": "symbol, buy_price, quantity required"}), 400
    pos_id = db.add_position(
        portfolio_id=pid, symbol=symbol,
        quantity=int(body["quantity"]),
        buy_price=float(body["buy_price"]),
        buy_date=body.get("buy_date", datetime.now().strftime("%Y-%m-%d")),
        stop_loss=float(body["stop_loss"]) if body.get("stop_loss") else None,
        target=float(body["target"]) if body.get("target") else None,
        notes=body.get("notes", ""),
    )
    live_feed.subscribe([symbol])
    return jsonify({"status": "ok", "id": pos_id})


@portfolio_bp.route("/api/positions/<int:pos_id>", methods=["PUT"])
def api_update_position(pos_id):
    body = request.get_json(silent=True) or {}
    db.update_position(pos_id, **body)
    return jsonify({"status": "ok"})


@portfolio_bp.route("/api/positions/<int:pos_id>/close", methods=["POST"])
def api_close_position(pos_id):
    body = request.get_json(silent=True) or {}
    sell_price = body.get("sell_price")
    if not sell_price:
        return jsonify({"error": "sell_price required"}), 400
    db.close_position(pos_id, float(sell_price), body.get("sell_date"))
    return jsonify({"status": "ok"})


@portfolio_bp.route("/api/positions/<int:pos_id>", methods=["DELETE"])
def api_delete_position(pos_id):
    db.delete_position(pos_id)
    return jsonify({"status": "ok"})
