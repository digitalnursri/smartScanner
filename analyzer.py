"""
Stock analysis engine — 18 parameters per stock.
Scoring, signals, risk assessment, targets.
"""

import logging
from collections import defaultdict

import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD, EMAIndicator, SMAIndicator, ADXIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator
from jugaad_data.nse import stock_df
from datetime import date, timedelta

from stocks import SECTORS
from config import (
    DATA_LOOKBACK_DAYS, BENCHMARK_LOOKBACK_DAYS,
    MAX_RAW_SCORE, ATR_SL_MULTIPLIER,
    HC_MIN_SCORE, HC_MIN_SIGNALS_BULLISH, HC_RSI_RANGE,
    HC_DELIVERY_MIN, HC_ATR_RANGE, HC_RISK_MAX,
    HC_REQUIRE_MACD_BULLISH, HC_REQUIRE_VOLUME, HC_MIN_RISK_REWARD,
    BP_RSI_MAX, BP_VOLUME_MIN, BP_DELIVERY_MIN,
    BP_WEEK1_MAX_LOSS, BP_MACD_BULLISH, BP_TARGET_PCT,
)

log = logging.getLogger("screener")


# ===================================================================
#  MARKET REGIME
# ===================================================================

def get_nifty50_benchmark():
    """Return (1-month return %, regime string)."""
    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=BENCHMARK_LOOKBACK_DAYS)
        df = stock_df(symbol="NIFTYBEES", from_date=start_date, to_date=end_date)
        if df.empty or len(df) < 30:
            return 0, "unknown"

        df = df.sort_values("DATE").reset_index(drop=True)
        close = df["CLOSE"].astype(float)

        ret_1m = ((close.iloc[-1] / close.iloc[-22]) - 1) * 100 if len(close) >= 22 else 0

        ema20 = EMAIndicator(close, window=20).ema_indicator()
        ema50 = EMAIndicator(close, window=min(50, len(close) - 1)).ema_indicator()
        adx_val = ADXIndicator(
            df["HIGH"].astype(float), df["LOW"].astype(float), close, window=14
        ).adx().iloc[-1]

        curr = close.iloc[-1]
        e20, e50 = ema20.iloc[-1], ema50.iloc[-1]

        if curr > e20 > e50 and adx_val > 20:
            regime = "bullish"
        elif curr < e20 < e50 and adx_val > 20:
            regime = "bearish"
        else:
            regime = "sideways"

        return round(ret_1m, 2), regime
    except (KeyError, ValueError, IndexError) as exc:
        log.warning("Benchmark fetch failed: %s", exc)
        return 0, "unknown"


# ===================================================================
#  HELPER CALCULATIONS
# ===================================================================

def calc_fibonacci(high_val, low_val, current):
    diff = high_val - low_val
    if diff <= 0:
        return {"level": "N/A", "score": 0, "support": None, "resistance": None}

    levels = {
        "0.236": high_val - diff * 0.236,
        "0.382": high_val - diff * 0.382,
        "0.5":   high_val - diff * 0.5,
        "0.618": high_val - diff * 0.618,
        "0.786": high_val - diff * 0.786,
    }

    nearest_support = nearest_resistance = None
    for _name, level in levels.items():
        if level <= current and (nearest_support is None or level > nearest_support[1]):
            nearest_support = (_name, level)
        if level >= current and (nearest_resistance is None or level < nearest_resistance[1]):
            nearest_resistance = (_name, level)

    fib_ratio = (high_val - current) / diff
    score = 0
    level_name = f"{fib_ratio:.3f}"

    if 0.55 <= fib_ratio <= 0.68:
        score, level_name = 15, "Golden Zone (0.618)"
    elif 0.45 <= fib_ratio <= 0.55:
        score, level_name = 12, "50% Retracement"
    elif 0.35 <= fib_ratio <= 0.45:
        score, level_name = 10, "38.2% Zone"
    elif 0.70 <= fib_ratio <= 0.85:
        score, level_name = 8, "Deep Retracement (78.6%)"
    elif fib_ratio > 0.85:
        score, level_name = 5, "Near Bottom"

    return {
        "level": level_name, "ratio": round(fib_ratio, 3), "score": score,
        "support": round(nearest_support[1], 2) if nearest_support else None,
        "resistance": round(nearest_resistance[1], 2) if nearest_resistance else None,
    }


def calc_support_resistance(high, low, close):
    try:
        h, l, c = float(high.iloc[-1]), float(low.iloc[-1]), float(close.iloc[-1])
        pivot = (h + l + c) / 3
        s1 = 2 * pivot - h
        r1 = 2 * pivot - l
        s2 = pivot - (h - l)
        r2 = pivot + (h - l)
        return {k: round(v, 2) for k, v in
                {"pivot": pivot, "s1": s1, "s2": s2, "r1": r1, "r2": r2}.items()}
    except (IndexError, ValueError):
        return {"pivot": 0, "s1": 0, "s2": 0, "r1": 0, "r2": 0}


def calc_risk_score(rsi, atr_pct, dist_high, vol_ratio, pct_1m, below_ema200, adx):
    risk = 40
    if rsi > 75: risk += 20
    elif rsi > 65: risk += 12
    elif rsi < 25: risk += 8
    if atr_pct > 5: risk += 15
    elif atr_pct > 3.5: risk += 8
    if dist_high > -3: risk += 15
    elif dist_high > -8: risk += 5
    if vol_ratio < 0.5: risk += 10
    if pct_1m > 20: risk += 18
    elif pct_1m > 12: risk += 10
    if pct_1m < -20: risk += 12
    elif pct_1m < -12: risk += 6
    if below_ema200: risk += 10
    if adx < 15: risk += 8
    return min(100, max(0, risk))


def detect_breakout(close, high, volume, avg_vol, bb_upper, atr):
    if len(close) < 20:
        return False
    current = float(close.iloc[-1])
    high_20 = float(high.tail(20).max())
    recent_range = float(high.tail(10).max()) - float(close.tail(10).min())
    tight = recent_range < (2.5 * atr)
    near_high = current >= high_20 * 0.98
    vol_confirm = float(volume.iloc[-1]) > avg_vol * 1.3
    bb_confirm = current >= bb_upper * 0.99
    return tight and near_high and vol_confirm and bb_confirm


def detect_vp_divergence(close, obv):
    if len(close) < 20 or len(obv) < 20:
        return False
    price_chg = (float(close.iloc[-1]) / float(close.iloc[-10]) - 1) * 100
    obv_val = float(obv.iloc[-10])
    obv_chg = (float(obv.iloc[-1]) - obv_val) / abs(obv_val) * 100 if abs(obv_val) > 0 else 0
    return price_chg < 2 and obv_chg > 8


def get_weekly_trend(close):
    if len(close) < 25:
        return "flat"
    weekly_closes = [float(close.iloc[-1]), float(close.iloc[-5]), float(close.iloc[-10]),
                     float(close.iloc[-15]), float(close.iloc[-20])]
    up_weeks = sum(1 for i in range(len(weekly_closes) - 1)
                   if weekly_closes[i] > weekly_closes[i + 1])
    if up_weeks >= 3:
        return "up"
    elif up_weeks <= 1:
        return "down"
    return "flat"


# ===================================================================
#  AI SUMMARY
# ===================================================================

def generate_ai_summary(results, regime):
    if not results:
        return "No strong picks found in current scan."

    top5 = results[:5]
    hc_count = sum(1 for r in results if r.get("high_conviction"))
    sectors = defaultdict(int)
    for r in top5:
        sectors[r["sector"]] += 1
    dominant = max(sectors, key=sectors.get) if sectors else "Mixed"
    avg_rsi = sum(r["rsi"] for r in top5) / len(top5)
    avg_delivery = sum(r.get("delivery_pct", 0) for r in top5) / len(top5)
    avg_rr = sum(r.get("risk_reward", 0) for r in top5) / len(top5)

    lines = []
    regime_text = {
        "bullish": "BULLISH — favour momentum & breakout plays",
        "bearish": "BEARISH — favour defensive, high-delivery, oversold bounces only",
        "sideways": "SIDEWAYS — favour range-bound reversals & Fibonacci entries",
    }.get(regime, "UNKNOWN — use caution")
    lines.append(f"Market Regime: {regime_text}")

    if regime == "bearish":
        lines.append("WARNING: Market in downtrend. Reduce position sizes. Only trade HIGH CONVICTION picks.")

    lines.append(f"Top 5 Avg RSI: {avg_rsi:.0f} — {'Oversold zone, reversal likely' if avg_rsi < 45 else 'Neutral zone with momentum' if avg_rsi < 60 else 'Getting overbought, be cautious'}")
    lines.append(f"Dominant Sector: {dominant} — institutional interest concentrated here")
    lines.append(f"High Conviction Picks: {hc_count} stocks passed all filters")
    lines.append(f"Avg Risk:Reward (Top 5): {avg_rr:.1f}x")

    if avg_delivery > 55:
        lines.append(f"Avg Delivery: {avg_delivery:.0f}% — Strong! Serious buying, not just trading")

    for i, r in enumerate(top5[:3]):
        risk = r.get("risk_score", 50)
        risk_label = "Low Risk" if risk < 40 else "Medium Risk" if risk < 65 else "High Risk"
        hc_tag = " [HC]" if r.get("high_conviction") else ""
        brk_tag = " [BREAKOUT]" if r.get("is_breakout") else ""
        lines.append(
            f"#{i+1} {r['symbol']}{hc_tag}{brk_tag} ({r['score']}/100) — "
            f"{r['signals'][0][0] if r['signals'] else 'N/A'} | {risk_label} | "
            f"Target: +{r.get('target_pct', 10)}% | R:R {r.get('risk_reward', 0):.1f}x"
        )

    return "\n".join(lines)


# ===================================================================
#  SECTOR STRENGTH
# ===================================================================

def apply_sector_strength(results):
    sector_scores = defaultdict(list)
    for r in results:
        sector_scores[r["sector"]].append(r["pct_1m"])

    sector_avg = {sec: (sum(vals) / len(vals)) for sec, vals in sector_scores.items() if vals}
    sorted_sectors = sorted(sector_avg.items(), key=lambda x: x[1], reverse=True)
    top_sectors = set(s[0] for s in sorted_sectors[:5] if s[1] > -5)

    heatmap = [
        {"sector": sec, "avg_return": round(avg, 2),
         "count": len(sector_scores[sec]), "strong": sec in top_sectors}
        for sec, avg in sorted_sectors
    ]

    for r in results:
        if r["sector"] in top_sectors:
            r["score"] = min(100, r["score"] + 3)
            r["signals"].append(("Strong Sector", f"{r['sector']} outperforming", "bullish"))
            r["sector_strong"] = True
        else:
            r["sector_strong"] = False

    results.sort(key=lambda x: (-x.get("high_conviction", False), -x["score"]))
    return heatmap


# ===================================================================
#  CORE ANALYSIS (18 parameters per stock)
# ===================================================================

def fetch_and_analyze(symbol: str, nifty_1m: float = 0, regime: str = "unknown",
                      ext_df: "pd.DataFrame | None" = None) -> dict | None:
    """Analyze a stock. If ext_df is provided, skip data fetch (used for Angel One fallback)."""
    try:
        clean = symbol.replace(".NS", "")

        if ext_df is not None:
            df = ext_df
        else:
            end_date = date.today()
            start_date = end_date - timedelta(days=DATA_LOOKBACK_DAYS)
            df = stock_df(symbol=clean, from_date=start_date, to_date=end_date)

        if df.empty or len(df) < 50:
            return None

        df = df.sort_values("DATE").reset_index(drop=True)
        close = df["CLOSE"].astype(float)
        high = df["HIGH"].astype(float)
        low = df["LOW"].astype(float)
        volume = df["VOLUME"].astype(float)
        vwap = df["VWAP"].astype(float) if "VWAP" in df.columns else close
        delivery_pct = (df["DELIVERY %"].astype(float)
                        if "DELIVERY %" in df.columns
                        else pd.Series([50.0] * len(df)))

        current_price = float(close.iloc[-1])
        sector = SECTORS.get(clean, "Other")

        # === CHART DATA ===
        chart_data = []
        for _, row in df.tail(30).iterrows():
            chart_data.append({
                "date": row["DATE"].strftime("%m/%d") if hasattr(row["DATE"], "strftime") else str(row["DATE"])[:5],
                "o": round(float(row.get("OPEN", row["CLOSE"])), 2),
                "h": round(float(row["HIGH"]), 2),
                "l": round(float(row["LOW"]), 2),
                "c": round(float(row["CLOSE"]), 2),
                "v": int(row.get("VOLUME", 0)),
            })

        # === INDICATORS ===
        rsi = float(RSIIndicator(close, window=14).rsi().iloc[-1])

        macd_ind = MACD(close)
        macd_line = float(macd_ind.macd().iloc[-1])
        macd_sig_val = float(macd_ind.macd_signal().iloc[-1])
        macd_hist = float(macd_ind.macd_diff().iloc[-1])
        macd_hist_prev = float(macd_ind.macd_diff().iloc[-2])

        ema_9 = float(EMAIndicator(close, window=9).ema_indicator().iloc[-1])
        ema_21 = float(EMAIndicator(close, window=21).ema_indicator().iloc[-1])
        sma_50 = float(SMAIndicator(close, window=50).sma_indicator().iloc[-1])
        ema_200 = float(EMAIndicator(close, window=min(200, len(close) - 1)).ema_indicator().iloc[-1])
        below_ema200 = current_price < ema_200

        adx = float(ADXIndicator(high, low, close, window=14).adx().iloc[-1])

        bb = BollingerBands(close, window=20, window_dev=2)
        bb_upper = float(bb.bollinger_hband().iloc[-1])
        bb_lower = float(bb.bollinger_lband().iloc[-1])
        bb_range = bb_upper - bb_lower
        bb_pct = (current_price - bb_lower) / bb_range if bb_range > 0 else 0.5

        avg_vol = float(volume.rolling(20).mean().iloc[-1])
        vol_ratio = float(volume.iloc[-1] / avg_vol) if avg_vol > 0 else 1.0

        atr_val = float(AverageTrueRange(high, low, close, window=14).average_true_range().iloc[-1])
        atr_pct = (atr_val / current_price) * 100

        stoch = StochasticOscillator(high, low, close)
        stoch_k = float(stoch.stoch().iloc[-1])
        stoch_d = float(stoch.stoch_signal().iloc[-1])

        pct_1w = float(((close.iloc[-1] / close.iloc[-5]) - 1) * 100) if len(close) >= 5 else 0
        pct_2w = float(((close.iloc[-1] / close.iloc[-10]) - 1) * 100) if len(close) >= 10 else 0
        pct_1m = float(((close.iloc[-1] / close.iloc[-22]) - 1) * 100) if len(close) >= 22 else 0

        high_52w = float(high.max())
        low_52w = float(low.min())
        dist_high = ((current_price - high_52w) / high_52w) * 100

        obv = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        obv_slope = float((obv.iloc[-1] - obv.iloc[-5]) / abs(obv.iloc[-5]) * 100
                          if abs(float(obv.iloc[-5])) > 0 else 0)

        avg_delivery = float(delivery_pct.rolling(10).mean().iloc[-1]
                             if len(delivery_pct) >= 10 else delivery_pct.mean())
        curr_delivery = float(delivery_pct.iloc[-1])
        delivery_trend = curr_delivery - avg_delivery
        if np.isnan(avg_delivery): avg_delivery = 50.0
        if np.isnan(curr_delivery): curr_delivery = 50.0
        if np.isnan(delivery_trend): delivery_trend = 0.0

        fib = calc_fibonacci(high_52w, low_52w, current_price)

        curr_vwap = float(vwap.iloc[-1])
        vwap_position = ((current_price - curr_vwap) / curr_vwap) * 100 if curr_vwap > 0 else 0

        rs_vs_nifty = pct_1m - nifty_1m

        sr = calc_support_resistance(high, low, close)
        is_breakout = detect_breakout(close, high, volume, avg_vol, bb_upper, atr_val)
        vp_divergence = detect_vp_divergence(close, obv)
        weekly_trend = get_weekly_trend(close)
        risk_score = calc_risk_score(rsi, atr_pct, dist_high, vol_ratio, pct_1m, below_ema200, adx)

        # ===============================================================
        #  SCORING  (max raw ~230)
        # ===============================================================
        score = 0
        signals = []

        # --- RSI ---
        if 28 <= rsi <= 45:
            score += 20; signals.append(("RSI Oversold Bounce", f"RSI: {rsi:.1f}", "bullish"))
        elif 45 < rsi <= 60:
            score += 15; signals.append(("RSI Neutral-Bullish", f"RSI: {rsi:.1f}", "bullish"))
        elif rsi < 28:
            score += 8; signals.append(("RSI Deeply Oversold", f"RSI: {rsi:.1f}", "neutral"))
        elif rsi > 75:
            score -= 15; signals.append(("RSI Overbought", f"RSI: {rsi:.1f}", "bearish"))
        elif rsi > 65:
            score -= 5; signals.append(("RSI Warm", f"RSI: {rsi:.1f}", "bearish"))

        # --- MACD ---
        if macd_line > macd_sig_val and macd_hist > macd_hist_prev:
            score += 20; signals.append(("MACD Bullish Crossover", "Histogram expanding", "bullish"))
        elif macd_line > macd_sig_val:
            score += 10; signals.append(("MACD Bullish", "Above signal", "bullish"))
        elif macd_hist > macd_hist_prev:
            score += 5; signals.append(("MACD Improving", "Recovering", "neutral"))
        else:
            signals.append(("MACD Bearish", "Below signal", "bearish"))

        if ema_9 > ema_21:
            score += 15; signals.append(("EMA Bullish Cross", "9 > 21 EMA", "bullish"))
        if current_price > sma_50:
            score += 5; signals.append(("Above 50 SMA", "Uptrend confirmed", "bullish"))

        if below_ema200:
            score -= 12; signals.append(("Below EMA 200", "Structural downtrend", "bearish"))
        else:
            score += 8; signals.append(("Above EMA 200", "Long-term uptrend", "bullish"))

        if adx > 30:
            score += 15; signals.append(("Strong Trend", f"ADX: {adx:.0f}", "bullish"))
        elif adx > 20:
            score += 8; signals.append(("Trending", f"ADX: {adx:.0f}", "bullish"))
        elif adx < 15:
            score -= 5; signals.append(("No Trend (Choppy)", f"ADX: {adx:.0f}", "bearish"))
        else:
            signals.append(("Weak Trend", f"ADX: {adx:.0f}", "neutral"))

        if bb_pct < 0.15:
            score += 15; signals.append(("BB Squeeze", f"{bb_pct:.0%} — bounce zone", "bullish"))
        elif bb_pct < 0.35:
            score += 10; signals.append(("BB Lower Half", f"{bb_pct:.0%}", "neutral"))
        elif bb_pct > 0.90:
            score -= 5; signals.append(("BB Overbought", f"{bb_pct:.0%}", "bearish"))

        if vol_ratio > 2.5:
            score += 18; signals.append(("Volume Explosion", f"{vol_ratio:.1f}x avg", "bullish"))
        elif vol_ratio > 2.0:
            score += 15; signals.append(("Volume Surge", f"{vol_ratio:.1f}x avg", "bullish"))
        elif vol_ratio > 1.5:
            score += 10; signals.append(("High Volume", f"{vol_ratio:.1f}x", "bullish"))

        if 2.0 <= atr_pct <= 4.0:
            score += 10; signals.append(("Volatility Sweet Spot", f"ATR: {atr_pct:.1f}%", "bullish"))
        elif 1.5 <= atr_pct < 2.0 or 4.0 < atr_pct <= 5.5:
            score += 5; signals.append(("Moderate Volatility", f"ATR: {atr_pct:.1f}%", "neutral"))

        if stoch_k < 25 and stoch_k > stoch_d:
            score += 12; signals.append(("Stoch Reversal", f"K={stoch_k:.0f} > D={stoch_d:.0f}", "bullish"))
        elif stoch_k < 35 and stoch_k > stoch_d:
            score += 7; signals.append(("Stoch Improving", f"K={stoch_k:.0f}", "bullish"))

        if -12 < pct_2w < -2 and pct_1w > 0 and pct_1w > pct_2w:
            score += 12; signals.append(("Strong Reversal", f"2W: {pct_2w:+.1f}% -> 1W: {pct_1w:+.1f}%", "bullish"))
        elif -10 < pct_2w < 0 and pct_1w > pct_2w:
            score += 8; signals.append(("Reversal Pattern", f"1W: {pct_1w:+.1f}%", "bullish"))
        elif 0 < pct_1w < 5:
            score += 4; signals.append(("Momentum Building", f"1W: +{pct_1w:.1f}%", "bullish"))

        if obv_slope > 8:
            score += 10; signals.append(("OBV Surging", f"+{obv_slope:.1f}% accumulation", "bullish"))
        elif obv_slope > 3:
            score += 5; signals.append(("OBV Rising", f"+{obv_slope:.1f}%", "bullish"))

        if -25 <= dist_high <= -8:
            score += 10; signals.append(("Pullback Zone", f"{dist_high:.1f}% from high", "bullish"))
        elif -8 < dist_high <= -3:
            score += 5; signals.append(("Mild Pullback", f"{dist_high:.1f}% from high", "neutral"))

        if curr_delivery > 65 and delivery_trend > 8:
            score += 18; signals.append(("Delivery Surge", f"{curr_delivery:.0f}% (+{delivery_trend:.0f}%)", "bullish"))
        elif curr_delivery > 55 and delivery_trend > 3:
            score += 14; signals.append(("Strong Delivery", f"{curr_delivery:.0f}% (+{delivery_trend:.0f}%)", "bullish"))
        elif curr_delivery > 45 and delivery_trend > 0:
            score += 8; signals.append(("Good Delivery", f"{curr_delivery:.0f}%", "bullish"))
        elif curr_delivery > 35:
            score += 3; signals.append(("Avg Delivery", f"{curr_delivery:.0f}%", "neutral"))

        if fib["score"] > 0:
            score += fib["score"]
            signals.append(("Fibonacci", fib["level"], "bullish" if fib["score"] >= 12 else "neutral"))

        if -0.5 <= vwap_position <= 1.0:
            score += 10; signals.append(("VWAP Support", f"{vwap_position:+.1f}%", "bullish"))
        elif -2 < vwap_position < -0.5:
            score += 5; signals.append(("Below VWAP", f"{vwap_position:+.1f}%", "neutral"))

        if rs_vs_nifty > 8:
            score += 12; signals.append(("Crushing Nifty", f"+{rs_vs_nifty:.1f}% vs Nifty", "bullish"))
        elif rs_vs_nifty > 3:
            score += 8; signals.append(("Outperforming Nifty", f"+{rs_vs_nifty:.1f}% vs Nifty", "bullish"))
        elif rs_vs_nifty > 0:
            score += 4; signals.append(("Beating Nifty", f"+{rs_vs_nifty:.1f}%", "bullish"))

        if is_breakout:
            score += 15; signals.append(("Breakout!", "Breaking consolidation with volume", "bullish"))
        if vp_divergence:
            score += 10; signals.append(("Accumulation", "OBV rising while price flat — smart money", "bullish"))

        if regime == "bearish":
            score = int(score * 0.85)
            signals.append(("Bear Market", "Score reduced 15%", "bearish"))
        elif regime == "bullish" and not below_ema200:
            score += 5
            signals.append(("Bull Market Tailwind", "Market supportive", "bullish"))

        if weekly_trend == "up" and ema_9 > ema_21:
            score += 8; signals.append(("Weekly Uptrend", "Multi-timeframe aligned", "bullish"))
        elif weekly_trend == "down" and ema_9 < ema_21:
            score -= 8; signals.append(("Weekly Downtrend", "Multi-timeframe bearish", "bearish"))

        if score < -50:
            return None

        score_100 = min(100, max(0, round((score / MAX_RAW_SCORE) * 100)))

        # ===============================================================
        #  ATR-BASED TARGETS & STOP-LOSS
        # ===============================================================
        atr_stop = current_price - (ATR_SL_MULTIPLIER * atr_val)
        stop_loss_pct = round(((atr_stop - current_price) / current_price) * 100, 1)

        risk_distance = current_price - atr_stop
        default_target = current_price + (2.5 * risk_distance)

        target_candidates = [default_target]
        if fib.get("resistance") and fib["resistance"] > current_price * 1.03:
            target_candidates.append(fib["resistance"])
        if sr.get("r1") and sr["r1"] > current_price * 1.03:
            target_candidates.append(sr["r1"])
        if sr.get("r2") and sr["r2"] > current_price * 1.05:
            target_candidates.append(sr["r2"])

        realistic_targets = [t for t in target_candidates if t <= default_target * 1.2]
        target_price = max(realistic_targets) if realistic_targets else default_target
        target_pct = round(((target_price - current_price) / current_price) * 100, 1)

        risk_reward = round(
            (target_price - current_price) / risk_distance, 1
        ) if risk_distance > 0 else 0

        # ===============================================================
        #  HIGH CONVICTION FLAG
        # ===============================================================
        bullish_signals = sum(1 for s in signals if s[2] == "bullish")
        macd_is_bullish = macd_line > macd_sig_val

        # --- Strict HC: quality picks across all markets ---
        high_conviction = (
            score >= HC_MIN_SCORE
            and bullish_signals >= HC_MIN_SIGNALS_BULLISH
            and HC_RSI_RANGE[0] <= rsi <= HC_RSI_RANGE[1]
            and curr_delivery >= HC_DELIVERY_MIN
            and HC_ATR_RANGE[0] <= atr_pct <= HC_ATR_RANGE[1]
            and risk_score <= HC_RISK_MAX
            and risk_reward >= HC_MIN_RISK_REWARD
            and (not HC_REQUIRE_MACD_BULLISH or macd_is_bullish)
            and vol_ratio >= HC_REQUIRE_VOLUME
        )

        # --- Bear Play: oversold bounce candidates (bear market only) ---
        bear_play = (
            regime == "bearish"
            and rsi < BP_RSI_MAX
            and vol_ratio >= BP_VOLUME_MIN
            and curr_delivery >= BP_DELIVERY_MIN
            and pct_1w >= BP_WEEK1_MAX_LOSS
            and (not BP_MACD_BULLISH or macd_is_bullish)
        )

        def _safe(v, default=0):
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                return default
            return v

        return {
            "symbol": clean, "name": clean, "sector": sector,
            "price": round(current_price, 2),
            "score": score_100,
            "signals": signals,
            "rsi": _safe(round(rsi, 1)),
            "adx": _safe(round(adx, 1)),
            "macd_signal": "Bullish" if macd_line > macd_sig_val else "Bearish",
            "volume_ratio": _safe(round(vol_ratio, 1), 1.0),
            "atr_pct": _safe(round(atr_pct, 2)),
            "pct_1w": _safe(round(pct_1w, 2)),
            "pct_2w": _safe(round(pct_2w, 2)),
            "pct_1m": _safe(round(pct_1m, 2)),
            "bb_position": _safe(round(bb_pct * 100, 1)),
            "dist_from_high": _safe(round(dist_high, 1)),
            "target_price": _safe(round(target_price, 2)),
            "target_pct": _safe(target_pct),
            "stop_loss": _safe(round(atr_stop, 2)),
            "stop_loss_pct": _safe(stop_loss_pct),
            "risk_reward": _safe(risk_reward),
            "delivery_pct": _safe(round(curr_delivery, 1), 50.0),
            "delivery_trend": _safe(round(delivery_trend, 1)),
            "fib_level": fib["level"],
            "fib_support": fib.get("support"),
            "fib_resistance": fib.get("resistance"),
            "vwap_position": _safe(round(vwap_position, 2)),
            "stoch_k": _safe(round(stoch_k, 1)),
            "stoch_d": _safe(round(stoch_d, 1)),
            "rs_vs_nifty": _safe(round(rs_vs_nifty, 2)),
            "risk_score": _safe(risk_score),
            "support_resistance": sr,
            "chart_data": chart_data,
            "high_conviction": high_conviction,
            "bear_play": bear_play,
            "is_breakout": is_breakout,
            "vp_divergence": vp_divergence,
            "weekly_trend": weekly_trend,
            "below_ema200": below_ema200,
        }
    except (KeyError, ValueError, IndexError, TypeError) as exc:
        log.debug("Analysis failed for %s: %s", symbol, exc)
        return None
