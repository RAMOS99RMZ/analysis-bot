# engine/signal_generator.py
# Runs all 11 experts, scores consensus, applies filters, builds final signal

from __future__ import annotations
import uuid
from typing import Dict, List, Optional, Tuple
from loguru import logger

from config import CONFIG, ASSET_VOLATILITY, VOL_THRESHOLDS
from strategies import EXPERTS, EXPERT_NAMES, EXPERT_WEIGHTS
from engine.indicator_engine import IndicatorEngine
from engine.risk_manager import (
    calc_sltp, calc_position_size, get_trade_type,
    atr_pct_check, check_price_drift, portfolio_risk_check, get_obi_signal
)
from utils.helpers import (
    was_recently_sent, mark_sent, check_re_entry,
    record_sl_hit, get_session, is_opportunity_window
)

IE = IndicatorEngine()


def _get_consensus(expert_results: List[Optional[Dict]], direction: str) -> Dict:
    """Weighted consensus of all 11 experts."""
    scores, total_w, weighted_sum, votes = [], 0.0, 0.0, 0
    for i, res in enumerate(expert_results):
        s = (res.get("long", 0) if direction == "LONG" else res.get("short", 0)) if res else 0.0
        w = EXPERT_WEIGHTS[i]
        scores.append(s)
        weighted_sum += s * w
        total_w += w
        if s >= 0.10: votes += 1
    avg      = sum(scores) / len(scores) if scores else 0.0
    weighted = (weighted_sum / total_w) if total_w > 0 else avg
    return {"avg_score": avg, "votes": votes, "weighted_score": weighted}


def _calc_z_score(c5m, c1h, c4h, direction: str) -> float:
    """Z-Score momentum filter."""
    try:
        def zs(candles, n=20):
            if not candles or len(candles) < n: return 0.0
            cl = [float(c[4]) for c in candles[:n]]
            m  = sum(cl) / n
            s  = (sum((x - m)**2 for x in cl) / n) ** 0.5
            return (cl[0] - m) / s if s > 0 else 0.0
        z5 = zs(c5m, 20); z1 = zs(c1h, 20); z4 = zs(c4h, 20)
        zC = z5 * 0.20 + z1 * 0.30 + z4 * 0.50
        score = 0.0
        if   zC < -2.0 and direction == "LONG":  score = 0.30
        elif zC < -1.5 and direction == "LONG":  score = 0.18
        elif zC >  2.0 and direction == "SHORT": score = 0.30
        elif zC >  1.5 and direction == "SHORT": score = 0.18
        return score
    except:
        return 0.0


def _calc_momentum(c5m, c4h, direction: str) -> float:
    """Momentum score."""
    try:
        if not c5m or len(c5m) < 13: return 0.0
        p0, p3, p6, p12 = [float(c5m[i][4]) for i in [0, 3, 6, 12]]
        m3 = (p0-p3)/(p3 or 1); m6 = (p0-p6)/(p6 or 1); m12 = (p0-p12)/(p12 or 1)
        score = 0.0
        if direction == "LONG":
            if m3>0 and m6>0 and m12>0: score += 0.30
            elif m3>0 and m6>0:         score += 0.18
            elif m3>0:                  score += 0.08
            if m3>m6 and m6>0:          score += 0.12
        else:
            if m3<0 and m6<0 and m12<0: score += 0.30
            elif m3<0 and m6<0:         score += 0.18
            elif m3<0:                  score += 0.08
            if m3<m6 and m6<0:          score += 0.12
        if c4h and len(c4h) >= 8:
            q0, q3 = float(c4h[0][4]), float(c4h[3][4])
            q6 = float(c4h[min(6, len(c4h)-1)][4])
            if direction == "LONG"  and q0>q3 and q0>q6: score += 0.15
            elif direction == "SHORT" and q0<q3 and q0<q6: score += 0.15
        return score
    except:
        return 0.0


def generate_signal(symbol: str, direction: str, data: Dict,
                    balance: float, open_trades: List[Dict],
                    run_id: str = "") -> Optional[Dict]:
    """Full signal generation pipeline for one symbol + direction."""
    c5m  = data.get("c5m",  [])
    c15m = data.get("c15m", [])
    c30m = data.get("c30m", [])
    c1h  = data.get("c1h",  [])
    c4h  = data.get("c4h",  [])
    if not c5m or len(c5m) < 20: return None

    price = float(c5m[0][4])

    # ── Portfolio check ───────────────────────────────────────────────
    port = portfolio_risk_check(open_trades, symbol, direction, balance)
    if not port["pass"]:
        logger.debug(f"[SG] {symbol} {direction} — portfolio: {port['reason']}")
        return None

    # ── Cooldown check ────────────────────────────────────────────────
    if was_recently_sent(symbol, direction, True):
        return None

    # ── Re-entry / SL block ───────────────────────────────────────────
    re = check_re_entry(symbol, direction)
    if re["blocked"]: return None

    # ── Run all 11 experts ────────────────────────────────────────────
    expert_results: List[Optional[Dict]] = []
    for mod in EXPERTS:
        try:
            res = mod.analyze(data)
            expert_results.append(res)
        except Exception as e:
            logger.warning(f"[SG] Expert error {mod.__name__}: {e}")
            expert_results.append(None)

    # ── Consensus ─────────────────────────────────────────────────────
    consensus   = _get_consensus(expert_results, direction)
    score       = consensus["weighted_score"]
    votes       = consensus["votes"]

    # ── Quant sub-scores ──────────────────────────────────────────────
    z_score  = _calc_z_score(c5m, c1h, c4h, direction)
    mom_sc   = _calc_momentum(c5m, c4h, direction)
    iw       = IE.calc_iw_score(c5m, c1h, c4h, direction)
    mtf      = IE.get_mtf_score(c5m, c15m, c30m, c1h, c4h, direction)
    regime_d = IE.get_market_regime(c4h, c1h)

    final_score = (score   * 0.45 + z_score        * 0.15 +
                   mom_sc  * 0.15 + iw["score"]    * 0.15 +
                   mtf["score"] * 0.10)
    mtf_score   = mtf["score"]

    obi = get_obi_signal(data.get("book_imbalance"), direction)
    final_score += obi["boost"] * 0.10

    regime = regime_d.get("regime", "UNKNOWN")

    # ── ATR & size ────────────────────────────────────────────────────
    atr = IE.atr(c5m, 14)
    if not atr_pct_check(price, atr): return None

    trade_type = get_trade_type(c5m, c1h, c4h, direction, regime)
    sltp       = calc_sltp(price, atr, direction, trade_type)
    if sltp["rr"] < CONFIG.MIN_RR: return None

    size_usdt = calc_position_size(balance, price, atr)
    if size_usdt <= 0: return None

    # ── Volatility-profile thresholds ─────────────────────────────────
    vol_profile = ASSET_VOLATILITY.get(symbol, "MEDIUM")
    thresh      = VOL_THRESHOLDS[vol_profile]   # ← dict
    is_scalp    = "Scalp" in trade_type
    session     = get_session()

    # ✅ الإصلاح: thresh["key"] بدل thresh.key
    # ✅ الإصلاح: أسماء CONFIG الصحيحة
    if is_opportunity_window() and session.get("overlap"):
        min_score = CONFIG.OPP_SCORE
        min_votes = CONFIG.OPP_VOTES
        min_mtf   = CONFIG.OPP_MTF
    elif is_scalp:
        min_score = max(thresh["min_score"], CONFIG.SCALP_MIN_SCORE)
        min_votes = max(thresh["min_votes"], CONFIG.SCALP_MIN_VOTES)
        min_mtf   = max(thresh["min_mtf"],   CONFIG.SCALP_MIN_MTF)
    else:
        min_score = max(thresh["min_score"], CONFIG.SWING_MIN_SCORE)
        min_votes = max(thresh["min_votes"], CONFIG.SWING_MIN_VOTES)
        min_mtf   = max(thresh["min_mtf"],   CONFIG.SWING_MIN_MTF)

    if final_score < min_score:
        logger.debug(f"[SG] {symbol} {direction} score={final_score:.3f} < {min_score}")
        return None
    if votes < min_votes:
        logger.debug(f"[SG] {symbol} {direction} votes={votes} < {min_votes}")
        return None
    if mtf_score < min_mtf:
        logger.debug(f"[SG] {symbol} {direction} mtf={mtf_score:.3f} < {min_mtf}")
        return None

    # ── Build experts_fired list ──────────────────────────────────────
    experts_fired = [
        EXPERT_NAMES[i] for i, r in enumerate(expert_results)
        if r and ((direction == "LONG"  and r.get("long",  0) >= 0.10) or
                  (direction == "SHORT" and r.get("short", 0) >= 0.10))
    ]

    mark_sent(symbol, direction, is_scalp)

    signal = {
        "symbol":           symbol,
        "direction":        direction,
        "trade_type":       trade_type,
        "score":            round(final_score, 4),
        "expert_votes":     votes,
        "mtf_score":        round(mtf_score, 4),
        "entry":            round(price, 6),
        "sl":               sltp["sl"],
        "tp1":              sltp["tp1"],
        "tp2":              sltp["tp2"],
        "tp3":              sltp["tp3"],
        "rr":               sltp["rr"],
        "size_usdt":        size_usdt,
        "regime":           regime,
        "timeframe":        "5m",
        "experts_fired":    experts_fired,
        "ai_confirmation":  "PENDING",
        "run_id":           run_id or str(uuid.uuid4())[:8],
        "session":          session["name"],
        "obi_boost":        obi["boost"],
    }
    logger.info(f"✅ Signal: {symbol} {direction} {trade_type} score={final_score:.3f} votes={votes}")
    return signal


def run_all_assets(assets_data: Dict, balance: float, open_trades: List[Dict],
                   run_id: str = "") -> List[Dict]:
    """Run signal generator across all assets + both directions."""
    signals = []
    for symbol, data in assets_data.items():
        if not data: continue
        for direction in ["LONG", "SHORT"]:
            sig = generate_signal(symbol, direction, data, balance, open_trades, run_id)
            if sig:
                signals.append(sig)
    signals.sort(key=lambda s: s["score"], reverse=True)
    return signals[:CONFIG.MAX_SIGNALS_PER_RUN]
