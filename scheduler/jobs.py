
"""
scheduler/jobs.py — Ramos 360 Ai 🎖️
Full Layer Architecture:
  Layer 1: Market Intelligence (every 4h)
  Layer 2: Swing Hunter (every 2h, only if L1 non-neutral)
  Layer 3: Scalp Engine (every 5m, session-filtered + AI pre-filter)
"""
from __future__ import annotations
import asyncio
from loguru import logger
from config import CONFIG, Secrets
from engine.data_fetcher import DataFetcher
from engine.signal_generator import run_all_assets
from engine.market_intelligence import run_layer1, get_cached_context
from engine.session_filter import (
    is_session_allowed, session_quality_score, calc_dynamic_sl_tp, validate_levels
)
from ai.gemini_prefilter import gemini_market_check
from ai.confirmation import confirm_signal
from database import SupabaseLogger
from notifier import TelegramNotifier
from utils.helpers import is_circuit_open, make_run_id, get_session

_db:       SupabaseLogger   = None
_notifier: TelegramNotifier = None
_fetcher:  DataFetcher      = None

def init(db, notifier, fetcher):
    global _db, _notifier, _fetcher
    _db = db; _notifier = notifier; _fetcher = fetcher


# ══════════════════════════════════════════════════════════════════
# MONITOR POSITIONS — UPDATE existing trades (not insert new)
# ══════════════════════════════════════════════════════════════════
async def job_monitor_positions():
    if is_circuit_open(): return
    try:
        open_trades = _db.get_open_trades()
        if not open_trades:
            logger.info("[Monitor] No open trades")
            return
        logger.info(f"[Monitor] Checking {len(open_trades)} open trades …")

        for trade in open_trades:
            trade_id  = trade.get("id")
            symbol    = trade.get("symbol")
            direction = trade.get("direction")
            entry     = float(trade.get("entry_price", 0) or 0)
            sl        = float(trade.get("sl_price",    0) or 0)
            tp1       = float(trade.get("tp1_price",   0) or 0)
            tp2       = float(trade.get("tp2_price",   0) or 0)
            tp3       = float(trade.get("tp3_price",   0) or 0)
            if not symbol or not entry or not trade_id:
                continue

            price = await _fetcher.get_live_price(symbol)
            if not price:
                continue

            pnl_pct = ((price - entry) / entry * 100) if direction == "LONG" \
                       else ((entry - price) / entry * 100)

            event = None
            if direction == "LONG":
                if sl  and price <= sl:  event = "SL"
                elif tp3 and price >= tp3: event = "TP3"
                elif tp2 and price >= tp2: event = "TP2"
                elif tp1 and price >= tp1: event = "TP1"
            else:
                if sl  and price >= sl:  event = "SL"
                elif tp3 and price <= tp3: event = "TP3"
                elif tp2 and price <= tp2: event = "TP2"
                elif tp1 and price <= tp1: event = "TP1"

            if event:
                new_status = "CLOSED" if event in ("SL","TP3") else event
                result     = "WIN" if pnl_pct > 0 else "LOSS"
                try:
                    await _db.update_trade_status(
                        trade_id=trade_id,
                        status=new_status,
                        exit_price=price,
                        pnl_pct=round(pnl_pct, 4),
                    )
                except Exception as e:
                    logger.warning(f"[Monitor] DB: {e}")

                try:
                    await _notifier.send_monitor_alert(
                        symbol, direction, event, price, pnl_pct
                    )
                    logger.info(f"[Monitor] {symbol} {event} @ ${price:.2f} PnL={pnl_pct:+.2f}%")
                except Exception as e:
                    logger.error(f"[Monitor] Telegram: {e}")

    except Exception as e:
        logger.error(f"[Monitor] {e}")


# ══════════════════════════════════════════════════════════════════
# LAYER 1 — Market Intelligence (every 4h)
# ══════════════════════════════════════════════════════════════════
async def job_run_layer1():
    """Layer 1: Wyckoff + Gann + William%R + USDT.D analysis."""
    logger.info("[L1] 🧠 Starting Market Intelligence …")
    try:
        ctx = await run_layer1(_db)

        btc = ctx.get("BTC", {})
        eth = ctx.get("ETH", {})
        macro = ctx.get("macro", {})
        overall = ctx.get("overall_bias", "NEUTRAL")

        msg = (
            f"🧠 <b>Layer 1 — Market Intelligence</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"₿ BTC: <b>{btc.get('bias','?')}</b>"
            f" | Wyckoff: {btc.get('wyckoff',{}).get('phase','?')}\n"
            f"Ξ ETH: <b>{eth.get('bias','?')}</b>"
            f" | Wyckoff: {eth.get('wyckoff',{}).get('phase','?')}\n"
            f"📡 USDT.D: {macro.get('usdt_d','?')}%"
            f" → {macro.get('crypto_bias','?')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏁 Overall: <b>{overall}</b>\n"
            f"⚡ Scalp: {'✅ مسموح' if ctx.get('scalp_allowed') else '🚫 محظور'}\n"
            f"🌊 Swing: {'✅ مسموح' if ctx.get('swing_allowed') else '🚫 محظور'}\n"
            f"<i>🎖️ Ramos 360 Ai — Layer 1</i>"
        )
        await _notifier.send(msg)
        logger.success("[L1] ✅ Done")

    except Exception as e:
        logger.error(f"[L1] {e}")
        try:
            await _notifier.send_error("LAYER1", str(e))
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
# CORE PIPELINE — used by Layer 2 (Swing) and Layer 3 (Scalp)
# ══════════════════════════════════════════════════════════════════
async def _run_pipeline(run_tag: str, trade_type: str):
    """
    Unified signal pipeline with:
    1. Layer 1 context check
    2. Session filter (Scalp only)
    3. Gemini pre-filter (Scalp only)
    4. Signal generation with ATR dynamic SL
    5. AI confirmation
    6. Telegram + DB
    """
    if is_circuit_open():
        logger.warning(f"[{run_tag}] Circuit breaker OPEN — skipping")
        return

    run_id  = make_run_id(run_tag)
    session = get_session()
    is_scalp = trade_type in ("Scalp", "QuickScalp")

    # ── Step 1: Layer 1 Context Check ────────────────────────────
    l1_ctx = get_cached_context()
    if l1_ctx:
        overall_bias = l1_ctx.get("overall_bias", "NEUTRAL")
        scalp_ok     = l1_ctx.get("scalp_allowed", True)
        swing_ok     = l1_ctx.get("swing_allowed", True)

        if is_scalp and not scalp_ok:
            logger.info(f"[{run_tag}] ⛔ Layer 1 blocked scalp — bias=NEUTRAL")
            return

        if not is_scalp and not swing_ok:
            logger.info(f"[{run_tag}] ⛔ Layer 1 blocked swing")
            return

        logger.info(f"[{run_tag}] Layer 1 OK — bias={overall_bias}")
    else:
        logger.info(f"[{run_tag}] No L1 cache — proceeding without filter")

    # ── Step 2: Session Filter (Scalp only) ──────────────────────
    if is_scalp:
        ok, reason = is_session_allowed(trade_type)
        if not ok:
            logger.info(f"[{run_tag}] 🕐 {reason}")
            return
        q_score = session_quality_score()
        logger.info(f"[{run_tag}] Session OK — quality={q_score}")
    else:
        q_score = 1.0

    # ── Step 3: Gemini Pre-Filter (Scalp only) ───────────────────
    if is_scalp and Secrets.has_gemini():
        try:
            btc_price = await _fetcher.get_live_price("BTC/USDT:USDT")
            eth_price = await _fetcher.get_live_price("ETH/USDT:USDT")
            btc_1d    = await _fetcher.get_candles("BTC/USDT:USDT", "1d", 14)
            eth_1d    = await _fetcher.get_candles("ETH/USDT:USDT", "1d", 14)
            gm = await gemini_market_check(btc_price, eth_price,
                                            btc_1d or [], eth_1d or [])
            if not gm.get("ok"):
                logger.info(f"[{run_tag}] 🤖 Gemini blocked scalp: {gm.get('reason')}")
                return
            logger.info(f"[{run_tag}] 🤖 Gemini: {gm.get('answer')} — {gm.get('reason')}")
        except Exception as e:
            logger.warning(f"[{run_tag}] Gemini pre-filter error: {e} — proceeding")

    # ── Step 4: Fetch Data ────────────────────────────────────────
    logger.info(f"[{run_tag}] Starting — session={session['name']} run_id={run_id}")
    assets_data = await _fetcher.fetch_all_assets()

    # Balance
    balance = await _fetcher.get_balance()
    if balance <= 0:
        balance = 1000.0 if not Secrets.has_okx() else balance
        if balance <= 0:
            logger.warning(f"[{run_tag}] Balance=0 — skipping")
            return

    # Enrich asset data
    try:
        fear_greed = await _fetcher.get_fear_greed()
    except Exception:
        fear_greed = {}

    for sym, d in assets_data.items():
        if d:
            d["fear_greed"]     = fear_greed
            d["usdt_dominance"] = {}
            d["trade_type"]     = trade_type
            d["l1_context"]     = l1_ctx or {}
            try:
                d["funding"] = await _fetcher.get_funding_rate(sym)
            except Exception:
                d["funding"] = {}

    open_trades = _db.get_open_trades()
    signals     = run_all_assets(assets_data, balance, open_trades, run_id)
    logger.info(f"[{run_tag}] {len(signals)} raw signals generated")

    if not signals:
        logger.info(f"[{run_tag}] No signals this cycle")
        return

    # ── Step 5: Apply ATR Dynamic SL + Session Quality ───────────
    enriched = []
    for sig in signals:
        symbol = sig.get("symbol","")
        d      = assets_data.get(symbol, {})
        c4h    = d.get("c4h") if d else None

        if c4h and len(c4h) >= 15:
            price  = sig.get("entry", 0)
            tt     = sig.get("trade_type", trade_type)
            levels = calc_dynamic_sl_tp(
                price=price,
                direction=sig.get("direction","LONG"),
                candles_4h=list(reversed(c4h)) if c4h else [],
                trade_type=tt,
            )
            valid, reason = validate_levels(
                price, sig.get("direction","LONG"),
                levels["sl"], levels["tp1"],
                min_rr=0.8,
            )
            if not valid:
                logger.debug(f"[{run_tag}] {symbol} invalid levels: {reason}")
                continue

            sig.update({
                "sl":  levels["sl"],
                "tp1": levels["tp1"],
                "tp2": levels["tp2"],
                "tp3": levels["tp3"],
                "atr": levels["atr"],
                "rr":  levels["rr"],
            })

        # Boost score by session quality
        sig["score"] = round(sig.get("score", 0) * q_score, 4)
        enriched.append(sig)

    if not enriched:
        logger.info(f"[{run_tag}] All signals filtered by ATR/RR check")
        return

    # ── Step 6: AI Confirmation + Send ───────────────────────────
    sent = 0
    for sig in enriched:
        try:
            ai_ans = await confirm_signal(sig)
        except Exception:
            ai_ans = "SKIP"
        sig["ai_confirmation"] = ai_ans

        if ai_ans == "NO":
            logger.info(f"[AI] Rejected: {sig['symbol']} {sig['direction']}")
            continue

        # Telegram
        try:
            await _notifier.send_signal(sig)
            sent += 1
        except Exception as e:
            logger.error(f"[{run_tag}] Telegram: {e}")
            continue

        # DB: signal log
        try:
            await _db.log_signal(sig)
        except Exception as e:
            logger.warning(f"[{run_tag}] log_signal: {e}")

        # DB: open trade
        try:
            await _db.log_trade({
                "symbol":      sig["symbol"],
                "direction":   sig["direction"],
                "trade_type":  sig.get("trade_type", trade_type),
                "status":      "OPEN",
                "entry_price": sig["entry"],
                "sl_price":    sig["sl"],
                "tp1_price":   sig["tp1"],
                "tp2_price":   sig["tp2"],
                "tp3_price":   sig["tp3"],
                "exit_price":  None,
                "pnl_pct":     0,
                "size_usdt":   sig.get("size_usdt"),
                "run_id":      run_id,
            })
        except Exception as e:
            logger.warning(f"[{run_tag}] log_trade: {e}")

        await asyncio.sleep(0.5)

    logger.info(f"[{run_tag}] ✅ Done — {sent}/{len(enriched)} sent")


# ══════════════════════════════════════════════════════════════════
# PUBLIC JOB FUNCTIONS
# ══════════════════════════════════════════════════════════════════

async def job_run_scalp():
    """Layer 3: Session-filtered + Gemini-checked scalp."""
    await _run_pipeline("SCALP", "Scalp")

async def job_run_swing():
    """Layer 2: Swing trading (no session restriction)."""
    await _run_pipeline("SWING", "Swing")

async def job_run_super_swing():
    """Layer 2+: Super swing — no time/session restrictions."""
    await _run_pipeline("SUPER_SWING", "SuperSwing")


# ══════════════════════════════════════════════════════════════════
# DAILY ANALYSIS (E10 + E11 + Layer 1)
# ══════════════════════════════════════════════════════════════════
async def job_daily_market():
    logger.info("[Daily] 📊 Starting …")
    try:
        from engine.analysis_engine import run_full_analysis
        await run_full_analysis(_db, _notifier, _fetcher)
        # Also run Layer 1 at midnight
        await run_layer1(_db)
        await _db.heartbeat(CONFIG.VERSION, CONFIG.ASSETS)
        logger.success("[Daily] ✅ Done")
    except Exception as e:
        logger.error(f"[Daily] {e}")
        try:
            await _notifier.send_error("DAILY", str(e))
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════
# WEEKLY REPORT
# ══════════════════════════════════════════════════════════════════
async def job_weekly_report():
    try:
        open_t    = _db.get_open_trades()
        daily_pnl = _db.get_daily_pnl()
        msg = (
            f"📋 <b>Weekly Report — {CONFIG.NAME}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 Trades open:  {len(open_t)}\n"
            f"💰 Weekly PnL:   {daily_pnl:+.2f}%\n"
            f"📊 Assets:       {len(CONFIG.ASSETS)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>🎖️ {CONFIG.NAME}</i>"
        )
        await _notifier.send(msg)
    except Exception as e:
        logger.error(f"[Weekly] {e}")


# ══════════════════════════════════════════════════════════════════
# SELF LEARN
# ══════════════════════════════════════════════════════════════════
async def job_self_learn():
    logger.info("[SelfLearn] 🧠 Starting …")
    try:
        from strategies import EXPERT_NAMES, EXPERT_WEIGHTS
        closed = _db.get_closed_trades_last_week()
        if not closed:
            await _notifier.send(
                "🧠 <b>Self Learn</b>\nلا توجد صفقات مغلقة هذا الأسبوع."
            )
            return

        scores = {n: {"w": 0, "l": 0} for n in EXPERT_NAMES}
        for t in closed:
            pnl  = float(t.get("pnl_pct", 0) or 0)
            exps = t.get("experts_fired", []) or []
            key  = "w" if pnl > 0 else "l"
            for e in exps:
                if e in scores: scores[e][key] += 1

        weights   = list(EXPERT_WEIGHTS)
        lines     = ["🧠 <b>Self Learn</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        for i, name in enumerate(EXPERT_NAMES):
            s    = scores.get(name, {"w": 0, "l": 0})
            tot  = s["w"] + s["l"]
            if tot == 0: lines.append(f"  ➡️ {name}: no data"); continue
            wr   = s["w"] / tot
            old  = weights[i] if i < len(weights) else 1.0
            if wr >= 0.60:   weights[i] = round(min(old*1.10, 2.5), 2); tag="⬆️"
            elif wr <= 0.40: weights[i] = round(max(old*0.90, 0.3), 2); tag="⬇️"
            else:            tag = "➡️"
            lines.append(f"  {tag} {name}: {wr:.0%} ({s['w']}W/{s['l']}L) "
                          f"{old}→{weights[i]}")

        await _db.log_performance({"symbol":"SELF_LEARN",
            "wins":  sum(s["w"] for s in scores.values()),
            "losses":sum(s["l"] for s in scores.values()),
            "total": len(closed),
            "win_rate":0.0,"total_pnl":0.0,"max_dd":0.0})
        await _notifier.send("\n".join(lines))
        logger.success("[SelfLearn] ✅ Done")
    except Exception as e:
        logger.error(f"[SelfLearn] {e}")
        try: await _notifier.send_error("SELF_LEARN", str(e))
        except Exception: pass
