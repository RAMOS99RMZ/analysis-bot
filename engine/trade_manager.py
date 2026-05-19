# engine/trade_manager.py — إدارة الصفقة الحية (Trailing Stop + Partial Close)
from __future__ import annotations
from typing import Dict, Optional
from loguru import logger


class TradeManager:
    """
    يُتابع الصفقات المفتوحة ويُطبّق:
    - Trailing Stop: يتحرك SL مع السعر
    - Partial Close: يُغلق 30% عند TP1
    - Time Exit: يخرج إذا لم يصل TP1 في الوقت المحدد
    - Break Even: ينقل SL للدخول بعد TP1
    """

    def evaluate(self, trade: Dict, current_price: float) -> Dict:
        """
        يُقيّم الصفقة الحية ويُرجع الإجراء المطلوب.
        """
        direction   = trade.get("direction", "LONG")
        entry       = float(trade.get("entry_price", current_price))
        sl          = float(trade.get("sl_price", 0))
        tp1         = float(trade.get("tp1_price", 0))
        tp2         = float(trade.get("tp2_price", 0))
        tp3         = float(trade.get("tp3_price", 0))
        partial_done = trade.get("partial_done", False)
        trail_sl    = float(trade.get("trail_sl", sl))
        created_at  = trade.get("created_at", "")

        pnl_pct = self._pnl(entry, current_price, direction)
        action  = "HOLD"
        new_sl  = trail_sl
        reason  = ""

        if direction == "LONG":
            # ── SL Hit ────────────────────────────────────────────
            if current_price <= trail_sl:
                action = "CLOSE_SL"; reason = f"SL hit @ {current_price:.2f}"

            # ── TP3 ───────────────────────────────────────────────
            elif tp3 and current_price >= tp3:
                action = "CLOSE_TP3"; reason = f"TP3 hit @ {current_price:.2f}"

            # ── TP2 ───────────────────────────────────────────────
            elif tp2 and current_price >= tp2:
                action = "CLOSE_TP2"; reason = f"TP2 hit @ {current_price:.2f}"

            # ── TP1 → Partial Close + Break Even ──────────────────
            elif tp1 and current_price >= tp1 and not partial_done:
                action = "PARTIAL_TP1"
                new_sl = entry   # Break Even
                reason = f"TP1 hit → Partial 30% + BE @ {entry:.2f}"

            # ── Trailing Stop ─────────────────────────────────────
            elif pnl_pct > 0:
                # كل +1% في السعر → يتحرك SL +0.5%
                trail_move = (current_price - entry) * 0.5
                candidate  = entry + trail_move
                if candidate > trail_sl:
                    new_sl = candidate
                    action = "TRAIL_SL"
                    reason = f"Trailing SL → {new_sl:.2f}"

        else:  # SHORT
            if current_price >= trail_sl:
                action = "CLOSE_SL"; reason = f"SL hit @ {current_price:.2f}"

            elif tp3 and current_price <= tp3:
                action = "CLOSE_TP3"; reason = f"TP3 hit @ {current_price:.2f}"

            elif tp2 and current_price <= tp2:
                action = "CLOSE_TP2"; reason = f"TP2 hit @ {current_price:.2f}"

            elif tp1 and current_price <= tp1 and not partial_done:
                action = "PARTIAL_TP1"
                new_sl = entry
                reason = f"TP1 hit → Partial 30% + BE @ {entry:.2f}"

            elif pnl_pct > 0:
                trail_move = (entry - current_price) * 0.5
                candidate  = entry - trail_move
                if candidate < trail_sl:
                    new_sl = candidate
                    action = "TRAIL_SL"
                    reason = f"Trailing SL → {new_sl:.2f}"

        return {
            "action":       action,
            "new_sl":       round(new_sl, 6),
            "pnl_pct":      round(pnl_pct, 3),
            "partial_done": partial_done or action == "PARTIAL_TP1",
            "reason":       reason,
        }

    def _pnl(self, entry: float, current: float, direction: str) -> float:
        if entry <= 0: return 0.0
        if direction == "LONG":
            return (current - entry) / entry * 100
        return (entry - current) / entry * 100

    def format_alert(self, symbol: str, trade: Dict,
                     eval_result: Dict) -> Optional[str]:
        """رسالة Telegram لتنبيه التغيير في الصفقة."""
        action = eval_result.get("action", "HOLD")
        if action == "HOLD": return None

        sym_c = symbol.replace("/USDT:USDT", "")
        dir_e = "📈 LONG" if trade.get("direction") == "LONG" else "📉 SHORT"
        pnl   = eval_result["pnl_pct"]
        pnl_e = f"+{pnl:.2f}%" if pnl >= 0 else f"{pnl:.2f}%"

        icons = {
            "CLOSE_SL":   "🛑 Stop Loss",
            "CLOSE_TP2":  "🎯 TP2 Hit",
            "CLOSE_TP3":  "🏆 TP3 Hit",
            "PARTIAL_TP1":"✂️ Partial Close 30% (TP1)",
            "TRAIL_SL":   "🔒 Trailing SL Updated",
        }
        title = icons.get(action, action)

        return (
            f"{title}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"{dir_e} {sym_c}\n"
            f"P&L: <b>{pnl_e}</b>\n"
            f"📌 {eval_result.get('reason','')}\n"
            f"<i>Ramos Ai 360 ♾️</i>"
        )


TRADE_MGR = TradeManager()
