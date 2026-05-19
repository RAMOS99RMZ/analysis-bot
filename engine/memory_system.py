
# engine/memory_system.py — نظام الذاكرة الذكية
# يحفظ ويسترجع أداء كل عملة وجلسة لتحسين قرارات البوت
from __future__ import annotations
import json, time
from collections import defaultdict
from typing import Dict, List, Optional
from loguru import logger

# ── ذاكرة في الـ RAM (تُحمَّل من Supabase عند البدء) ─────────────────
_MEMORY: Dict = defaultdict(lambda: {
    "signals": [], "wins": 0, "losses": 0,
    "session_wins": defaultdict(int),
    "session_losses": defaultdict(int),
    "best_hours": defaultdict(int),
})

_LOADED = False


class MemorySystem:
    """
    يتذكر البوت أداءه السابق لكل عملة وجلسة.
    يُعدّل ثقة الإشارة بناءً على التاريخ.
    """

    def __init__(self, db=None):
        self._db = db

    async def load(self) -> None:
        """تحميل الذاكرة من Supabase عند بدء البوت."""
        global _LOADED
        if _LOADED or not self._db:
            return
        try:
            res = self._db.client.table("trades") \
                      .select("symbol,direction,pnl_pct,created_at") \
                      .eq("status", "CLOSED") \
                      .order("created_at", desc=True) \
                      .limit(500).execute()
            for row in (res.data or []):
                sym  = row.get("symbol", "")
                pnl  = float(row.get("pnl_pct") or 0)
                hour = 12  # default if no timestamp parsing
                try:
                    from datetime import datetime
                    dt   = datetime.fromisoformat(row["created_at"].replace("Z",""))
                    hour = dt.hour
                    session = _hour_to_session(hour)
                except Exception:
                    session = "Unknown"
                mem = _MEMORY[sym]
                if pnl > 0:
                    mem["wins"] += 1
                    mem["session_wins"][session] += 1
                    mem["best_hours"][hour] += 1
                else:
                    mem["losses"] += 1
                    mem["session_losses"][session] += 1
            _LOADED = True
            logger.info(f"[Memory] ✅ تم تحميل ذاكرة {len(_MEMORY)} عملة")
        except Exception as e:
            logger.warning(f"[Memory] load error: {e}")

    def record(self, symbol: str, direction: str, pnl_pct: float, hour: int = 12) -> None:
        """تسجيل نتيجة صفقة في الذاكرة."""
        session = _hour_to_session(hour)
        mem = _MEMORY[symbol]
        if pnl_pct > 0:
            mem["wins"] += 1
            mem["session_wins"][session] += 1
            mem["best_hours"][hour] += 1
        else:
            mem["losses"] += 1
            mem["session_losses"][session] += 1

    def get_confidence_multiplier(self, symbol: str, hour: int = 12) -> float:
        """
        يُرجع مضاعف الثقة بناءً على التاريخ (0.7 → 1.3).
        عملة فوزها عالي في هذه الجلسة → ثقة أعلى.
        """
        mem = _MEMORY.get(symbol)
        if not mem:
            return 1.0

        total = mem["wins"] + mem["losses"]
        if total < 10:
            return 1.0   # بيانات غير كافية

        overall_wr = mem["wins"] / total
        session    = _hour_to_session(hour)
        sw = mem["session_wins"].get(session, 0)
        sl = mem["session_losses"].get(session, 0)
        session_wr = sw / max(sw + sl, 1)

        # مزج: 60% إجمالي + 40% جلسة
        wr = overall_wr * 0.6 + session_wr * 0.4

        if wr >= 0.70:   return 1.30
        elif wr >= 0.60: return 1.15
        elif wr >= 0.50: return 1.00
        elif wr >= 0.40: return 0.85
        else:            return 0.70

    def get_summary(self, symbol: str) -> Dict:
        """ملخص أداء عملة معينة."""
        mem = _MEMORY.get(symbol, {})
        total = mem.get("wins", 0) + mem.get("losses", 0)
        wr    = mem.get("wins", 0) / max(total, 1) * 100
        best_session = max(
            mem.get("session_wins", {}).items(),
            key=lambda x: x[1], default=("—", 0)
        )[0]
        return {
            "symbol": symbol, "total": total,
            "wins": mem.get("wins", 0), "losses": mem.get("losses", 0),
            "win_rate": round(wr, 1), "best_session": best_session,
        }

    def get_all_summaries(self) -> List[Dict]:
        return [self.get_summary(sym) for sym in _MEMORY]


def _hour_to_session(hour: int) -> str:
    if 0 <= hour < 8:   return "Asia"
    elif 8 <= hour < 13: return "London"
    elif 13 <= hour < 17: return "NY+London"
    elif 17 <= hour < 21: return "NY"
    else:                return "After"


# singleton
MEMORY = MemorySystem()
