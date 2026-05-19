
# engine/weekly_backtest.py — Backtest تلقائي أسبوعي
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from loguru import logger


class WeeklyBacktest:
    """
    يُشغّل backtest على آخر 30 يوم من Supabase.
    يُرسل تقرير كل أحد عبر Telegram.
    """

    def __init__(self, db=None):
        self._db = db

    async def run(self) -> Dict:
        """تشغيل الـ backtest وإرجاع النتائج."""
        try:
            # آخر 30 يوم
            since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

            res = self._db.client.table("trades") \
                      .select("*") \
                      .eq("status", "CLOSED") \
                      .gte("created_at", since) \
                      .order("created_at", desc=True) \
                      .limit(200).execute()

            trades = res.data or []
            return self._analyze(trades)

        except Exception as e:
            logger.error(f"[Backtest] Error: {e}")
            return {}

    def _analyze(self, trades: List[Dict]) -> Dict:
        if not trades:
            return {"total": 0, "win_rate": 0, "pnl": 0}

        total  = len(trades)
        wins   = [t for t in trades if (t.get("pnl_pct") or 0) > 0]
        losses = [t for t in trades if (t.get("pnl_pct") or 0) <= 0]

        win_rate   = len(wins) / total * 100
        total_pnl  = sum(t.get("pnl_pct", 0) for t in trades)
        avg_win    = sum(t.get("pnl_pct",0) for t in wins)    / max(len(wins),1)
        avg_loss   = sum(t.get("pnl_pct",0) for t in losses)  / max(len(losses),1)
        pf = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        # أداء كل عملة
        sym_stats: Dict = {}
        for t in trades:
            sym = t.get("symbol","?").replace("/USDT:USDT","")
            if sym not in sym_stats:
                sym_stats[sym] = {"wins":0,"total":0,"pnl":0}
            sym_stats[sym]["total"] += 1
            pnl = t.get("pnl_pct",0) or 0
            sym_stats[sym]["pnl"] += pnl
            if pnl > 0:
                sym_stats[sym]["wins"] += 1

        best = sorted(sym_stats.items(),
                      key=lambda x: x[1]["wins"]/max(x[1]["total"],1),
                      reverse=True)[:3]

        # Max Drawdown بسيط
        pnls  = [t.get("pnl_pct",0) or 0 for t in trades]
        peak  = max(pnls) if pnls else 0
        trough= min(pnls) if pnls else 0
        max_dd = abs(peak - trough)

        return {
            "total": total, "wins": len(wins), "losses": len(losses),
            "win_rate": round(win_rate, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(pf, 2),
            "max_dd": round(max_dd, 2),
            "best_assets": best,
        }

    def format_report(self, result: Dict, config_name: str) -> str:
        if not result or not result.get("total"):
            return "📊 <b>Backtest</b>\nلا بيانات كافية (أقل من 30 يوم)"

        total  = result["total"]
        wr     = result["win_rate"]
        pnl    = result["total_pnl"]
        pf     = result["profit_factor"]
        dd     = result["max_dd"]
        avg_w  = result["avg_win"]
        avg_l  = result["avg_loss"]
        best   = result.get("best_assets", [])

        # تقييم الأداء
        if wr >= 65 and pf >= 2.0:
            grade = "🏆 ممتاز"
        elif wr >= 55 and pf >= 1.5:
            grade = "✅ جيد"
        elif wr >= 50:
            grade = "🟡 متوسط"
        else:
            grade = "🔴 يحتاج تحسين"

        best_str = "\n".join(
            f"  • {sym}: {int(v['wins']/max(v['total'],1)*100)}%"
            f" ({v['wins']}/{v['total']}) | P&L: {v['pnl']:+.1f}%"
            for sym, v in best
        ) or "  —"

        return (
            f"📊 <b>Backtest أسبوعي — {config_name}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>آخر 30 يوم:</b>\n"
            f"  🔢 إجمالي: {total} صفقة\n"
            f"  ✅ فوز: {result['wins']} | ❌ خسارة: {result['losses']}\n"
            f"  🎯 نسبة الفوز: <b>{wr}%</b> {grade}\n"
            f"  💰 P&L الكلي: <b>{pnl:+.2f}%</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"  📈 متوسط الفوز: +{avg_w:.2f}%\n"
            f"  📉 متوسط الخسارة: {avg_l:.2f}%\n"
            f"  ⚖️ Profit Factor: {pf}\n"
            f"  🔻 Max Drawdown: {dd:.2f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏅 <b>أفضل الأصول:</b>\n{best_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>🔄 تقرير تلقائي كل أحد — Ramos Ai 360 ♾️</i>"
        )


BACKTEST = WeeklyBacktest()
