
# engine/correlation_matrix.py — مصفوفة الارتباط بين الأصول
from __future__ import annotations
from typing import Dict, List, Optional, Tuple

_CLOSES_CACHE: Dict[str, List[float]] = {}


def update_closes(symbol: str, candles: List) -> None:
    """تحديث أسعار الإغلاق في الكاش."""
    if candles and len(candles) >= 20:
        _CLOSES_CACHE[symbol] = [float(c[4]) for c in candles[:30]]


def pearson_corr(a: List[float], b: List[float], n: int = 20) -> float:
    """حساب ارتباط Pearson بين عملتين."""
    try:
        n = min(n, len(a), len(b))
        if n < 10: return 1.0
        ax = a[:n]; bx = b[:n]
        am = sum(ax)/n; bm = sum(bx)/n
        num   = sum((ax[i]-am)*(bx[i]-bm) for i in range(n))
        den_a = sum((ax[i]-am)**2 for i in range(n))**0.5
        den_b = sum((bx[i]-bm)**2 for i in range(n))**0.5
        if den_a == 0 or den_b == 0: return 1.0
        return round(max(-1.0, min(1.0, num/(den_a*den_b))), 3)
    except:
        return 1.0


def check_btc_alignment(symbol: str, direction: str) -> Dict:
    """
    يفحص هل الإشارة على alt تتوافق مع اتجاه BTC.
    إذا BTC SHORT وأنت تريد LONG على SOL → رفض.
    """
    btc_key  = "BTC/USDT:USDT"
    sym_cls  = _CLOSES_CACHE.get(symbol, [])
    btc_cls  = _CLOSES_CACHE.get(btc_key, [])

    if not btc_cls or len(btc_cls) < 10:
        return {"aligned": True, "corr": 1.0,
                "reason": "لا بيانات BTC — تجاوز الفحص"}

    # هل BTC في اتجاه؟
    btc_trend = "BULL" if btc_cls[0] > btc_cls[4] else "BEAR"

    # ارتباط مع BTC
    corr = pearson_corr(sym_cls, btc_cls) if sym_cls else 1.0

    # عملة مرتبطة جداً (corr > 0.7) ومعاكسة للبوت → رفض
    if corr >= 0.70:
        if direction == "LONG" and btc_trend == "BEAR":
            return {
                "aligned": False, "corr": corr,
                "reason": f"BTC هابط (corr={corr:.2f}) — LONG على {symbol} خطر"
            }
        if direction == "SHORT" and btc_trend == "BULL":
            return {
                "aligned": False, "corr": corr,
                "reason": f"BTC صاعد (corr={corr:.2f}) — SHORT على {symbol} خطر"
            }

    return {
        "aligned": True, "corr": corr,
        "btc_trend": btc_trend,
        "reason": f"BTC {btc_trend} | corr={corr:.2f} ✅"
    }


def get_matrix_report() -> str:
    """تقرير مصفوفة الارتباط لكل العملات."""
    btc_cls = _CLOSES_CACHE.get("BTC/USDT:USDT", [])
    if not btc_cls:
        return "لا بيانات كافية"

    lines = ["📡 <b>مصفوفة الارتباط مع BTC:</b>"]
    for sym, cls in _CLOSES_CACHE.items():
        if sym == "BTC/USDT:USDT": continue
        sym_c = sym.replace("/USDT:USDT", "")
        corr  = pearson_corr(cls, btc_cls)
        emoji = "🟢" if corr > 0.7 else "🟡" if corr > 0.4 else "🔴"
        lines.append(f"  {emoji} {sym_c}: {corr:.2f}")
    return "\n".join(lines)
