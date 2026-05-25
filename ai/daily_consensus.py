
# ai/daily_consensus.py — Gemini Daily Analysis (مرة يومياً فقط)
# ══════════════════════════════════════════════════════════════════════
# يستدعي Gemini مرة واحدة يومياً (أو عند تغير شمعة 4H الكبرى)
# يُمرّر نتائج E10 + E11 كـ JSON ويطلب رأي الخبير
# يُخزّن النتيجة في market_state لاستخدامها طوال اليوم
# ══════════════════════════════════════════════════════════════════════
from __future__ import annotations
import json
from typing import Dict, Optional
from loguru import logger
from config import Secrets
from engine.market_state import (get_state, update_gemini,
                                  gemini_needs_update)


_GEMINI_PROMPT_TEMPLATE = """أنت خبير تداول متخصص في التحليل الفني الكمي.

فيما يلي نتائج نظام التحليل الآلي لأسواق العملات الرقمية:

**E10 (تحليل BTC/ETH - مدير السوق الرئيسي):**
{e10_json}

**E11 (تحليل USDT.D / BTC.D - مؤشر السيولة):**
{e11_json}

**الاتجاه المحسوب تلقائياً:** {bias}
**نوع التداول المقترح:** {trade_type}

المطلوب منك:
1. هل يتوافق تحليل SMC مع إشارات Gann؟ وما التقاطع بينهما؟
2. بناءً على USDT.D و BTC.D، هل السوق في مرحلة تراكم أم توزيع؟
3. توصيتك النهائية في سطر واحد: Long / Short / Neutral + السبب الرئيسي.

أجب بشكل مختصر ومباشر (3 أسطر بالعربية)."""


async def run_daily_consensus(notifier=None) -> Optional[str]:
    """
    يُشغّل Gemini مرة يومياً فقط.
    يُرجع رأي Gemini كنص أو None.
    """
    if not gemini_needs_update():
        logger.debug("[DailyConsensus] Gemini up-to-date, skipping")
        return get_state().get("gemini")

    if not Secrets.has_gemini():
        logger.debug("[DailyConsensus] No Gemini key")
        return None

    state = get_state()
    e10   = state.get("e10") or {}
    e11   = state.get("e11") or {}

    if not e10 and not e11:
        logger.debug("[DailyConsensus] No E10/E11 data yet")
        return None

    # تنظيف البيانات للـ prompt (حذف البيانات الثقيلة)
    e10_clean = {
        k: v for k, v in e10.items()
        if k in ("long", "short", "trade_type", "atr_label", "why")
    }
    e11_clean = {
        k: v for k, v in e11.items()
        if k in ("long", "short", "usdt_level", "usdt_trend", "btc_d", "why")
    }

    prompt = _GEMINI_PROMPT_TEMPLATE.format(
        e10_json   = json.dumps(e10_clean, ensure_ascii=False, indent=2),
        e11_json   = json.dumps(e11_clean, ensure_ascii=False, indent=2),
        bias       = state.get("bias", "NEUTRAL"),
        trade_type = state.get("trade_type", "SCALP+SWING"),
    )

    opinion = await _call_gemini(prompt)
    if opinion:
        update_gemini(opinion)
        logger.success(f"[DailyConsensus] ✅ Gemini: {opinion[:80]}…")

        # إرسال على Telegram إذا كان Notifier متاحاً
        if notifier:
            msg = (
                f"🧠 <b>تحليل Gemini اليومي</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{opinion}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>🎖️ Ramos 360 Ai — Daily Consensus</i>"
            )
            try:
                await notifier.send(msg)
            except Exception:
                pass

    return opinion


async def _call_gemini(prompt: str) -> Optional[str]:
    """استدعاء Gemini Flash API."""
    try:
        import httpx
        api_key = Secrets.GEMINI_KEY
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-1.5-flash:generateContent?key={api_key}"
        )
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": 200,
                "topP": 0.8,
            }
        }
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(url, json=body)
            if r.status_code != 200:
                logger.warning(f"[Gemini] HTTP {r.status_code}")
                return None
            data = r.json()
            text = (data.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", ""))
            return text.strip() if text else None
    except Exception as e:
        logger.warning(f"[Gemini] Error: {e}")
        return None
