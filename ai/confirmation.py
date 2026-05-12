"""
ai/confirmation.py — Ramos 360 Ai 🎖️
Grok (primary) + Gemini (fallback) with free-tier rate limiting.
Grok free:   ~30 req/min  | Gemini Flash free: 15 req/min
We cap at 25/hr Grok, 12/hr Gemini to stay safely within limits.
"""
from __future__ import annotations
import time
from typing import Dict
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from config import Secrets, CONFIG

# ── Rate limit state (in-memory, per process) ─────────────────────────────────
_GROQ_CALLS:   list = []   # timestamps
_GEMINI_CALLS: list = []


def _can_call_groq() -> bool:
    now = time.time()
    _GROQ_CALLS[:] = [t for t in _GROQ_CALLS if now - t < 3600]
    return len(_GROQ_CALLS) < CONFIG.GROQ_MAX_PER_HOUR

def _can_call_gemini() -> bool:
    now = time.time()
    _GEMINI_CALLS[:] = [t for t in _GEMINI_CALLS if now - t < 3600]
    return len(_GEMINI_CALLS) < CONFIG.GEMINI_MAX_PER_HOUR

def _mark_groq()   -> None: _GROQ_CALLS.append(time.time())
def _mark_gemini() -> None: _GEMINI_CALLS.append(time.time())


def _prompt(signal: Dict) -> str:
    sym   = signal.get("symbol","?").replace("/USDT:USDT","")
    dir_  = signal.get("direction","?")
    score = signal.get("score", 0)
    votes = signal.get("expert_votes", 0)
    entry = signal.get("entry", 0)
    sl    = signal.get("sl", 0)
    tp2   = signal.get("tp2", 0)
    rr    = signal.get("rr", 0)
    reg   = signal.get("regime", "?")
    exps  = ", ".join(signal.get("experts_fired", []))
    return (
        f"You are a professional crypto quant. Review this signal briefly.\n"
        f"Asset: {sym} | Direction: {dir_} | Score: {score:.3f} | "
        f"Votes: {votes}/14 | RR: {rr:.2f}\n"
        f"Entry: {entry} | SL: {sl} | TP2: {tp2} | Regime: {reg}\n"
        f"Experts voted: {exps}\n\n"
        f"Reply with ONE word only: YES or NO."
    )


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5), reraise=False)
async def _ask_groq(prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    async with httpx.AsyncClient(timeout=12) as cl:
        r = await cl.post(url,
            headers={"Authorization": f"Bearer {Secrets.GROQ_KEY}"},
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 5, "temperature": 0})
        if r.status_code == 200:
            txt = r.json()["choices"][0]["message"]["content"].strip().upper()
            return "YES" if "YES" in txt else "NO"
        if r.status_code == 429:
            logger.warning("[AI/Grok] Rate limited (429)")
            return "SKIP"
    return "SKIP"


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5), reraise=False)
async def _ask_gemini(prompt: str) -> str:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"gemini-1.5-flash:generateContent?key={Secrets.GEMINI_KEY}")
    async with httpx.AsyncClient(timeout=12) as cl:
        r = await cl.post(url, json={"contents": [{"parts": [{"text": prompt}]}]})
        if r.status_code == 200:
            txt = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip().upper()
            return "YES" if "YES" in txt else "NO"
        if r.status_code == 429:
            logger.warning("[AI/Gemini] Rate limited (429)")
            return "SKIP"
    return "SKIP"


async def confirm_signal(signal: Dict) -> str:
    """
    Get AI confirmation. Returns: 'YES' | 'NO' | 'SKIP'
    Grok is primary; Gemini is fallback.
    Both respect free-tier hourly limits.
    """
    if not Secrets.has_groq() and not Secrets.has_gemini():
        return "SKIP"

    prompt = _prompt(signal)
    sym    = signal.get("symbol","?")

    # Try Grok first
    if Secrets.has_groq() and _can_call_groq():
        try:
            ans = await _ask_groq(prompt)
            _mark_groq()
            logger.info(f"[AI/Grok] {sym}: {ans} (used {len(_GROQ_CALLS)}/{CONFIG.GROQ_MAX_PER_HOUR} hr)")
            if ans != "SKIP":
                return ans
        except Exception as e:
            logger.warning(f"[AI/Grok] {e}")
    else:
        logger.debug(f"[AI/Grok] quota full or no key")

    # Fallback to Gemini
    if Secrets.has_gemini() and _can_call_gemini():
        try:
            ans = await _ask_gemini(prompt)
            _mark_gemini()
            logger.info(f"[AI/Gemini] {sym}: {ans} (used {len(_GEMINI_CALLS)}/{CONFIG.GEMINI_MAX_PER_HOUR} hr)")
            return ans
        except Exception as e:
            logger.warning(f"[AI/Gemini] {e}")

    logger.info(f"[AI] Both unavailable for {sym} — SKIP")
    return "SKIP"
