# ai/confirmation.py — AI Trade Confirmation (Grok + Gemini)
# Mirrors: groqConfirmTrade() + geminiConfirmTrade() in GAS

from __future__ import annotations
import json
from typing import Dict, Optional
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from config import Secrets

_GROQ_URL   = "https://api.groq.com/openai/v1/chat/completions"
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

def _build_prompt(signal: Dict) -> str:
    sym   = signal.get("symbol","?").replace("/USDT:USDT","")
    dir_  = signal.get("direction","?")
    score = signal.get("score",0)
    votes = signal.get("expert_votes",0)
    mtf   = signal.get("mtf_score",0)
    entry = signal.get("entry",0)
    sl    = signal.get("sl",0)
    tp2   = signal.get("tp2",0)
    rr    = signal.get("rr",0)
    reg   = signal.get("regime","?")
    exp   = ", ".join(signal.get("experts_fired",[]))
    return (
        f"You are a professional crypto futures trader reviewing a trade signal.\n"
        f"Symbol: {sym} | Direction: {dir_} | Trade Type: {signal.get('trade_type','?')}\n"
        f"Score: {score:.3f}/1.0 | Expert Votes: {votes}/11 | MTF: {mtf:.3f}\n"
        f"Entry: {entry} | SL: {sl} | TP2: {tp2} | RR: {rr:.2f}\n"
        f"Market Regime: {reg}\n"
        f"Experts agreed: {exp}\n\n"
        f"Based ONLY on the data above, reply with ONE word: YES or NO.\n"
        f"YES = confirm the trade. NO = reject it."
    )

@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5), reraise=False)
async def _ask_groq(prompt: str) -> Optional[str]:
    if not Secrets.GROQ_KEY: return None
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.post(
            _GROQ_URL,
            headers={"Authorization": f"Bearer {Secrets.GROQ_KEY}"},
            json={"model": "llama-3.3-70b-versatile", "messages": [{"role":"user","content":prompt}],
                  "max_tokens": 10, "temperature": 0},
        )
        if r.status_code == 200:
            txt = r.json()["choices"][0]["message"]["content"].strip().upper()
            return "YES" if "YES" in txt else "NO"
    return None

@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5), reraise=False)
async def _ask_gemini(prompt: str) -> Optional[str]:
    if not Secrets.GEMINI_KEY: return None
    url = f"{_GEMINI_URL}?key={Secrets.GEMINI_KEY}"
    async with httpx.AsyncClient(timeout=12) as client:
        r = await client.post(url, json={"contents":[{"parts":[{"text":prompt}]}]})
        if r.status_code == 200:
            txt = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip().upper()
            return "YES" if "YES" in txt else "NO"
    return None

async def confirm_signal(signal: Dict) -> str:
    """
    Get AI confirmation from Grok (primary) + Gemini (fallback).
    Returns: 'YES' | 'NO' | 'SKIP' (when both unavailable)
    """
    try:
        prompt = _build_prompt(signal)
        # Try Grok first
        answer = await _ask_groq(prompt)
        if answer:
            logger.info(f"[AI/Grok] {signal.get('symbol')} {signal.get('direction')}: {answer}")
            return answer
        # Fallback to Gemini
        answer = await _ask_gemini(prompt)
        if answer:
            logger.info(f"[AI/Gemini] {signal.get('symbol')} {signal.get('direction')}: {answer}")
            return answer
        logger.warning("[AI] Both APIs unavailable — skipping confirmation")
        return "SKIP"
    except Exception as e:
        logger.warning(f"[AI] confirm_signal error: {e}")
        return "SKIP"
