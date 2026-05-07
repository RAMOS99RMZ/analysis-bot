# engine/indicator_engine.py
# All technical indicators — using pandas-ta (NO manual math)
# Mirrors: calcRSI, calcMACD, calcATR, calcIchimoku, calcSuperTrend, etc.

from __future__ import annotations
import numpy as np
import pandas as pd
import pandas_ta as ta
from typing import Dict, List, Optional
from loguru import logger
from utils.helpers import closes_asc, calc_hurst, get_session


class IndicatorEngine:
    """
    Computes all technical indicators needed by the 11 Experts.
    Uses pandas-ta exclusively — no manual TA math.
    """

    # ── DataFrame builder ─────────────────────────────────────────────────────

    @staticmethod
    def to_df(candles: List) -> pd.DataFrame:
        """Convert OKX candles (newest-first) to OHLCV DataFrame (oldest-first)."""
        df = pd.DataFrame(
            reversed(candles),
            columns=["ts", "open", "high", "low", "close", "volume", *["_"] * max(0, len(candles[0]) - 6)]
        )
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col])
        df["ts"] = pd.to_numeric(df["ts"])
        df = df.sort_values("ts").reset_index(drop=True)
        return df[["ts", "open", "high", "low", "close", "volume"]]

    # ── Core indicators ───────────────────────────────────────────────────────

    def rsi(self, candles: List, period: int = 14) -> float:
        if not candles or len(candles) < period + 2: return 50.0
        df = self.to_df(candles)
        val = ta.rsi(df["close"], length=period)
        return float(val.iloc[-1]) if val is not None and not val.empty else 50.0

    def macd(self, candles: List) -> Dict:
        if not candles or len(candles) < 35:
            return {"macd": 0, "signal": 0, "hist": 0}
        df = self.to_df(candles)
        m = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if m is None or m.empty:
            return {"macd": 0, "signal": 0, "hist": 0}
        return {
            "macd":   float(m["MACD_12_26_9"].iloc[-1]),
            "signal": float(m["MACDs_12_26_9"].iloc[-1]),
            "hist":   float(m["MACDh_12_26_9"].iloc[-1]),
        }

    def atr(self, candles: List, period: int = 14) -> float:
        if not candles or len(candles) < period + 2: return 0.0
        df = self.to_df(candles)
        val = ta.atr(df["high"], df["low"], df["close"], length=period)
        return float(val.iloc[-1]) if val is not None and not val.empty else 0.0

    def bbands(self, candles: List, period: int = 20) -> Dict:
        if not candles or len(candles) < period + 2:
            return {"upper": 0, "mid": 0, "lower": 0, "pct_b": 0.5}
        df = self.to_df(candles)
        bb = ta.bbands(df["close"], length=period, std=2)
        if bb is None or bb.empty:
            return {"upper": 0, "mid": 0, "lower": 0, "pct_b": 0.5}
        price = df["close"].iloc[-1]
        upper = float(bb[f"BBU_{period}_2.0"].iloc[-1])
        lower = float(bb[f"BBL_{period}_2.0"].iloc[-1])
        mid   = float(bb[f"BBM_{period}_2.0"].iloc[-1])
        pct_b = (price - lower) / (upper - lower) if upper != lower else 0.5
        return {"upper": upper, "mid": mid, "lower": lower, "pct_b": pct_b}

    def ichimoku(self, candles: List) -> Optional[Dict]:
        if not candles or len(candles) < 55: return None
        df = self.to_df(candles)
        ichi, spans = ta.ichimoku(df["high"], df["low"], df["close"])
        if ichi is None: return None
        price  = df["close"].iloc[-1]
        tenkan = float(ichi["ITS_9"].iloc[-1])  if "ITS_9"  in ichi.columns else price
        kijun  = float(ichi["IKS_26"].iloc[-1]) if "IKS_26" in ichi.columns else price
        span_a = float(ichi["ISA_9"].iloc[-1])  if "ISA_9"  in ichi.columns else price
        span_b = float(ichi["ISB_26"].iloc[-1]) if "ISB_26" in ichi.columns else price
        cloud_top, cloud_bot = max(span_a, span_b), min(span_a, span_b)
        return {
            "above_cloud": price > cloud_top,
            "below_cloud": price < cloud_bot,
            "bull_cross":  tenkan > kijun,
            "tenkan": tenkan, "kijun": kijun,
            "span_a": span_a, "span_b": span_b,
        }

    def supertrend(self, candles: List, mult: float = 3.0) -> Optional[Dict]:
        if not candles or len(candles) < 12: return None
        df = self.to_df(candles)
        st = ta.supertrend(df["high"], df["low"], df["close"], length=10, multiplier=mult)
        if st is None or st.empty: return None
        col_dir = [c for c in st.columns if "SUPERTd" in c]
        if not col_dir: return None
        cur  = int(st[col_dir[0]].iloc[-1])
        prev = int(st[col_dir[0]].iloc[-2]) if len(st) > 1 else cur
        bull = cur == 1
        return {
            "bull": bull, "bear": not bull,
            "flip": cur != prev,
            "bull_flip": bull and prev != 1,
            "bear_flip": not bull and prev == 1,
        }

    def heikin_ashi(self, candles: List) -> Optional[Dict]:
        if not candles or len(candles) < 4: return None
        df = self.to_df(candles)
        ha = ta.ha(df["open"], df["high"], df["low"], df["close"])
        if ha is None or len(ha) < 3: return None
        h0 = ha.iloc[-1]; h1 = ha.iloc[-2]; h2 = ha.iloc[-3]
        bull = float(h0["HA_close"]) > float(h0["HA_open"])
        p_bull = float(h1["HA_close"]) > float(h1["HA_open"])
        bull_strong = bull and p_bull and float(h0["HA_low"]) == float(h0["HA_open"])
        bear_strong = not bull and not p_bull and float(h0["HA_high"]) == float(h0["HA_open"])
        return {
            "bull": bull, "bear": not bull,
            "bull_strong": bull_strong, "bear_strong": bear_strong,
            "flip": bull != p_bull,
        }

    def stoch_rsi(self, candles: List) -> Optional[Dict]:
        if not candles or len(candles) < 35: return None
        df = self.to_df(candles)
        sr = ta.stochrsi(df["close"], length=14, rsi_length=14, k=3, d=3)
        if sr is None or sr.empty: return None
        cols = [c for c in sr.columns if "STOCHRSIk" in c]
        if not cols: return None
        k = float(sr[cols[0]].iloc[-1])
        pk = float(sr[cols[0]].iloc[-2]) if len(sr) > 1 else k
        return {
            "k": k, "oversold": k < 20, "overbought": k > 80,
            "bull_cross": k > pk and pk < 20,
            "bear_cross": k < pk and pk > 80,
            "rising": k > pk,
        }

    def adx(self, candles: List, period: int = 14) -> Optional[Dict]:
        if not candles or len(candles) < period * 2: return None
        df = self.to_df(candles)
        adx_df = ta.adx(df["high"], df["low"], df["close"], length=period)
        if adx_df is None or adx_df.empty: return None
        adx_col = [c for c in adx_df.columns if c.startswith("ADX_")]
        pdi_col = [c for c in adx_df.columns if "DMP_" in c]
        ndi_col = [c for c in adx_df.columns if "DMN_" in c]
        if not adx_col: return None
        adx_v = float(adx_df[adx_col[0]].iloc[-1])
        pdi_v = float(adx_df[pdi_col[0]].iloc[-1]) if pdi_col else 0
        ndi_v = float(adx_df[ndi_col[0]].iloc[-1]) if ndi_col else 0
        return {
            "adx": adx_v, "pdi": pdi_v, "ndi": ndi_v,
            "trending": adx_v > 25, "ranging": adx_v < 20,
            "bullish": pdi_v > ndi_v and adx_v > 20,
            "bearish": ndi_v > pdi_v and adx_v > 20,
        }

    def obv(self, candles: List) -> Optional[Dict]:
        if not candles or len(candles) < 12: return None
        df = self.to_df(candles)
        obv_s = ta.obv(df["close"], df["volume"])
        if obv_s is None or len(obv_s) < 10: return None
        recent = obv_s.iloc[-5:].mean()
        old    = obv_s.iloc[-10:-5].mean()
        p0     = df["close"].iloc[-1]
        p5     = df["close"].iloc[-6] if len(df) > 5 else p0
        return {
            "rising":   recent > old,
            "falling":  recent < old,
            "div_bull": p0 < p5 and recent > old,
            "div_bear": p0 > p5 and recent < old,
        }

    def pivot_points(self, candles: List) -> Optional[Dict]:
        if not candles or len(candles) < 2: return None
        prev = candles[1]  # yesterday's candle (newest-first index)
        h, l, c = float(prev[2]), float(prev[3]), float(prev[4])
        P = (h + l + c) / 3
        return {
            "P": P, "R1": 2*P-l, "R2": P+(h-l), "R3": h+2*(P-l),
            "S1": 2*P-h, "S2": P-(h-l), "S3": l-2*(h-P),
        }

    def psar(self, candles: List) -> Optional[Dict]:
        if not candles or len(candles) < 10: return None
        df = self.to_df(candles)
        ps = ta.psar(df["high"], df["low"], df["close"])
        if ps is None or ps.empty: return None
        long_col  = [c for c in ps.columns if "PSARl" in c]
        short_col = [c for c in ps.columns if "PSARs" in c]
        if not long_col: return None
        is_bull  = not pd.isna(ps[long_col[0]].iloc[-1])
        was_bull = not pd.isna(ps[long_col[0]].iloc[-2]) if len(ps) > 1 else is_bull
        return {"bull": is_bull, "flip": is_bull != was_bull}

    def williams_r(self, candles: List, period: int = 14) -> float:
        if not candles or len(candles) < period: return 0.0
        df = self.to_df(candles)
        wr = ta.willr(df["high"], df["low"], df["close"], length=period)
        return float(wr.iloc[-1]) if wr is not None and not wr.empty else 0.0

    def cmf(self, candles: List, period: int = 14) -> Optional[Dict]:
        if not candles or len(candles) < period + 2: return None
        df = self.to_df(candles)
        cmf_s = ta.cmf(df["high"], df["low"], df["close"], df["volume"], length=period)
        if cmf_s is None or cmf_s.empty: return None
        val = float(cmf_s.iloc[-1])
        return {"value": val, "bullish": val > 0.05, "bearish": val < -0.05, "strong": abs(val) > 0.15}

    def vwap(self, candles: List) -> float:
        if not candles or len(candles) < 5: return 0.0
        df = self.to_df(candles).tail(20)
        vw = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        return float(vw.iloc[-1]) if vw is not None and not vw.empty else 0.0

    def ema(self, candles: List, period: int = 20) -> float:
        if not candles or len(candles) < period: return 0.0
        df = self.to_df(candles)
        e = ta.ema(df["close"], length=period)
        return float(e.iloc[-1]) if e is not None and not e.empty else 0.0

    # ── Trend helpers ─────────────────────────────────────────────────────────

    def get_trend(self, candles: List) -> str:
        """4H trend: BULL / BEAR / MIXED. Mirrors getTrend() in GAS."""
        if not candles or len(candles) < 55: return "MIXED"
        p    = float(candles[0][4])
        e50  = self.ema(candles, 50)
        e200 = self.ema(candles, 200)
        if p > e50 and e50 > e200: return "BULL"
        if p < e50 and e50 < e200: return "BEAR"
        return "MIXED"

    def get_trend_1h(self, candles: List) -> str:
        """1H trend. Mirrors getTrend1H() in GAS."""
        if not candles or len(candles) < 30: return "MIXED"
        p   = float(candles[0][4])
        e20 = self.ema(candles, 20)
        e50 = self.ema(candles, 50)
        if p > e20 and e20 > e50: return "BULL"
        if p < e20 and e20 < e50: return "BEAR"
        return "MIXED"

    # ── Market Regime ─────────────────────────────────────────────────────────

    def get_market_regime(self, c4h: List, c1h: List) -> Dict:
        """
        Hurst + ADX + EMA alignment regime detection.
        Mirrors getMarketRegime() in GAS.
        """
        try:
            closes = closes_asc(c4h)
            hurst  = calc_hurst(closes[-60:] if len(closes) >= 60 else closes)
            adx_r  = self.adx(c4h, 14)
            e50    = self.ema(c4h, 50)
            e200   = self.ema(c4h, 200)
            price  = float(c4h[0][4])
            bull_align = price > e50 > e200
            bear_align = price < e50 < e200

            ts, rs = 0, 0
            if hurst > 0.60: ts += 4
            elif hurst > 0.55: ts += 2
            if hurst < 0.40: rs += 4
            if bull_align or bear_align: ts += 2
            else: rs += 1
            if adx_r:
                if adx_r["trending"]: ts += 2
                if adx_r["ranging"]:  rs += 2

            if ts > rs + 2:
                strength = min(1.0, ts / 12)
                return {
                    "regime": "TRENDING", "strength": strength,
                    "label": ("📈" if bull_align else "📉") + f" {int(strength*100)}%",
                    "hurst": hurst, "bull_align": bull_align, "bear_align": bear_align,
                }
            if rs > ts + 2:
                strength = min(1.0, rs / 12)
                return {
                    "regime": "RANGING", "strength": strength,
                    "label": f"↔️ Range {int(strength*100)}%",
                    "hurst": hurst, "bull_align": bull_align, "bear_align": bear_align,
                }
            return {
                "regime": "MIXED", "strength": 0.5, "label": "🔀 Mixed",
                "hurst": hurst, "bull_align": bull_align, "bear_align": bear_align,
            }
        except Exception as e:
            logger.warning(f"[IE] market_regime: {e}")
            return {"regime": "UNKNOWN", "strength": 0, "label": "❓"}

    # ── MTF Score ─────────────────────────────────────────────────────────────

    def get_mtf_score(self, c5m, c15m, c30m, c1h, c4h, direction: str) -> Dict:
        """
        Multi-timeframe alignment score. Mirrors getMTF() in GAS.
        Weights: 4H=0.30, 1H=0.25, 30m=0.20, 15m=0.15, 5m=0.10
        """
        weights = [
            (c4h,  0.30, "4H"),
            (c1h,  0.25, "1H"),
            (c30m, 0.20, "30m"),
            (c15m, 0.15, "15m"),
            (c5m,  0.10, "5m"),
        ]
        mtf_score = 0.0
        tf_scores: Dict[str, float] = {}
        for candles, w, name in weights:
            if not candles or len(candles) < 20:
                tf_scores[name] = 0; continue
            p    = float(candles[0][4])
            prev = float(candles[min(4, len(candles)-1)][4])
            e20  = self.ema(candles, 20)
            e50  = self.ema(candles, 50)
            mom  = (p - prev) / (prev or 1)
            s = 0.0
            if direction == "LONG":
                if mom > 0: s += 0.34
                if p > e20:  s += 0.33
                if e20 > e50: s += 0.33
            else:
                if mom < 0: s += 0.34
                if p < e20:  s += 0.33
                if e20 < e50: s += 0.33
            tf_scores[name] = round(s, 2)
            if s >= 0.30: mtf_score += s * w
        return {"score": round(mtf_score, 4), "tf_scores": tf_scores}

    # ── Indicator Weighting Score ─────────────────────────────────────────────

    def calc_iw_score(self, c5m, c1h, c4h, direction: str) -> Dict:
        """
        Composite indicator weighting score. Mirrors calcIndicatorWeightScore() in GAS.
        """
        score, max_score = 0.0, 0.0
        details: Dict[str, str] = {}
        is_long = (direction == "LONG")
        W = {  # mirror of CONFIG.IW
            "ICHIMOKU_4H": 3.0, "ADX_4H": 1.5, "EMA_TREND_4H": 1.5,
            "SUPERTREND_1H": 2.0, "HA_1H": 1.5, "RSI_DIV_1H": 1.5,
            "RSI_5M": 1.0, "MACD_5M": 1.0, "BB_5M": 0.8,
            "OBV_5M": 0.8, "PSAR_5M": 0.7, "PIVOT_1H": 1.0,
            "CANDLE_5M": 0.8, "STOCH_5M": 0.6, "WR_5M": 0.5,
        }

        # ── 4H ───────────────────────────────────────────────────────────────
        if c4h and len(c4h) >= 52:
            max_score += W["ICHIMOKU_4H"]
            ichi = self.ichimoku(c4h)
            if ichi:
                v = W["ICHIMOKU_4H"] if (is_long and ichi["above_cloud"] and ichi["bull_cross"]) or \
                                        (not is_long and ichi["below_cloud"] and not ichi["bull_cross"]) else \
                    W["ICHIMOKU_4H"] * 0.4 if (is_long and ichi["above_cloud"]) or \
                                              (not is_long and ichi["below_cloud"]) else 0
                score += v; details["ichi4h"] = f"{v:.1f}/{W['ICHIMOKU_4H']}"

            max_score += W["ADX_4H"]
            adx_r = self.adx(c4h)
            if adx_r:
                v = W["ADX_4H"] if (is_long and adx_r["bullish"]) or (not is_long and adx_r["bearish"]) else \
                    W["ADX_4H"] * 0.5 if adx_r["trending"] else 0
                if adx_r["ranging"]: v *= 0.3
                score += v; details["adx4h"] = f"{v:.1f}"

            max_score += W["EMA_TREND_4H"]
            p4  = float(c4h[0][4])
            e20_4 = self.ema(c4h, 20); e50_4 = self.ema(c4h, 50); e200_4 = self.ema(c4h, 200)
            bullc = sum([p4 > e20_4, e20_4 > e50_4, e50_4 > e200_4])
            bearc = sum([p4 < e20_4, e20_4 < e50_4, e50_4 < e200_4])
            v = W["EMA_TREND_4H"] if (is_long and bullc == 3) or (not is_long and bearc == 3) else \
                W["EMA_TREND_4H"] * 0.6 if (is_long and bullc == 2) or (not is_long and bearc == 2) else 0
            score += v; details["ema4h"] = f"{v:.1f}"

        # ── 1H ───────────────────────────────────────────────────────────────
        if c1h and len(c1h) >= 20:
            max_score += W["SUPERTREND_1H"]
            st = self.supertrend(c1h, 3)
            if st:
                v = W["SUPERTREND_1H"] if (is_long and (st["bull"] or st["bull_flip"])) or \
                                          (not is_long and (st["bear"] or st["bear_flip"])) else 0
                score += v; details["st1h"] = f"{v:.1f}"

            max_score += W["HA_1H"]
            ha = self.heikin_ashi(c1h)
            if ha:
                v = W["HA_1H"] if (is_long and ha["bull_strong"]) or (not is_long and ha["bear_strong"]) else \
                    W["HA_1H"] * 0.6 if (is_long and ha["bull"]) or (not is_long and ha["bear"]) else 0
                if ha["flip"]: v = max(v, W["HA_1H"] * 0.8)
                score += v; details["ha1h"] = f"{v:.1f}"

            max_score += W["RSI_DIV_1H"]
            # Divergence check (simplified)
            if len(c1h) >= 25:
                r0 = self.rsi(c1h[:15], 14); r1 = self.rsi(c1h[5:25], 14)
                p0 = float(c1h[0][4]); p1 = float(c1h[5][4])
                div_bull = is_long and p0 < p1 and r0 > r1 + 3
                div_bear = not is_long and p0 > p1 and r0 < r1 - 3
                v = W["RSI_DIV_1H"] if (div_bull or div_bear) else 0
                score += v; details["div1h"] = f"{v:.1f}"

        # ── 5M ───────────────────────────────────────────────────────────────
        if c5m and len(c5m) >= 20:
            p5 = float(c5m[0][4])
            rsi5 = self.rsi(c5m, 14)
            max_score += W["RSI_5M"]
            v = W["RSI_5M"] if (is_long and rsi5 < 35) or (not is_long and rsi5 > 65) else \
                W["RSI_5M"] * 0.5 if (is_long and rsi5 < 45) or (not is_long and rsi5 > 55) else 0
            score += v; details["rsi5m"] = f"{v:.1f}"

            max_score += W["MACD_5M"]
            mcd = self.macd(c5m)
            v = W["MACD_5M"] if (is_long and mcd["hist"] > 0 and mcd["macd"] > 0) or \
                                (not is_long and mcd["hist"] < 0 and mcd["macd"] < 0) else \
                W["MACD_5M"] * 0.5 if (is_long and mcd["hist"] > 0) or (not is_long and mcd["hist"] < 0) else 0
            score += v; details["macd5m"] = f"{v:.1f}"

            max_score += W["BB_5M"]
            bb = self.bbands(c5m, 20)
            v = W["BB_5M"] if (is_long and p5 <= bb["lower"] * 1.002) or \
                              (not is_long and p5 >= bb["upper"] * 0.998) else 0
            score += v; details["bb5m"] = f"{v:.1f}"

            max_score += W["OBV_5M"]
            obv_r = self.obv(c5m)
            if obv_r:
                v = W["OBV_5M"] if (is_long and obv_r["div_bull"]) or (not is_long and obv_r["div_bear"]) else \
                    W["OBV_5M"] * 0.5 if (is_long and obv_r["rising"]) or (not is_long and obv_r["falling"]) else 0
                score += v; details["obv5m"] = f"{v:.1f}"

            max_score += W["PSAR_5M"]
            psar_r = self.psar(c5m)
            if psar_r:
                v = W["PSAR_5M"] if (is_long and psar_r["bull"]) or (not is_long and not psar_r["bull"]) else 0
                if psar_r["flip"]: v = W["PSAR_5M"]
                score += v; details["psar5m"] = f"{v:.1f}"

            max_score += W["STOCH_5M"]
            sr = self.stoch_rsi(c5m)
            if sr:
                v = W["STOCH_5M"] if (is_long and sr["oversold"]) or (not is_long and sr["overbought"]) else 0
                score += v; details["stoch5m"] = f"{v:.1f}"

            max_score += W["WR_5M"]
            wr_v = self.williams_r(c5m, 14)
            v = W["WR_5M"] if (is_long and wr_v <= -80) or (not is_long and wr_v >= -20) else 0
            score += v; details["wr5m"] = f"{v:.1f}"

            max_score += W["CANDLE_5M"]
            if len(c5m) >= 3:
                c1 = c5m[1]
                o1, h1, l1, cl1 = float(c1[1]), float(c1[2]), float(c1[3]), float(c1[4])
                bd = abs(cl1 - o1)
                lw = min(o1, cl1) - l1; uw = h1 - max(o1, cl1)
                v = W["CANDLE_5M"] if (is_long and lw > bd * 2 and uw < bd * 0.5) or \
                                      (not is_long and uw > bd * 2 and lw < bd * 0.5) else 0
                score += v; details["candle5m"] = f"{v:.1f}"

            # Pivot
            if c1h and len(c1h) >= 2:
                max_score += W["PIVOT_1H"]
                pv = self.pivot_points(c1h)
                if pv:
                    near_s = any(abs(p5 - pv[f"S{i}"]) / p5 < 0.004 for i in [1, 2])
                    near_r = any(abs(p5 - pv[f"R{i}"]) / p5 < 0.004 for i in [1, 2])
                    v = W["PIVOT_1H"] if (is_long and near_s) or (not is_long and near_r) else \
                        W["PIVOT_1H"] * 0.3 if (is_long and p5 > pv["P"]) or (not is_long and p5 < pv["P"]) else 0
                    score += v; details["pivot1h"] = f"{v:.1f}"

        normalized = min(score / max_score, 1.0) if max_score > 0 else 0.0
        return {"score": round(normalized, 4), "raw": round(score, 2),
                "max": round(max_score, 2), "details": details}
