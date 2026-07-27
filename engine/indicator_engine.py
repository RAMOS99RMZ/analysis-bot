
"""
engine/indicator_engine.py — Ramos 360 Ai 🎖️
FIXED: pandas_ta import wrapped in try/except
Falls back to manual numpy/pandas implementations when pandas_ta unavailable.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from loguru import logger

# ── pandas_ta optional import ─────────────────────────────────────────────────
try:
    import pandas_ta as ta
    HAS_TA = True
    logger.debug("pandas_ta loaded ✅")
except ImportError:
    ta = None
    HAS_TA = False
    logger.debug("pandas_ta not available — using manual fallback implementations")


class IndicatorEngine:
    """
    Computes all technical indicators needed by the bot.
    Works with OR without pandas_ta installed.
    """

    def add_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all indicators to dataframe. Main entry point."""
        if df is None or len(df) < 5:
            return df
        df = df.copy()
        try:
            df = self._add_trend(df)
            df = self._add_momentum(df)
            df = self._add_volatility(df)
            df = self._add_volume(df)
            df = self._add_structure(df)
        except Exception as e:
            logger.warning(f"[IE] indicator error: {e}")
        return df.ffill().bfill()

    # ── also accept old name ──────────────────────────────────────────────────
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.add_all_indicators(df)

    # ═════════════════════════════════════════════════════════════════
    # TREND INDICATORS
    # ═════════════════════════════════════════════════════════════════

    def _add_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        c = df["close"]

        # EMAs
        for p in [9, 20, 50, 100, 200]:
            col = f"ema{p}"
            if HAS_TA:
                try:
                    v = ta.ema(c, length=p)
                    if v is not None:
                        df[col] = v.ffill().bfill(); continue
                except Exception:
                    pass
            df[col] = c.ewm(span=p, adjust=False).mean()

        # Ichimoku
        h = df["high"]; l = df["low"]
        df["iten"] = (h.rolling(9).max()  + l.rolling(9).min())  / 2
        df["ikij"] = (h.rolling(26).max() + l.rolling(26).min()) / 2
        sa = (df.iten + df.ikij) / 2
        sb = (h.rolling(52).max() + l.rolling(52).min()) / 2
        df["ict"] = pd.concat([sa, sb], axis=1).max(axis=1)
        df["icb"] = pd.concat([sa, sb], axis=1).min(axis=1)

        # Supertrend (manual)
        atr = self._atr(df, 10)
        m = 3.0
        ub = (h + l) / 2 + m * atr
        lb = (h + l) / 2 - m * atr
        st = pd.Series(index=df.index, dtype=float)
        direction = pd.Series(1, index=df.index)
        for i in range(1, len(df)):
            prev_ub = ub.iloc[i-1]; prev_lb = lb.iloc[i-1]
            ub.iloc[i]  = min(ub.iloc[i],  prev_ub) if c.iloc[i-1] <= prev_ub else ub.iloc[i]
            lb.iloc[i]  = max(lb.iloc[i],  prev_lb) if c.iloc[i-1] >= prev_lb else lb.iloc[i]
            if c.iloc[i] > ub.iloc[i-1]:   direction.iloc[i] = 1
            elif c.iloc[i] < lb.iloc[i-1]: direction.iloc[i] = -1
            else:                           direction.iloc[i] = direction.iloc[i-1]
            st.iloc[i] = lb.iloc[i] if direction.iloc[i] == 1 else ub.iloc[i]
        df["supertrend"]  = st
        df["supertrend_d"] = direction

        return df

    # ═════════════════════════════════════════════════════════════════
    # MOMENTUM INDICATORS
    # ═════════════════════════════════════════════════════════════════

    def _add_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        c = df["close"]; h = df["high"]; l = df["low"]

        # RSI 14 + 6
        for p in [6, 14]:
            col = f"rsi{p}" if p != 14 else "rsi"
            if HAS_TA:
                try:
                    v = ta.rsi(c, length=p)
                    if v is not None:
                        df[col] = v.fillna(50); continue
                except Exception:
                    pass
            d = c.diff()
            g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
            ls = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
            df[col] = (100 - 100 / (1 + g / ls.replace(0, 1e-10))).fillna(50)

        df["rsi_prev"] = df["rsi"].shift(1).fillna(50)

        # MACD
        if HAS_TA:
            try:
                m = ta.macd(c, fast=12, slow=26, signal=9)
                if m is not None and not m.empty:
                    df["macd"]   = m.iloc[:, 0].fillna(0)
                    df["macds"]  = m.iloc[:, 1].fillna(0)
                    df["macd_h"] = m.iloc[:, 2].fillna(0)
            except Exception:
                self._macd_manual(df, c)
        else:
            self._macd_manual(df, c)

        df["mh"] = df.get("macd_h", pd.Series(0.0, index=df.index)).fillna(0)
        df["mh_p"] = df.mh.shift(1).fillna(0)

        # Stochastic
        lo14 = l.rolling(14).min(); hi14 = h.rolling(14).max()
        df["stoch"]   = 100 * (c - lo14) / (hi14 - lo14 + 1e-10)
        df["stoch_d"] = df.stoch.rolling(3).mean()

        # Williams %R
        df["wr"] = (-100 * (hi14 - c) / (hi14 - lo14 + 1e-10)).fillna(-50)

        # ADX + DI
        up = (h - h.shift()).clip(lower=0)
        dn = (l.shift() - l).clip(lower=0)
        df["pdi"] = up.ewm(alpha=1/14, adjust=False).mean()
        df["mdi"] = dn.ewm(alpha=1/14, adjust=False).mean()
        df["adx"] = ((df.pdi - df.mdi).abs() /
                     (df.pdi + df.mdi + 1e-10) * 100
                    ).ewm(alpha=1/14, adjust=False).mean().fillna(15)

        return df

    def _macd_manual(self, df, c):
        e12 = c.ewm(span=12, adjust=False).mean()
        e26 = c.ewm(span=26, adjust=False).mean()
        ms  = e12 - e26
        df["macd"]   = ms.fillna(0)
        df["macds"]  = ms.ewm(span=9, adjust=False).mean().fillna(0)
        df["macd_h"] = (df["macd"] - df["macds"]).fillna(0)

    # ═════════════════════════════════════════════════════════════════
    # VOLATILITY INDICATORS
    # ═════════════════════════════════════════════════════════════════

    def _add_volatility(self, df: pd.DataFrame) -> pd.DataFrame:
        c = df["close"]; h = df["high"]; l = df["low"]

        # ATR
        df["atr"] = self._atr(df, 14)

        # Bollinger Bands
        mid = c.rolling(20).mean(); sd = c.rolling(20).std()
        df["bbu"] = mid + 2 * sd
        df["bbl"] = mid - 2 * sd
        df["bbm"] = mid
        df["bbp"] = (c - df.bbl) / (df.bbu - df.bbl + 1e-10) * 100
        df["bbw"] = (df.bbu - df.bbl) / mid.replace(0, 1)

        # Heikin Ashi
        df["hac"]  = (df.open + h + l + c) / 4
        df["hao"]  = df.hac.shift(2)
        df["hab"]  = (df.hac > df.hao).astype(int)

        # Noise ratio (body/range)
        body = (c - df.open).abs()
        rng  = (h - l).replace(0, 1e-6)
        df["noise"]  = (body / rng).fillna(0.5)
        df["body_r"] = df["noise"]

        return df

    def _atr(self, df: pd.DataFrame, p: int = 14) -> pd.Series:
        h = df["high"]; l = df["low"]; c = df["close"]
        if HAS_TA:
            try:
                v = ta.atr(h, l, c, length=p)
                if v is not None:
                    return v.ffill().bfill()
            except Exception:
                pass
        hl  = h - l
        hpc = (h - c.shift()).abs()
        lpc = (l - c.shift()).abs()
        return pd.concat([hl, hpc, lpc], axis=1).max(axis=1).ewm(alpha=1/p, adjust=False).mean()

    # ═════════════════════════════════════════════════════════════════
    # VOLUME INDICATORS
    # ═════════════════════════════════════════════════════════════════

    def _add_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        c = df["close"]; v = df["volume"]

        # OBV
        sign    = c.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
        df["obv"]    = (sign * v).cumsum()
        df["obv_ma"] = df.obv.rolling(14).mean()

        # Volume metrics
        df["vma"] = v.rolling(20).mean()
        df["vr"]  = v / df.vma.replace(0, 1)

        # Historical volatility
        df["hv"] = c.pct_change().rolling(20).std() * np.sqrt(252)

        return df

    # ═════════════════════════════════════════════════════════════════
    # STRUCTURE LEVELS
    # ═════════════════════════════════════════════════════════════════

    def _add_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        h = df["high"]; l = df["low"]

        # Swing levels
        for p in [5, 8, 10, 14]:
            df[f"slo{p}"] = l.rolling(p).min()
            df[f"shi{p}"] = h.rolling(p).max()

        # Pivot Points
        df["pp"] = (h + l + df["close"]) / 3
        df["r1"] = 2 * df.pp - l
        df["s1"] = 2 * df.pp - h
        df["r2"] = df.pp + (h - l)
        df["s2"] = df.pp - (h - l)

        # Wyckoff simplified
        c = df["close"]
        df["wyck"] = 0.0
        df.loc[(c > df.ema50) & (df.vr > 1.2), "wyck"] =  1.0
        df.loc[(c < df.ema50) & (df.vr > 1.2), "wyck"] = -1.0

        return df
