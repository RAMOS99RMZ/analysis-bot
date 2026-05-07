# backtesting/backtest_engine.py
# Vectorbt-powered backtester for strategy validation
# Run: python -m backtesting.backtest_engine

from __future__ import annotations
import asyncio
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from loguru import logger

try:
    import vectorbt as vbt
    VBT_AVAILABLE = True
except ImportError:
    VBT_AVAILABLE = False
    logger.warning("vectorbt not installed — backtesting disabled")

from engine.data_fetcher import DataFetcher
from engine.indicator_engine import IndicatorEngine

IE = IndicatorEngine()


class BacktestEngine:
    """
    Fetch historical data and run vectorbt backtests.
    Mirrors the GAS backtesting logic with proper statistical validation.
    """

    def __init__(self):
        self._fetcher = DataFetcher()

    async def fetch_history(self, symbol: str, timeframe: str = "1h",
                            limit: int = 1000) -> Optional[pd.DataFrame]:
        candles = await self._fetcher.get_candles(symbol, timeframe, limit)
        if not candles: return None
        df = IE.to_df(candles)
        df.index = pd.to_datetime(df["ts"], unit="ms")
        return df

    def run_rsi_strategy(self, df: pd.DataFrame, oversold: float = 35,
                          overbought: float = 65) -> Optional[Dict]:
        if not VBT_AVAILABLE or df is None or len(df) < 50:
            return None
        try:
            rsi_s = pd.Series(
                [float(x) for x in df["close"].rolling(14).apply(
                    lambda x: 100 - 100/(1 + (x[x.diff()>0].mean() or 1e-9)/
                                         (abs(x[x.diff()<0].mean()) or 1e-9)), raw=True
                )]
            )
            entries  = rsi_s < oversold
            exits    = rsi_s > overbought
            pf = vbt.Portfolio.from_signals(df["close"], entries, exits,
                                            fees=0.0005, slippage=0.0005)
            return {
                "total_return":  round(pf.total_return(), 4),
                "sharpe":        round(pf.sharpe_ratio(), 4),
                "max_drawdown":  round(pf.max_drawdown(), 4),
                "win_rate":      round(pf.trades.win_rate(), 4),
                "total_trades":  int(pf.trades.count()),
            }
        except Exception as e:
            logger.warning(f"[BT] run_rsi_strategy: {e}")
            return None

    def run_ema_crossover(self, df: pd.DataFrame,
                           fast: int = 20, slow: int = 50) -> Optional[Dict]:
        if not VBT_AVAILABLE or df is None or len(df) < slow + 10: return None
        try:
            ema_fast = df["close"].ewm(span=fast).mean()
            ema_slow = df["close"].ewm(span=slow).mean()
            entries = (ema_fast > ema_slow) & (ema_fast.shift(1) <= ema_slow.shift(1))
            exits   = (ema_fast < ema_slow) & (ema_fast.shift(1) >= ema_slow.shift(1))
            pf = vbt.Portfolio.from_signals(df["close"], entries, exits,
                                            fees=0.0005, slippage=0.0005)
            return {
                "total_return": round(pf.total_return(), 4),
                "sharpe":       round(pf.sharpe_ratio(), 4),
                "max_drawdown": round(pf.max_drawdown(), 4),
                "win_rate":     round(pf.trades.win_rate(), 4),
                "total_trades": int(pf.trades.count()),
            }
        except Exception as e:
            logger.warning(f"[BT] run_ema_crossover: {e}")
            return None

    async def run_full_backtest(self, symbol: str = "BTC/USDT:USDT") -> Dict:
        logger.info(f"[BT] Running backtest for {symbol} …")
        df = await self.fetch_history(symbol, "1h", 1000)
        if df is None:
            return {"error": "No data"}
        rsi_res = self.run_rsi_strategy(df)
        ema_res = self.run_ema_crossover(df)
        results = {"symbol": symbol, "rsi_strategy": rsi_res, "ema_crossover": ema_res}
        logger.info(f"[BT] Results: {results}")
        return results

    async def close(self):
        await self._fetcher.close()


async def _main():
    """Quick CLI backtest."""
    engine = BacktestEngine()
    try:
        for sym in ["BTC/USDT:USDT", "ETH/USDT:USDT"]:
            result = await engine.run_full_backtest(sym)
            print(f"\n{'='*50}")
            print(f"BACKTEST: {sym}")
            for k, v in result.items():
                print(f"  {k}: {v}")
    finally:
        await engine.close()

if __name__ == "__main__":
    asyncio.run(_main())
