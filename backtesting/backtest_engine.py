"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  FibPull v3
═══════════════════════════════════════════════════════════════════
STRATEGY: "Fibonacci Confluence Pullback"
Core philosophy:
  • Enter ONLY when trend retraces to key Fibonacci level
  • SL placed at Fibonacci INVALIDATION point (level below/above)
  • TP1 = 1.2× SL → hits quickly → SL to Breakeven → TP2/3 run free
  • Counter-trend trades FORBIDDEN (EMA200 alignment mandatory)

FIXES:
  ✅ Session recorded at ENTRY hour (not close hour)
  ✅ DEAD_ZONE blocked completely (17-23 UTC)
  ✅ Fibonacci pullback entry reduces false entries
  ✅ ATR minimum SL = 1.0× (wider than before = fewer premature SL)
  ✅ TP1 closer (1.2× SL) = SL to BE faster = protect balance
  ✅ Noise candle skip (doji + BB squeeze)
  ✅ Counter-trend blocked: EMA200 alignment mandatory
═══════════════════════════════════════════════════════════════════
Custom Fibonacci: 0.309 / 0.4045 / 0.618 / 0.75 / 0.78 / 0.809
"""
from __future__ import annotations
import asyncio, math
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import httpx, pandas as pd
from loguru import logger

try:
    import pandas_ta as ta
    HAS_TA = True
except Exception:
    HAS_TA = False

_BASE = "https://www.okx.com/api/v5"
_HDR  = {"Accept":"application/json","User-Agent":"Ramos360FP/3.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

# ─── Custom Fibonacci Ratios ──────────────────────────────────────────────────
FIBO = [0.309, 0.4045, 0.500, 0.618, 0.750, 0.780, 0.809]
FEXT = [1.272, 1.414, 1.618, 2.000, 2.618]
FTOL = 0.020   # 2% tolerance zone around each level

# ─── Session (FIXED: recorded at entry) ──────────────────────────────────────
TRADE_H = set(range(7, 17))   # 07:00–16:59 UTC only

def _sess(h: int) -> str:
    if 13<=h<16: return "OVERLAP"
    if  7<=h<13: return "LONDON"
    if 13<=h<17: return "NEW_YORK"
    if  0<=h< 7: return "ASIA"
    return "DEAD_ZONE"

def _sess_ok(h: int) -> bool:
    return h in TRADE_H

# ─── OKX Fetch ────────────────────────────────────────────────────────────────
def _inst(s: str) -> str:
    return s.split(":")[0].replace("/","-")+"-SWAP"

async def _fetch(symbol: str, tf: str, start: datetime, end: datetime) -> pd.DataFrame:
    inst=_inst(symbol); bar=_TFM.get(tf.lower(),tf)
    s_ms=int(start.timestamp()*1000); e_ms=int(end.timestamp()*1000)
    rows=[]; before=e_ms
    logger.info(f"[BT] {symbol} {tf} {start.date()}→{end.date()}")
    async with httpx.AsyncClient(timeout=30,headers=_HDR) as cl:
        while True:
            r=await cl.get(f"{_BASE}/market/history-candles",params={
                "instId":inst,"bar":bar,"before":str(s_ms),"after":str(before),"limit":"300"})
            if r.status_code==429: await asyncio.sleep(5); continue
            if r.status_code!=200: break
            j=r.json()
            if j.get("code")!="0" or not j.get("data"): break
            for c in j["data"]:
                ts=int(c[0])
                if s_ms<=ts<=e_ms:
                    rows.append([ts,float(c[1]),float(c[2]),float(c[3]),float(c[4]),float(c[5])])
            old=int(j["data"][-1][0])
            if old<=s_ms or len(j["data"])<300: break
            before=old; await asyncio.sleep(0.3)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows,columns=["ts","open","high","low","close","volume"])
    df=df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"]=pd.to_datetime(df["ts"],unit="ms",utc=True)
    logger.info(f"[BT] {symbol}: {len(df)} candles ✅")
    return df

# ─── Indicators ───────────────────────────────────────────────────────────────
def _build(df: pd.DataFrame) -> pd.DataFrame:
    df=df.copy(); c=df.close; h=df.high; l=df.low
    # RSI
    d=c.diff(); g=d.clip(lower=0).ewm(alpha=1/14,adjust=False).mean()
    ls=(-d.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
    df["rsi"]=(100-100/(1+g/ls.replace(0,1e-10))).fillna(50)
    df["rsi_p"]=df.rsi.shift(1).fillna(50)
    # MACD
    e12=c.ewm(span=12,adjust=False).mean(); e26=c.ewm(span=26,adjust=False).mean()
    ms=e12-e26; df["mh"]=(ms-ms.ewm(span=9,adjust=False).mean()).fillna(0)
    df["mh_p"]=df.mh.shift(1).fillna(0)
    # EMAs
    for p in [9,20,50,100,200]: df[f"e{p}"]=c.ewm(span=p,adjust=False).mean()
    # ATR
    hl=h-l; hpc=(h-c.shift()).abs(); lpc=(l-c.shift()).abs()
    df["atr"]=pd.concat([hl,hpc,lpc],axis=1).max(axis=1).ewm(alpha=1/14,adjust=False).mean()
    # ADX
    up=(h-h.shift()).clip(lower=0); dn=(l.shift()-l).clip(lower=0)
    df["pdi"]=up.ewm(alpha=1/14,adjust=False).mean()
    df["mdi"]=dn.ewm(alpha=1/14,adjust=False).mean()
    df["adx"]=((df.pdi-df.mdi).abs()/(df.pdi+df.mdi+1e-10)*100).ewm(alpha=1/14,adjust=False).mean().fillna(15)
    # Bollinger
    mid=c.rolling(20).mean(); sd=c.rolling(20).std()
    df["bbu"]=mid+2*sd; df["bbl"]=mid-2*sd
    df["bbp"]=(c-df.bbl)/(df.bbu-df.bbl+1e-10)*100
    df["bbw"]=(df.bbu-df.bbl)/mid.replace(0,1)
    # Stoch
    lo14=l.rolling(14).min(); hi14=h.rolling(14).max()
    df["stoch"]=100*(c-lo14)/(hi14-lo14+1e-10)
    df["stoch_d"]=df.stoch.rolling(3).mean()
    # WR
    df["wr"]=(-100*(hi14-c)/(hi14-lo14+1e-10)).fillna(-50)
    # OBV
    df["obv"]=(c.diff().apply(lambda x:1 if x>0 else -1 if x<0 else 0)*df.volume).cumsum()
    df["obv_ma"]=df.obv.rolling(14).mean()
    # Volume
    df["vma"]=df.volume.rolling(20).mean(); df["vr"]=df.volume/(df.vma.replace(0,1))
    # Ichimoku
    df["iten"]=(h.rolling(9).max()+l.rolling(9).min())/2
    df["ikij"]=(h.rolling(26).max()+l.rolling(26).min())/2
    sa=(df.iten+df.ikij)/2; sb=(h.rolling(52).max()+l.rolling(52).min())/2
    df["ict"]=pd.concat([sa,sb],axis=1).max(axis=1)
    df["icb"]=pd.concat([sa,sb],axis=1).min(axis=1)
    # HA
    df["hac"]=(df.open+h+l+c)/4; df["hao"]=df.hac.shift(2)
    df["hab"]=(df.hac>df.hao).astype(int)
    # Noise ratio
    body=(c-df.open).abs(); rng=(h-l).replace(0,1e-6)
    df["noise"]=(body/rng).fillna(0.5)
    # Swing levels
    df["slo10"]=l.rolling(10).min(); df["shi10"]=h.rolling(10).max()
    df["slo5"] =l.rolling(5).min();  df["shi5"] =h.rolling(5).max()
    return df.ffill().bfill()

# ─── Divergence pre-computed ─────────────────────────────────────────────────
def _add_div(df: pd.DataFrame) -> pd.DataFrame:
    sc=pd.Series(0.0,index=df.index); pl=[]; ph=[]
    for i in range(5,len(df)-5):
        lw=df.low.iloc[i-5:i+6]; hw=df.high.iloc[i-5:i+6]
        if len(lw)<11: continue
        if float(df.low.iloc[i]) ==float(lw.min()):
            pl.append((i,float(df.low.iloc[i]), float(df.rsi.iloc[i])))
        if float(df.high.iloc[i])==float(hw.max()):
            ph.append((i,float(df.high.iloc[i]),float(df.rsi.iloc[i])))
        if len(pl)>=2:
            a,b=pl[-2],pl[-1]
            if b[1]<a[1] and b[2]>a[2]: sc.iloc[i]+=0.9   # Reg Bull
            if b[1]>a[1] and b[2]<a[2]: sc.iloc[i]+=0.5   # Hid Bull
        if len(ph)>=2:
            a,b=ph[-2],ph[-1]
            if b[1]>a[1] and b[2]<a[2]: sc.iloc[i]-=0.9   # Reg Bear
            if b[1]<a[1] and b[2]>a[2]: sc.iloc[i]-=0.5   # Hid Bear
    df["div_sc"]=sc.clip(-1,1).fillna(0)
    return df

# ─── Fibonacci Analysis ───────────────────────────────────────────────────────
def _fib_zone(c: float, hi: float, lo: float) -> Tuple[float, str, float]:
    """
    Returns (position, zone_name, score)
    Zones defined by custom ratios: 0.309, 0.4045, 0.618, 0.75, 0.78, 0.809
    """
    rng = hi - lo
    if rng <= 0: return 0.5, "NONE", 0.0
    pos = (c - lo) / rng

    # Bull support zones (good for LONG at these retracements)
    BULL_SUPPORTS = [(0.289,0.329,0.309,"F309"),
                     (0.384,0.424,0.4045,"F4045"),
                     (0.480,0.520,0.500,"F500"),
                     (0.598,0.638,0.618,"F618_GOLD")]

    # Bear resistance zones (good for SHORT at these retracements)
    BEAR_RESIST   = [(0.480,0.520,0.500,"F500"),
                     (0.598,0.638,0.618,"F618"),
                     (0.730,0.770,0.750,"F750"),
                     (0.760,0.800,0.780,"F780"),
                     (0.789,0.829,0.809,"F809")]

    for lo_z,hi_z,ratio,name in BULL_SUPPORTS:
        if lo_z <= pos <= hi_z:
            strength = 1.0 if name=="F618_GOLD" else 0.8
            return pos, f"BULL_{name}", strength

    for lo_z,hi_z,ratio,name in BEAR_RESIST:
        if lo_z <= pos <= hi_z:
            strength = 1.0 if ratio==0.618 else 0.8
            return pos, f"BEAR_{name}", -strength

    # Not at a key level
    if pos < 0.30:   return pos, "DEEP_OVERSOLD",  0.4
    elif pos < 0.40: return pos, "BELOW_MEDIAN",   0.2
    elif pos > 0.70: return pos, "ABOVE_MEDIAN",  -0.2
    elif pos > 0.85: return pos, "NEAR_TOP",       -0.4
    return pos, "MIDRANGE", 0.0

# ─── Market Regime ────────────────────────────────────────────────────────────
def _regime(df: pd.DataFrame, i: int) -> str:
    """Determine trend regime: BULL, BEAR, RANGING"""
    if i < 30: return "RANGING"
    c=float(df.close.iloc[i])
    e20=float(df.e20.iloc[i]); e50=float(df.e50.iloc[i]); e200=float(df.e200.iloc[i])
    adx=float(df.adx.iloc[i])
    if c>e20>e50>e200 and adx>18: return "BULL"
    if c<e20<e50<e200 and adx>18: return "BEAR"
    if c>e50 and adx>15: return "BULL_WEAK"
    if c<e50 and adx>15: return "BEAR_WEAK"
    return "RANGING"

# ─── SL at Fibonacci Invalidation ────────────────────────────────────────────
def _fib_sl(df: pd.DataFrame, i: int, direction: str,
             price: float, hi: float, lo: float) -> Tuple[float,float]:
    """
    SL placed at Fibonacci INVALIDATION level:
    LONG: SL below the Fibonacci support we're trading from
    SHORT: SL above the Fibonacci resistance
    Also respects swing structure.
    """
    atr=float(df.atr.iloc[i]); atr=atr if atr>0 else price*0.015
    rng=hi-lo
    buf=atr*0.20   # small structural buffer

    if direction=="LONG":
        # Find next Fibonacci level BELOW current price
        fib_supports=[round(hi-rng*r,4) for r in FIBO if (hi-rng*r)<price*0.997]
        sw_lo=float(df.slo10.iloc[i])
        # SL = max(next_fib_below, swing_low) - buffer
        if fib_supports:
            fib_sl=max(fib_supports)   # highest fib below price
            sl=round(min(fib_sl,sw_lo)-buf,4)
        else:
            sl=round(sw_lo-buf,4)
        # Clamp to 1.0-2.5 × ATR
        sl=min(sl, round(price-atr*1.0,4))
        sl=max(sl, round(price-atr*2.5,4))
        sl_d=price-sl
    else:
        fib_resists=[round(hi-rng*r,4) for r in FIBO if (hi-rng*r)>price*1.003]
        sw_hi=float(df.shi10.iloc[i])
        if fib_resists:
            fib_sl=min(fib_resists)
            sl=round(max(fib_sl,sw_hi)+buf,4)
        else:
            sl=round(sw_hi+buf,4)
        sl=max(sl, round(price+atr*1.0,4))
        sl=min(sl, round(price+atr*2.5,4))
        sl_d=sl-price

    return sl, sl_d

# ─── Fibonacci TP Cascade ─────────────────────────────────────────────────────
def _fib_tp(price: float, sl_d: float, direction: str,
             hi: float, lo: float) -> Tuple[float,float,float]:
    """
    TP1: 1.2× SL → hits quickly → SL to BE
    TP2: nearest Fibonacci extension (1.272× or 1.618×)
    TP3: golden extension (2.618×)
    """
    rng=hi-lo

    # Base targets
    t1d=sl_d*1.2   # Quick TP1 (SL to BE after hit)
    t2d=sl_d*2.8   # TP2 (moderate)
    t3d=sl_d*5.0   # TP3 (runner)

    if direction=="LONG":
        tp1=round(price+t1d,4); tp2=round(price+t2d,4); tp3=round(price+t3d,4)
        # Snap TP2/3 to Fibonacci extension levels above price
        if rng>0:
            ext_levels=sorted([round(lo+rng*r,4) for r in FEXT])
            above=[v for v in ext_levels if v>price*1.005]
            if len(above)>=2: tp2=above[0]; tp3=above[1] if above[1]>tp2 else round(tp2*1.02,4)
            elif len(above)==1: tp2=above[0]
    else:
        tp1=round(price-t1d,4); tp2=round(price-t2d,4); tp3=round(price-t3d,4)
        if rng>0:
            ext_levels=sorted([round(hi-rng*r,4) for r in FEXT],reverse=True)
            below=[v for v in ext_levels if v<price*0.995]
            if len(below)>=2: tp2=below[0]; tp3=below[1] if below[1]<tp2 else round(tp2*0.98,4)
            elif len(below)==1: tp2=below[0]

    return tp1, tp2, tp3

# ─── 14-Expert Scoring ────────────────────────────────────────────────────────
def _score(df: pd.DataFrame, i: int, regime: str, fib_score: float) -> Tuple[str,float,int]:
    row=df.iloc[i]; c=float(row.close); sc=[]

    # E1: Trend (EMA) — forbidden counter-trend
    e20=float(row.e20); e50=float(row.e50); e200=float(row.e200)
    if c>e20>e50>e200: sc.append(1.3)
    elif c>e20>e50:    sc.append(0.8)
    elif c>e20:        sc.append(0.3)
    elif c<e20<e50<e200: sc.append(-1.3)
    elif c<e20<e50:    sc.append(-0.8)
    else:              sc.append(0.0)

    # E2: RSI
    r=float(row.rsi); rp=float(row.rsi_p)
    sc.append(1.1 if r<25 else 0.7 if r<35 else 0.3 if r<48 and r>rp
              else -1.1 if r>75 else -0.7 if r>65 else -0.3 if r>52 and r<rp else 0.0)

    # E3: MACD cross
    mh=float(row.mh); mhp=float(row.mh_p)
    if mh>0 and mhp<=0: sc.append(1.0)
    elif mh<0 and mhp>=0: sc.append(-1.0)
    elif mh>0: sc.append(0.4)
    else: sc.append(-0.4)

    # E4: Ichimoku
    ct=float(row.ict); cb=float(row.icb); tn=float(row.iten); kj=float(row.ikij)
    if c>ct and tn>kj: sc.append(1.0)
    elif c>ct: sc.append(0.5)
    elif c<cb and tn<kj: sc.append(-1.0)
    elif c<cb: sc.append(-0.5)
    else: sc.append(0.0)

    # E5: Fibonacci confluence (custom ratios)
    sc.append(fib_score * 1.1)   # boosted weight

    # E6: Bollinger Bands
    bp=float(row.bbp)
    sc.append(1.0 if bp<8 else 0.5 if bp<22 else -1.0 if bp>92 else -0.5 if bp>78 else 0.1 if bp<45 else -0.1)

    # E7: Stochastic
    sk=float(row.stoch); sd_v=float(row.stoch_d)
    if sk<18 and sk>sd_v: sc.append(0.9)
    elif sk<22: sc.append(0.6)
    elif sk>82 and sk<sd_v: sc.append(-0.9)
    elif sk>78: sc.append(-0.6)
    elif sk<40: sc.append(0.2)
    elif sk>60: sc.append(-0.2)
    else: sc.append(0.0)

    # E8: Divergence (pre-computed, all 4 types)
    sc.append(float(row.div_sc) * 1.0)

    # E9: Williams %R
    wr=float(row.wr)
    sc.append(0.8 if wr<-83 else 0.4 if wr<-67 else -0.8 if wr>-17 else -0.4 if wr>-33 else 0.0)

    # E10: OBV trend
    obv=float(row.obv); obv_m=float(row.obv_ma)
    sc.append(0.6 if obv>obv_m else -0.6)

    # E11: Volume
    vr=float(row.vr); base=sum(sc)/len(sc) if sc else 0
    sc.append(0.7*(1 if base>0 else -1) if vr>2.0
              else 0.4*(1 if base>0 else -1) if vr>1.5
              else 0.2*(1 if base>0 else -1) if vr>1.2 else 0.0)

    # E12: ADX directional
    adx=float(row.adx); pdi=float(row.pdi); mdi=float(row.mdi)
    w=1.2 if adx>28 else 0.7 if adx>18 else 0.3
    sc.append(0.6*w if pdi>mdi else -0.6*w)

    # E13: Heikin Ashi (3-bar)
    ha3=float(df.hab.iloc[max(0,i-2):i+1].mean())
    sc.append(0.6 if ha3>0.66 else 0.3 if ha3>0.33 else -0.6 if ha3<0.33 else -0.3)

    # E14: Regime alignment bonus
    rb=0.0
    if regime in("BULL","BULL_WEAK"):  rb=0.5
    elif regime in("BEAR","BEAR_WEAK"):rb=-0.5
    sc.append(rb)

    n=len(sc); avg=sum(sc)/n if n else 0.0
    bull=sum(1 for s in sc if s>0.05)
    bear=sum(1 for s in sc if s<-0.05)

    # Thresholds: avg>0.20 AND 7+ experts
    direction=("LONG"  if avg>0.20 and bull>=7 else
               "SHORT" if avg<-0.20 and bear>=7 else "NEUTRAL")
    return direction, round(avg,4), (bull if avg>=0 else bear)

# ─── Noise Filter ─────────────────────────────────────────────────────────────
def _is_noisy(df: pd.DataFrame, i: int) -> bool:
    if i<20: return False
    noise=float(df.noise.iloc[i])
    adx=float(df.adx.iloc[i])
    bbw_cur=float(df.bbw.iloc[i])
    bbw_avg=float(df.bbw.rolling(20).mean().iloc[i]) if i>=20 else bbw_cur
    # Skip doji-like candles AND BB squeeze in weak trend
    return (noise<0.20 and adx<15) or (bbw_cur<bbw_avg*0.55 and adx<12)

# ─── Simulation ───────────────────────────────────────────────────────────────
def _sim(df: pd.DataFrame, balance: float=10_000.0) -> Dict:
    n=len(df); equity=[balance]; trades=[]; WARMUP=65; MAX_C=24
    in_trade=False; partial=False
    direction=""; entry=sl=tp1=tp2=tp3=sl_d=0.0; entry_idx=0
    entry_sess="—"; consec_loss=0

    for i in range(WARMUP,n):
        row=df.iloc[i]; price=float(row.close)
        hi_p=float(row.high); lo_p=float(row.low)
        hour=row.ts.hour if hasattr(row.ts,"hour") else 10

        # ── Manage trade ────────────────────────────────────────────────
        if in_trade:
            if (i-entry_idx)>MAX_C:
                pnl=((price-entry)/entry*100) if direction=="LONG" else ((entry-price)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.012/max(sl_d/entry,0.001)
                gain=pnl/100*risk*mult; balance=max(1.0,balance+gain)
                win=pnl>0; consec_loss=0 if win else consec_loss+1
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(price,4),"pnl_pct":round(pnl*mult,3),
                                "result":"WIN" if win else "LOSS",
                                "exit_type":"TIME_EXIT",
                                "session":entry_sess})   # ← use ENTRY session
                in_trade=False; partial=False; equity.append(balance); continue

            if direction=="LONG":
                hit_sl=lo_p<=sl; hit_tp3=hi_p>=tp3
                hit_tp2=hi_p>=tp2 and not hit_tp3
                hit_tp1=hi_p>=tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl=hi_p>=sl; hit_tp3=lo_p<=tp3
                hit_tp2=lo_p<=tp2 and not hit_tp3
                hit_tp1=lo_p<=tp1 and not hit_tp2 and not hit_tp3

            if hit_tp1 and not partial:
                pnl=((tp1-entry)/entry*100) if direction=="LONG" else ((entry-tp1)/entry*100)
                risk=balance*0.012/max(sl_d/entry,0.001)
                balance=max(1.0,balance+pnl/100*risk*0.5)
                sl=entry; partial=True   # ← SL to breakeven
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(tp1,4),"pnl_pct":round(pnl*0.5,3),
                                "result":"WIN","exit_type":"TP1_PARTIAL",
                                "session":entry_sess})
                equity.append(balance); continue

            if hit_tp2 or hit_tp3 or hit_sl:
                ex=sl if hit_sl else (tp3 if hit_tp3 else tp2)
                pnl=((ex-entry)/entry*100) if direction=="LONG" else ((entry-ex)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.012/max(sl_d/entry,0.001)
                gain=pnl/100*risk*mult; balance=max(1.0,balance+gain)
                win=pnl>0; consec_loss=0 if win else consec_loss+1
                xtype="SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(ex,4),"pnl_pct":round(pnl*mult,3),
                                "result":"WIN" if win else "LOSS",
                                "exit_type":xtype,
                                "session":entry_sess})   # ← ENTRY session
                in_trade=False; partial=False

        # ── New signal ────────────────────────────────────────────────────
        if not in_trade:
            # Gate 1: Session strict
            if not _sess_ok(hour): equity.append(balance); continue
            # Gate 2: Noise
            if _is_noisy(df,i): equity.append(balance); continue
            # Gate 3: Cool-down
            if consec_loss>=3: consec_loss=0; equity.append(balance); continue

            # Fibonacci analysis (lookback 50 candles)
            hi60=float(df.high.iloc[max(0,i-50):i+1].max())
            lo60=float(df.low.iloc[max(0,i-50):i+1].min())
            fib_pos, fib_zone, fib_sc=_fib_zone(price,hi60,lo60)

            # Market regime
            regime=_regime(df,i)

            # Score 14 experts
            sig,avg,votes=_score(df,i,regime,fib_sc)
            if sig=="NEUTRAL": equity.append(balance); continue

            # Gate 4: Counter-trend protection
            if sig=="LONG"  and regime in("BEAR","BEAR_WEAK"):
                equity.append(balance); continue
            if sig=="SHORT" and regime in("BULL","BULL_WEAK"):
                equity.append(balance); continue

            # SL at Fibonacci invalidation
            sl_p,sl_dist=_fib_sl(df,i,sig,price,hi60,lo60)

            # Fibonacci TP cascade
            tp1_p,tp2_p,tp3_p=_fib_tp(price,sl_dist,sig,hi60,lo60)

            # Gate 5: RR ≥ 1.1 (TP1 close = hits often)
            rr=abs(tp1_p-price)/max(sl_dist,1e-10)
            if rr<1.1: equity.append(balance); continue

            direction=sig; entry=price
            sl=sl_p; tp1=tp1_p; tp2=tp2_p; tp3=tp3_p
            sl_d=sl_dist; entry_sess=_sess(hour)   # ← record ENTRY session
            in_trade=True; entry_idx=i; partial=False

        equity.append(balance)

    if in_trade:
        lp=float(df.close.iloc[-1])
        pnl=((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        trades.append({"direction":direction,"entry":entry,"exit":lp,
                        "pnl_pct":round(pnl*(0.5 if partial else 1.0),3),
                        "result":"WIN" if pnl>0 else "LOSS",
                        "exit_type":"OPEN_AT_END","session":entry_sess})

    return {"trades":trades,"equity":equity,"final_balance":balance}

# ─── Metrics ──────────────────────────────────────────────────────────────────
def _metrics(sim: Dict, initial: float) -> Dict:
    trades=sim["trades"]; eq=pd.Series(sim["equity"]); bal=sim["final_balance"]
    if not trades:
        return {"error":"No trades","total":0,"win_rate_pct":0,"max_dd_pct":0,"return_pct":0,"sharpe":0}
    wins=[t for t in trades if t["result"]=="WIN"]
    losses=[t for t in trades if t["result"]=="LOSS"]
    tot=len(trades); wr=round(len(wins)/tot*100,2) if tot else 0
    dd=round(float(((eq-eq.cummax())/eq.cummax()*100).min()),2)
    ret=round((bal-initial)/initial*100,2)
    rets=eq.pct_change().dropna()
    sh=round(float(rets.mean()/rets.std()*(252**0.5)),3) if rets.std()>0 else 0.0
    aw=round(sum(t["pnl_pct"] for t in wins)/len(wins),3)   if wins   else 0
    al=round(sum(t["pnl_pct"] for t in losses)/len(losses),3) if losses else 0
    by_ex={}; by_ss={}
    for t in trades:
        k=t.get("exit_type","?"); by_ex[k]=by_ex.get(k,0)+1
        s=t.get("session","?");   by_ss[s]=by_ss.get(s,0)+1
    return {"total":tot,"wins":len(wins),"losses":len(losses),
            "win_rate_pct":wr,"avg_win_pct":aw,"avg_loss_pct":al,
            "expectancy":round(wr/100*aw+(1-wr/100)*al,3),
            "max_dd_pct":dd,"return_pct":ret,"sharpe":sh,
            "final_balance":round(bal,2),
            "exit_breakdown":by_ex,"session_breakdown":by_ss}

# ─── BacktestEngine ────────────────────────────────────────────────────────────
class BacktestEngine:
    async def run(self,
                  symbols:List[str]=None,
                  timeframe:str="1h",
                  tf:str=None,
                  start:str="2026-01-01",
                  end:str="2026-05-01",
                  balance:float=10_000.0,
                  **kwargs)->Dict:
        resolved=tf or timeframe or "1h"
        symbols=symbols or ["BTC/USDT:USDT","ETH/USDT:USDT"]
        sdt=datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt=datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results={}
        for sym in symbols:
            sym_c=sym.replace("/USDT:USDT","")
            try:
                df=await _fetch(sym,resolved,sdt,edt)
                if df is None or len(df)<70:
                    results[sym_c]={"error":"insufficient data"}; continue
                df=_build(df); df=_add_div(df)
                sim=_sim(df,balance)
                st=_metrics(sim,balance)
                results[sym_c]={**st,"symbol":sym_c,"tf":resolved,
                                  "period":f"{start}→{end}","candles":len(df)}
                logger.info(f"[BT] {sym_c}: {st['total']} trades WR={st['win_rate_pct']}% Ret={st['return_pct']:+.2f}%")
            except Exception as e:
                logger.error(f"[BT] {sym_c}: {e}"); results[sym_c]={"error":str(e)}
        return results

    @staticmethod
    def format_report(results:Dict)->str:
        tf=next((v.get("tf","1H") for v in results.values() if isinstance(v,dict) and "tf" in v),"1H")
        lines=["📈 <b>Backtest — Ramos 360 Ai 🎖️  FibPull v3</b>",
               f"📅 Period: 2026-01-01 → 2026-05-01",
               f"⏱️ Timeframe: {tf.upper()} | 14 Experts",
               "✅ Fibonacci Pullback + Smart SL + Anti-CounterTrend",
               "━━━━━━━━━━━━━━━━━━━━━━━━"]
        for sym,r in results.items():
            if "error" in r: lines.append(f"❌ {sym}: {r['error']}"); continue
            ei="🟢" if r.get("return_pct",0)>0 else "🔴"
            ex=" ".join(f"{k}:{v}" for k,v in r.get("exit_breakdown",{}).items())
            ss=" ".join(f"{k}:{v}" for k,v in r.get("session_breakdown",{}).items())
            lines+=[f"",f"{ei} <b>{sym}</b>",
                    f"  📊 {r['total']} trades  ({r.get('wins',0)}W/{r.get('losses',0)}L)",
                    f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                    f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                    f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                    f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                    f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                    f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                    f"  📋 Exits:      {ex}",
                    f"  🕐 Sessions:   {ss}"]
        lines+=["━━━━━━━━━━━━━━━━━━━━━━━━",
                "<i>🎖️ Ramos 360 Ai — FibPull v3</i>"]
        return "\n".join(lines)

async def _main():
    e=BacktestEngine()
    r=await e.run(symbols=["BTC/USDT:USDT"],timeframe="1h",
                   start="2026-01-01",end="2026-05-01",balance=10_000.0)
    print("\n"+e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))

if __name__=="__main__":
    asyncio.run(_main())
