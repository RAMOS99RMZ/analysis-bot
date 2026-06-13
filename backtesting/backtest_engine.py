"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  v3
══════════════════════════════════════════════════════
ALL 8 IMPROVEMENTS APPLIED:
  1. Session Filter FIXED  (hours 7-17 UTC strict)
  2. ATR × 2.0 wider SL    (fewer SL hits)
  3. Min 8/14 votes         (higher quality signals)
  4. Confirmation Candle    (wait next bar close)
  5. Partial Exit @ TP1     (50% close + breakeven SL)
  6. ADX > 20 filter        (no sideways markets)
  7. 4H HTF Confirmation    (higher TF alignment)
  8. Time-Based Exit        (max 12 candles in trade)
══════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio, math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import httpx
import pandas as pd
import numpy as np
from loguru import logger

try:
    import pandas_ta as ta
    HAS_TA = True
except Exception:
    HAS_TA = False

_BASE = "https://www.okx.com/api/v5"
_HDR  = {"Accept": "application/json", "User-Agent": "Ramos360BT/3.0"}
_TF   = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
         "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

# ═══════════════════════════════════════
# IMPROVEMENT 1 — SESSION FILTER FIXED
# ═══════════════════════════════════════
# Only trade 07:00–16:59 UTC (London + NY)
ALLOWED_HOURS = set(range(7, 17))   # 07,08,...,16

def _session(h: int) -> str:
    if 13 <= h < 16: return "OVERLAP"
    if  7 <= h < 12: return "LONDON"
    if 13 <= h < 17: return "NEW_YORK"
    if  0 <= h <  7: return "ASIA"
    return "DEAD_ZONE"

def _session_ok(h: int) -> bool:
    """FIXED: strict hours 07:00-16:59 UTC only"""
    return h in ALLOWED_HOURS

def _session_q(h: int) -> float:
    return {"OVERLAP":1.5,"LONDON":1.2,"NEW_YORK":1.1,
            "ASIA":0.4,"DEAD_ZONE":0.2}.get(_session(h), 1.0)


# ═══════════════════════════════════════
# OKX FETCH WITH PAGINATION
# ═══════════════════════════════════════

def _inst(s: str) -> str:
    return s.split(":")[0].replace("/","-") + "-SWAP"

async def _fetch(symbol: str, tf: str,
                  start: datetime, end: datetime) -> pd.DataFrame:
    inst      = _inst(symbol)
    bar       = _TF.get(tf.lower().replace("h","h"), tf)
    start_ms  = int(start.timestamp()*1000)
    end_ms    = int(end.timestamp()*1000)
    rows:List[List] = []
    before_ms = end_ms
    logger.info(f"[BT] Fetching {symbol} {tf} {start.date()} → {end.date()} …")
    async with httpx.AsyncClient(timeout=30, headers=_HDR) as cl:
        while True:
            r = await cl.get(f"{_BASE}/market/history-candles", params={
                "instId":inst,"bar":bar,
                "before":str(start_ms),"after":str(before_ms),"limit":"300",
            })
            if r.status_code == 429: await asyncio.sleep(5); continue
            if r.status_code != 200: break
            j = r.json()
            if j.get("code")!="0" or not j.get("data"): break
            batch = j["data"]
            for c in batch:
                ts = int(c[0])
                if start_ms <= ts <= end_ms:
                    rows.append([ts,float(c[1]),float(c[2]),
                                  float(c[3]),float(c[4]),float(c[5])])
            oldest = int(batch[-1][0])
            if oldest <= start_ms or len(batch) < 300: break
            before_ms = oldest
            await asyncio.sleep(0.3)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume"])
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    logger.info(f"[BT] {symbol}: {len(df)} candles ✅")
    return df


# ═══════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════

def _rsi(s:pd.Series, p:int=14)->pd.Series:
    if HAS_TA:
        try:
            v=ta.rsi(s,length=p)
            if v is not None: return v.fillna(50)
        except Exception: pass
    d=s.diff(); g=d.clip(lower=0).ewm(alpha=1/p,adjust=False).mean()
    l=(-d.clip(upper=0)).ewm(alpha=1/p,adjust=False).mean()
    return (100-100/(1+g/l.replace(0,1e-10))).fillna(50)

def _ema(s:pd.Series,p:int)->pd.Series:
    if HAS_TA:
        try:
            v=ta.ema(s,length=p)
            if v is not None: return v.ffill().bfill()
        except Exception: pass
    return s.ewm(span=p,adjust=False).mean()

def _macd_h(s:pd.Series)->pd.Series:
    if HAS_TA:
        try:
            v=ta.macd(s,fast=12,slow=26,signal=9)
            if v is not None and not v.empty: return v.iloc[:,2].fillna(0)
        except Exception: pass
    e12=s.ewm(span=12,adjust=False).mean(); e26=s.ewm(span=26,adjust=False).mean()
    ms=e12-e26; return (ms-ms.ewm(span=9,adjust=False).mean()).fillna(0)

def _atr_s(df:pd.DataFrame,p:int=14)->pd.Series:
    if HAS_TA:
        try:
            v=ta.atr(df.high,df.low,df.close,length=p)
            if v is not None: return v.ffill().bfill()
        except Exception: pass
    hl=df.high-df.low; hpc=(df.high-df.close.shift()).abs()
    lpc=(df.low-df.close.shift()).abs()
    return pd.concat([hl,hpc,lpc],axis=1).max(axis=1).ewm(alpha=1/p,adjust=False).mean()

def _adx_s(df:pd.DataFrame,p:int=14)->pd.Series:
    if HAS_TA:
        try:
            v=ta.adx(df.high,df.low,df.close,length=p)
            if v is not None and not v.empty:
                adx_cols=[c for c in v.columns if "ADX_" in c]
                if adx_cols: return v[adx_cols[0]].fillna(20)
        except Exception: pass
    up=(df.high-df.high.shift()).clip(lower=0)
    dn=(df.low.shift()-df.low).clip(lower=0)
    pdi=up.ewm(alpha=1/p,adjust=False).mean()
    mdi=dn.ewm(alpha=1/p,adjust=False).mean()
    return ((pdi-mdi).abs()/(pdi+mdi+1e-10)*100).ewm(alpha=1/p,adjust=False).mean()

def _build(df:pd.DataFrame)->pd.DataFrame:
    df=df.copy(); c=df.close
    df["rsi"]=_rsi(c,14); df["rsi6"]=_rsi(c,6)
    df["mh"]=_macd_h(c); df["atr"]=_atr_s(df,14)
    df["adx"]=_adx_s(df,14)                        # IMPROVEMENT 6
    df["wr"]=(-100*(df.high.rolling(14).max()-c)/(df.high.rolling(14).max()-df.low.rolling(14).min()+1e-10)).fillna(-50)
    df["obv"]=(df.close.diff().apply(lambda x:1 if x>0 else -1 if x<0 else 0)*df.volume).cumsum()
    df["volma"]=df.volume.rolling(20).mean(); df["volr"]=df.volume/(df.volma.replace(0,1))
    for p in [9,20,50,100,200]: df[f"e{p}"]=_ema(c,p)
    # Ichimoku
    df["iten"]=(df.high.rolling(9).max()+df.low.rolling(9).min())/2
    df["ikij"]=(df.high.rolling(26).max()+df.low.rolling(26).min())/2
    df["ict"]=pd.concat([(df.iten+df.ikij)/2,(df.high.rolling(52).max()+df.low.rolling(52).min())/2],axis=1).max(axis=1)
    df["icb"]=pd.concat([(df.iten+df.ikij)/2,(df.high.rolling(52).max()+df.low.rolling(52).min())/2],axis=1).min(axis=1)
    # BB
    mid=c.rolling(20).mean(); sd=c.rolling(20).std()
    df["bbu"]=mid+2*sd; df["bbl"]=mid-2*sd
    df["bbp"]=(c-df.bbl)/(df.bbu-df.bbl+1e-10)*100
    # Stoch
    lo14=df.low.rolling(14).min(); hi14=df.high.rolling(14).max()
    df["stoch"]=100*(c-lo14)/(hi14-lo14+1e-10)
    # Heikin Ashi
    df["hac"]=(df.open+df.high+df.low+df.close)/4; df["hao"]=df.hac.shift(2)
    df["hab"]=(df.hac>df.hao).astype(int)
    # Wyckoff
    df["wyck"]=0.0
    df.loc[(c>df.e50)&(df.volr>1.2),"wyck"]=1.0
    df.loc[(c<df.e50)&(df.volr>1.2),"wyck"]=-1.0
    return df.ffill().bfill()


# ═══════════════════════════════════════
# IMPROVEMENT 7 — HTF (4H) BIAS
# ═══════════════════════════════════════

def _build_htf_bias(df_htf: Optional[pd.DataFrame]) -> pd.Series:
    """
    Returns a Series of 4H bias values (-1, 0, +1)
    aligned to the 1H dataframe index by forward-fill.
    """
    if df_htf is None or df_htf.empty:
        return pd.Series(dtype=float)
    df4 = _build(df_htf.copy())
    bias_vals = []
    for i in range(len(df4)):
        c   = float(df4.close.iloc[i])
        e20 = float(df4.e20.iloc[i])
        e50 = float(df4.e50.iloc[i])
        w   = float(df4.wyck.iloc[i])
        sc  = 0.0
        if c > e20 > e50: sc += 1.0
        elif c < e20 < e50: sc -= 1.0
        sc += w * 0.5
        bias_vals.append(1 if sc > 0.4 else -1 if sc < -0.4 else 0)
    return pd.Series(bias_vals, index=df4.ts)


# ═══════════════════════════════════════
# LAYER 1 BIAS
# ═══════════════════════════════════════

def _l1(df:pd.DataFrame,i:int)->Tuple[str,float]:
    if i<50: return "NEUTRAL",0.0
    row=df.iloc[i]; c=float(row.close)
    sc=[float(row.wyck)]
    e20,e50,e200=float(row.e20),float(row.e50),float(row.get("e200",row.e50))
    if c>e20>e50>e200:  sc.append(1.0)
    elif c>e20>e50:     sc.append(0.5)
    elif c<e20<e50<e200:sc.append(-1.0)
    elif c<e20<e50:     sc.append(-0.5)
    else:               sc.append(0.0)
    wr=float(row.wr)
    sc.append(0.8 if wr<-80 else -0.8 if wr>-20 else 0.0)
    avg=sum(sc)/len(sc) if sc else 0.0
    return ("BULL" if avg>0.25 else "BEAR" if avg<-0.25 else "NEUTRAL"), round(avg,3)


# ═══════════════════════════════════════
# DIVERGENCE
# ═══════════════════════════════════════

def _div(df:pd.DataFrame,i:int)->float:
    if i<30: return 0.0
    ps=df.close.iloc[max(0,i-40):i+1]
    rs=df.rsi.iloc[max(0,i-40):i+1]
    lows=[]; highs=[]
    for j in range(5,len(ps)-5):
        w=ps.iloc[j-5:j+6]
        if len(w)<11: continue
        v=ps.iloc[j]
        if v==w.min(): lows.append((j,float(v),float(rs.iloc[j])))
        if v==w.max(): highs.append((j,float(v),float(rs.iloc[j])))
    sc=0.0
    if len(lows)>=2:
        l1,l2=lows[-2],lows[-1]
        if l2[1]<l1[1] and l2[2]>l1[2]: sc+=0.6   # Reg Bull
        if l2[1]>l1[1] and l2[2]<l1[2]: sc+=0.4   # Hid Bull
    if len(highs)>=2:
        h1,h2=highs[-2],highs[-1]
        if h2[1]>h1[1] and h2[2]<h1[2]: sc-=0.6   # Reg Bear
        if h2[1]<h1[1] and h2[2]>h1[2]: sc-=0.4   # Hid Bear
    vr=float(df.volr.iloc[i]) if i<len(df) else 1.0
    if vr>1.5 and abs(sc)>0: sc*=1.2
    return round(max(-1.0,min(1.0,sc)),3)


# ═══════════════════════════════════════
# SMC
# ═══════════════════════════════════════

def _smc(df:pd.DataFrame,i:int)->float:
    if i<12: return 0.0
    sc=0.0; c=float(df.close.iloc[i])
    rh=float(df.high.iloc[max(0,i-5):i+1].max())
    rl=float(df.low.iloc[max(0,i-5):i+1].min())
    ph=float(df.high.iloc[max(0,i-11):i-5].max()) if i>=11 else rh
    pl=float(df.low.iloc[max(0,i-11):i-5].min())  if i>=11 else rl
    if rh>ph*1.003 and c>ph: sc+=0.7
    elif rl<pl*0.997 and c<pl: sc-=0.7
    prev_lo=float(df.low.iloc[max(0,i-7):i].min())
    prev_hi=float(df.high.iloc[max(0,i-7):i].max())
    if float(df.low.iloc[i])<prev_lo*0.999 and c>prev_lo: sc+=0.5
    if float(df.high.iloc[i])>prev_hi*1.001 and c<prev_hi: sc-=0.5
    return round(max(-1.0,min(1.0,sc)),3)


# ═══════════════════════════════════════
# FIBONACCI
# ═══════════════════════════════════════

def _fib(df:pd.DataFrame,i:int)->float:
    if i<30: return 0.0
    hi=float(df.high.iloc[max(0,i-60):i+1].max())
    lo=float(df.low.iloc[max(0,i-60):i+1].min())
    rng=hi-lo
    if rng<=0: return 0.0
    c=float(df.close.iloc[i]); pos=(c-lo)/rng
    BULL=[(0.289,0.329),(0.3845,0.4245),(0.598,0.638)]
    BEAR=[(0.730,0.770),(0.760,0.800),(0.789,0.829)]
    for a,b in BULL:
        if a<=pos<=b: return 0.8
    for a,b in BEAR:
        if a<=pos<=b: return -0.8
    return 0.5 if pos<0.35 else -0.5 if pos>0.65 else 0.0


# ═══════════════════════════════════════
# IMPROVEMENT 3 — 14 EXPERTS (need 8 votes)
# ═══════════════════════════════════════

def _experts(df:pd.DataFrame, i:int, l1:str) -> Tuple[str,float,int]:
    row=df.iloc[i]; c=float(row.close); sc=[]

    # L1 bias (weight ×2)
    if l1=="BULL":  sc+=[0.8,0.8]
    elif l1=="BEAR":sc+=[-0.8,-0.8]

    # E1: RSI
    r=float(row.rsi)
    sc.append(0.9 if r<30 else 0.5 if r<40 else -0.9 if r>70 else -0.5 if r>60 else 0.0)

    # E2: MACD histogram
    sc.append(0.7 if float(row.mh)>0 else -0.7)

    # E3: EMA alignment
    e20,e50=float(row.e20),float(row.e50)
    e200=float(row.get("e200",row.e50))
    if c>e20>e50>e200:   sc.append(1.0)
    elif c>e20>e50:      sc.append(0.5)
    elif c<e20<e50<e200: sc.append(-1.0)
    elif c<e20<e50:      sc.append(-0.5)
    else:                sc.append(0.0)

    # E4: Ichimoku
    ct,cb=float(row.ict),float(row.icb)
    tn,kj=float(row.iten),float(row.ikij)
    if c>ct and tn>kj:   sc.append(1.0)
    elif c>ct:           sc.append(0.5)
    elif c<cb and tn<kj: sc.append(-1.0)
    elif c<cb:           sc.append(-0.5)
    else:                sc.append(0.0)

    # E5: Bollinger Bands
    bp=float(row.bbp)
    sc.append(0.8 if bp<10 else 0.3 if bp<25 else -0.8 if bp>90 else -0.3 if bp>75 else 0.0)

    # E6: Stochastic
    sk=float(row.stoch)
    sc.append(0.7 if sk<20 else 0.3 if sk<35 else -0.7 if sk>80 else -0.3 if sk>65 else 0.0)

    # E7: SMC
    sc.append(_smc(df,i))

    # E8: Fibonacci
    sc.append(_fib(df,i))

    # E9: Divergence (all 4 types)
    sc.append(_div(df,i))

    # E10: Gann Sq9
    root=math.sqrt(max(c,0.001))
    g=[round((root+s)**2,2) for s in [-1,-.5,0,.5,1] if round((root+s)**2,2)>0]
    if g:
        near=min(g,key=lambda x:abs(x-c)); d=(near-c)/c
        sc.append(0.5 if abs(d)<0.003 else 0.3 if d>0 else -0.3)
    else: sc.append(0.0)

    # E11: Volume
    vr=float(row.volr); base=sum(sc)/len(sc) if sc else 0
    sc.append(0.4*(1 if base>0 else -1) if vr>1.5 else 0.0)

    # E12: Wyckoff
    sc.append(float(row.wyck)*0.8)

    # E13: Williams %R
    wr=float(row.wr)
    sc.append(0.7 if wr<-80 else -0.7 if wr>-20 else 0.0)

    # E14: Heikin Ashi
    sc.append(0.4 if float(row.hab)==1 else -0.4)

    n=len(sc); avg=sum(sc)/n if n else 0.0
    bull=sum(1 for s in sc if s>0)
    bear=sum(1 for s in sc if s<0)

    # IMPROVEMENT 3: need 8 votes minimum
    direction=("LONG"  if avg> 0.30 and bull>=8 else
               "SHORT" if avg<-0.30 and bear>=8 else "NEUTRAL")
    votes=bull if avg>0 else bear
    return direction, round(avg,4), votes


# ═══════════════════════════════════════
# SIMULATION WITH ALL 8 IMPROVEMENTS
# ═══════════════════════════════════════

def _simulate(df: pd.DataFrame,
               balance: float = 10_000.0,
               risk_pct: float = 0.01,
               use_sessions: bool = True,
               htf_bias_series: Optional[pd.Series] = None) -> Dict:
    n       = len(df)
    equity  = [balance]
    trades  = []
    WARMUP  = 60
    MAX_CANDLES = 12    # IMPROVEMENT 8: time-based exit

    # Trade state
    in_trade    = False
    partial_done= False
    direction   = ""
    entry       = sl = tp1 = tp2 = tp3 = atr_v = 0.0
    entry_idx   = 0
    pending_sig = None   # IMPROVEMENT 4: confirmation candle
    pending_idx = -1

    for i in range(WARMUP, n):
        row   = df.iloc[i]
        price = float(row.close)
        hi    = float(row.high)
        lo    = float(row.low)
        hour  = row.ts.hour if hasattr(row.ts,"hour") else 10

        # ─── Check open trade ───────────────────────────────────────────
        if in_trade:

            # IMPROVEMENT 8: Time-based exit
            if (i - entry_idx) > MAX_CANDLES:
                pnl = ((price-entry)/entry*100) if direction=="LONG" \
                       else ((entry-price)/entry*100)
                risk  = balance*risk_pct/max(abs(entry-sl)/entry,0.001)
                gain  = pnl/100*risk
                balance = max(1.0, balance+gain)
                trades.append({
                    "direction":direction,"entry":round(entry,4),
                    "exit":round(price,4),"pnl_pct":round(pnl,3),
                    "result":"WIN" if pnl>0 else "LOSS",
                    "exit_type":"TIME_EXIT","atr":round(atr_v,4),
                    "session":_session(hour),
                })
                in_trade=False; partial_done=False
                equity.append(balance); continue

            if direction=="LONG":
                hit_sl =lo<=sl;  hit_tp3=hi>=tp3
                hit_tp2=hi>=tp2 and not hit_tp3
                hit_tp1=hi>=tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl =hi>=sl;  hit_tp3=lo<=tp3
                hit_tp2=lo<=tp2 and not hit_tp3
                hit_tp1=lo<=tp1 and not hit_tp2 and not hit_tp3

            # IMPROVEMENT 5: Partial Exit at TP1
            if hit_tp1 and not partial_done:
                # Close 50% at TP1
                pnl_p = ((tp1-entry)/entry*100) if direction=="LONG" \
                         else ((entry-tp1)/entry*100)
                risk  = balance*risk_pct/max(abs(entry-sl)/entry,0.001)
                gain  = pnl_p/100*risk*0.50    # 50% of position
                balance = max(1.0, balance+gain)
                sl    = entry   # Move SL to breakeven
                partial_done = True
                trades.append({
                    "direction":direction,"entry":round(entry,4),
                    "exit":round(tp1,4),"pnl_pct":round(pnl_p*0.5,3),
                    "result":"WIN","exit_type":"TP1_PARTIAL",
                    "atr":round(atr_v,4),"session":_session(hour),
                })
                equity.append(balance); continue

            if hit_tp2 or hit_tp3 or hit_sl:
                ex  = sl if hit_sl else (tp3 if hit_tp3 else tp2)
                pnl = ((ex-entry)/entry*100) if direction=="LONG" \
                       else ((entry-ex)/entry*100)
                # If partial done, only remaining 50%
                mult= 0.50 if partial_done else 1.0
                risk= balance*risk_pct/max(abs(entry-sl)/entry,0.001)
                gain= pnl/100*risk*mult
                balance = max(1.0, balance+gain)
                xtype="SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({
                    "direction":direction,"entry":round(entry,4),
                    "exit":round(ex,4),"pnl_pct":round(pnl*mult,3),
                    "result":"WIN" if pnl>0 else "LOSS",
                    "exit_type":xtype,"atr":round(atr_v,4),
                    "session":_session(hour),
                })
                in_trade=False; partial_done=False

        # ─── IMPROVEMENT 4: Confirmation Candle Logic ───────────────────
        if pending_sig and i == pending_idx:
            # Enter on this confirmed bar
            direction  = pending_sig
            entry      = price
            pending_sig= None; pending_idx = -1

            atr_v = float(df.atr.iloc[i-1]) if i>0 and float(df.atr.iloc[i-1])>0 else price*0.02

            # IMPROVEMENT 2: ATR × 2.0 wider SL
            if direction=="LONG":
                sl  = round(price - atr_v*2.0, 4)
                tp1 = round(price + atr_v*1.5, 4)
                tp2 = round(price + atr_v*3.0, 4)
                tp3 = round(price + atr_v*5.0, 4)
            else:
                sl  = round(price + atr_v*2.0, 4)
                tp1 = round(price - atr_v*1.5, 4)
                tp2 = round(price - atr_v*3.0, 4)
                tp3 = round(price - atr_v*5.0, 4)

            # Validate RR ≥ 1.5
            rr = abs(tp1-price)/max(abs(sl-price),1e-10)
            if rr >= 1.5:
                in_trade   = True
                entry_idx  = i
                partial_done = False
            equity.append(balance); continue

        # ─── Generate new signal ────────────────────────────────────────
        if not in_trade and pending_sig is None:

            # IMPROVEMENT 1: Session filter FIXED
            if use_sessions and not _session_ok(hour):
                equity.append(balance); continue

            # IMPROVEMENT 6: ADX filter > 20
            adx_val = float(row.adx) if hasattr(row,"adx") else 20.0
            if adx_val < 20:
                equity.append(balance); continue

            # Layer 1 bias
            l1, l1sc = _l1(df, i)
            if l1 == "NEUTRAL":
                equity.append(balance); continue

            # IMPROVEMENT 7: HTF (4H) Confirmation
            if htf_bias_series is not None and not htf_bias_series.empty:
                ts_now = row.ts
                htf_slice = htf_bias_series[htf_bias_series.index <= ts_now]
                if not htf_slice.empty:
                    htf_val = int(htf_slice.iloc[-1])
                    if l1=="BULL" and htf_val==-1:
                        equity.append(balance); continue
                    if l1=="BEAR" and htf_val==1:
                        equity.append(balance); continue

            # IMPROVEMENT 3: 14 experts, need 8 votes
            sig, avg, votes = _experts(df, i, l1)
            if sig=="NEUTRAL":
                equity.append(balance); continue
            if abs(avg) < 0.35 or votes < 8:
                equity.append(balance); continue

            # Session quality gate
            if _session_q(hour) < 0.8:
                equity.append(balance); continue

            # IMPROVEMENT 4: Queue for next bar confirmation
            pending_sig = sig
            pending_idx = i + 1   # Enter on NEXT candle open/close

        equity.append(balance)

    # Close any open trade at end
    if in_trade:
        lp  = float(df.close.iloc[-1])
        pnl = ((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        mult= 0.5 if partial_done else 1.0
        trades.append({
            "direction":direction,"entry":entry,"exit":lp,
            "pnl_pct":round(pnl*mult,3),
            "result":"WIN" if pnl>0 else "LOSS",
            "exit_type":"OPEN_AT_END","atr":round(atr_v,4),
        })

    return {"trades":trades,"equity":equity,"final_balance":balance}


# ═══════════════════════════════════════
# METRICS
# ═══════════════════════════════════════

def _metrics(sim:Dict, initial:float)->Dict:
    trades=sim["trades"]; eq=pd.Series(sim["equity"]); bal=sim["final_balance"]
    if not trades:
        return {"error":"No trades","total":0,"win_rate_pct":0,
                "max_dd_pct":0,"return_pct":0,"sharpe":0}
    wins  =[t for t in trades if t["result"]=="WIN"]
    losses=[t for t in trades if t["result"]=="LOSS"]
    total =len(trades); wr=round(len(wins)/total*100,2) if total else 0
    dd    =round(float(((eq-eq.cummax())/eq.cummax()*100).min()),2)
    ret   =round((bal-initial)/initial*100,2)
    rets  =eq.pct_change().dropna()
    sh    =round(float(rets.mean()/rets.std()*(252**0.5)),3) if rets.std()>0 else 0.0
    aw    =round(sum(t["pnl_pct"] for t in wins)/len(wins),3) if wins else 0
    al    =round(sum(t["pnl_pct"] for t in losses)/len(losses),3) if losses else 0
    by_ex={};by_ss={}
    for t in trades:
        k=t.get("exit_type","?"); by_ex[k]=by_ex.get(k,0)+1
        s=t.get("session","?");   by_ss[s]=by_ss.get(s,0)+1
    return {
        "total":total,"wins":len(wins),"losses":len(losses),
        "win_rate_pct":wr,"avg_win_pct":aw,"avg_loss_pct":al,
        "expectancy":round(wr/100*aw+(1-wr/100)*al,3),
        "max_dd_pct":dd,"return_pct":ret,"sharpe":sh,
        "final_balance":round(bal,2),
        "exit_breakdown":by_ex,"session_breakdown":by_ss,
    }


# ═══════════════════════════════════════
# BacktestEngine CLASS
# ═══════════════════════════════════════

class BacktestEngine:
    """
    Ramos 360 Ai Backtester v3 — All 8 improvements active.
    run() accepts: timeframe= OR tf= (both work)
    """

    async def run(self,
                  symbols:      List[str] = None,
                  timeframe:    str       = "1h",
                  tf:           str       = None,
                  start:        str       = "2026-01-01",
                  end:          str       = "2026-05-01",
                  balance:      float     = 10_000.0,
                  use_sessions: bool      = True,
                  trade_type:   str       = "Scalp",
                  **kwargs) -> Dict:

        # Accept both 'timeframe' and 'tf'
        resolved = tf or timeframe or kwargs.get("bar","1h") or "1h"
        symbols  = symbols or ["BTC/USDT:USDT","ETH/USDT:USDT"]
        sdt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        edt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
        results:Dict = {}

        for sym in symbols:
            sym_c = sym.replace("/USDT:USDT","")
            logger.info(f"[BT] ── {sym_c} {resolved} {start}→{end} ──")
            try:
                df = await _fetch(sym, resolved, sdt, edt)
                if df is None or len(df) < 70:
                    results[sym_c]={"error":"insufficient data"}; continue

                # IMPROVEMENT 7: fetch 4H for HTF confirmation
                htf_bias = None
                if resolved in ("1h","15m","5m"):
                    df4h = await _fetch(sym, "4h", sdt, edt)
                    if df4h is not None and len(df4h) >= 50:
                        htf_bias = _build_htf_bias(df4h)

                df    = _build(df)
                sim   = _simulate(df, balance,
                                   use_sessions=use_sessions,
                                   htf_bias_series=htf_bias)
                stats = _metrics(sim, balance)
                results[sym_c]={**stats,"symbol":sym_c,
                                 "tf":resolved,"period":f"{start}→{end}",
                                 "candles":len(df)}
                logger.info(
                    f"[BT] {sym_c}: {stats['total']} trades | "
                    f"WR={stats['win_rate_pct']}% | "
                    f"Ret={stats['return_pct']:+.2f}% | "
                    f"DD={stats['max_dd_pct']:.2f}%"
                )
            except Exception as e:
                logger.error(f"[BT] {sym_c}: {e}")
                results[sym_c]={"error":str(e)}

        return results

    @staticmethod
    def format_report(results:Dict)->str:
        tf_used=next((v.get("tf","1H") for v in results.values()
                      if isinstance(v,dict) and "tf" in v),"1H")
        lines=[
            "📈 <b>Backtest — Ramos 360 Ai 🎖️  v3</b>",
            f"📅 Period: 2026-01-01 → 2026-05-01",
            f"⏱️ Timeframe: {tf_used.upper()} | 14 Experts",
            "✅ Session+ATR×2+Votes≥8+ConfirmCandle",
            "✅ PartialExit+ADX>20+HTF+TimeExit",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
        ]
        for sym,r in results.items():
            if "error" in r: lines.append(f"❌ {sym}: {r['error']}"); continue
            ei="🟢" if r.get("return_pct",0)>0 else "🔴"
            exits=" ".join(f"{k}:{v}" for k,v in r.get("exit_breakdown",{}).items())
            sess =" ".join(f"{k}:{v}" for k,v in r.get("session_breakdown",{}).items())
            lines+=[
                f"",f"{ei} <b>{sym}</b>",
                f"  📊 {r['total']} trades  ({r.get('wins',0)}W / {r.get('losses',0)}L)",
                f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                f"  📋 Exits:      {exits}",
                f"  🕐 Sessions:   {sess}",
            ]
        lines+=["━━━━━━━━━━━━━━━━━━━━━━━━",
                "<i>🎖️ Ramos 360 Ai — Backtest v3 | 8 Improvements</i>"]
        return "\n".join(lines)


async def _main():
    engine  = BacktestEngine()
    results = await engine.run(
        symbols=["BTC/USDT:USDT"],
        timeframe="1h",
        start="2026-01-01",
        end="2026-05-01",
        balance=10_000.0,
        use_sessions=True,
    )
    print("\n"+"="*55)
    print(engine.format_report(results)
          .replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))
    print("="*55)

if __name__ == "__main__":
    asyncio.run(_main())
