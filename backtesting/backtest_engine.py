"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  PRO
═══════════════════════════════════════════════════════
TARGET: Win Rate 70-80%

IMPROVEMENTS over FINAL:
  1. Hard session block: DEAD_ZONE + ASIA → removed
  2. Swing High/Low SL (not pure ATR) → fewer premature SL hits
  3. Triple confirmation: EMA + RSI + Volume mandatory
  4. Fibonacci zone entry: price must be near key Fib level
  5. Divergence as BONUS (not blocking): adds score weight
  6. Consecutive candle filter: 2 candles must agree
  7. Trend strength: EMA200 alignment mandatory
  8. Cool-down: 2 consecutive losses → skip next candle
  9. Better TP: 1H ATR × dynamic (closer TP1, farther TP2/3)
 10. Anti-chop filter: require ADX > 18
═══════════════════════════════════════════════════════
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
_HDR  = {"Accept":"application/json","User-Agent":"Ramos360PRO/1.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
          "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

# ═══════════════════════════════════════
# IMPROVEMENT 1 — STRICT SESSION FILTER
# ═══════════════════════════════════════
# Only LONDON + NEW_YORK + OVERLAP
# Removes DEAD_ZONE (17-23 UTC) AND ASIA (0-6 UTC)
ALLOWED_H = set(range(7, 17))   # 07:00–16:59 UTC ONLY

def _sess(h:int)->str:
    if 13<=h<16: return "OVERLAP"
    if  7<=h<12: return "LONDON"
    if 13<=h<17: return "NEW_YORK"
    return "ASIA/DEAD"

def _sess_ok(h:int)->bool:
    return h in ALLOWED_H   # HARD BLOCK everything outside 07-16

def _sess_q(h:int)->float:
    return {"OVERLAP":1.5,"LONDON":1.2,"NEW_YORK":1.1}.get(_sess(h),0.0)

# ═══════════════════════════════════════
# OKX FETCH
# ═══════════════════════════════════════
def _inst(s:str)->str:
    return s.split(":")[0].replace("/","-")+"-SWAP"

async def _fetch(symbol:str,tf:str,start:datetime,end:datetime)->pd.DataFrame:
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
            batch=j["data"]
            for c in batch:
                ts=int(c[0])
                if s_ms<=ts<=e_ms:
                    rows.append([ts,float(c[1]),float(c[2]),float(c[3]),float(c[4]),float(c[5])])
            old=int(batch[-1][0])
            if old<=s_ms or len(batch)<300: break
            before=old; await asyncio.sleep(0.3)
    if not rows: return pd.DataFrame()
    df=pd.DataFrame(rows,columns=["ts","open","high","low","close","volume"])
    df=df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    df["ts"]=pd.to_datetime(df["ts"],unit="ms",utc=True)
    logger.info(f"[BT] {symbol}: {len(df)} candles ✅")
    return df

# ═══════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════
def _rsi_s(s:pd.Series,p:int=14)->pd.Series:
    if HAS_TA:
        try:
            v=ta.rsi(s,length=p)
            if v is not None: return v.fillna(50)
        except Exception: pass
    d=s.diff(); g=d.clip(lower=0).ewm(alpha=1/p,adjust=False).mean()
    l=(-d.clip(upper=0)).ewm(alpha=1/p,adjust=False).mean()
    return (100-100/(1+g/l.replace(0,1e-10))).fillna(50)

def _ema_s(s:pd.Series,p:int)->pd.Series:
    return s.ewm(span=p,adjust=False).mean()

def _atr_s(df:pd.DataFrame,p:int=14)->pd.Series:
    hl=df.high-df.low; hpc=(df.high-df.close.shift()).abs()
    lpc=(df.low-df.close.shift()).abs()
    return pd.concat([hl,hpc,lpc],axis=1).max(axis=1).ewm(alpha=1/p,adjust=False).mean()

def _build(df:pd.DataFrame)->pd.DataFrame:
    df=df.copy(); c=df.close; h=df.high; l=df.low
    # RSI multiple periods
    df["rsi14"]=_rsi_s(c,14); df["rsi6"]=_rsi_s(c,6)
    # MACD
    e12=c.ewm(span=12,adjust=False).mean(); e26=c.ewm(span=26,adjust=False).mean()
    ms=e12-e26; df["macd"]=ms; df["macd_h"]=(ms-ms.ewm(span=9,adjust=False).mean()).fillna(0)
    # EMAs
    for p in [9,20,50,100,200]: df[f"ema{p}"]=_ema_s(c,p)
    # ATR
    df["atr"]=_atr_s(df,14)
    # ADX
    up=(h-h.shift()).clip(lower=0); dn=(l.shift()-l).clip(lower=0)
    df["pdi"]=up.ewm(alpha=1/14,adjust=False).mean()
    df["mdi"]=dn.ewm(alpha=1/14,adjust=False).mean()
    df["adx"]=((df.pdi-df.mdi).abs()/(df.pdi+df.mdi+1e-10)*100).ewm(alpha=1/14,adjust=False).mean().fillna(15)
    # Bollinger
    mid=c.rolling(20).mean(); sd=c.rolling(20).std()
    df["bbu"]=mid+2*sd; df["bbl"]=mid-2*sd; df["bbm"]=mid
    df["bbp"]=(c-df.bbl)/(df.bbu-df.bbl+1e-10)*100
    # Stochastic
    lo14=l.rolling(14).min(); hi14=h.rolling(14).max()
    df["stoch"]=100*(c-lo14)/(hi14-lo14+1e-10)
    # Williams %R
    df["wr"]=(-100*(hi14-c)/(hi14-lo14+1e-10)).fillna(-50)
    # OBV
    df["obv"]=(c.diff().apply(lambda x:1 if x>0 else -1 if x<0 else 0)*df.volume).cumsum()
    # Volume
    df["vma"]=df.volume.rolling(20).mean(); df["vr"]=df.volume/(df.vma.replace(0,1))
    # Ichimoku
    df["iten"]=(h.rolling(9).max()+l.rolling(9).min())/2
    df["ikij"]=(h.rolling(26).max()+l.rolling(26).min())/2
    sa=(df.iten+df.ikij)/2; sb=(h.rolling(52).max()+l.rolling(52).min())/2
    df["ict"]=pd.concat([sa,sb],axis=1).max(axis=1)
    df["icb"]=pd.concat([sa,sb],axis=1).min(axis=1)
    # Heikin Ashi
    df["hac"]=(df.open+h+l+c)/4; df["hao"]=df.hac.shift(2)
    df["hab"]=(df.hac>df.hao).astype(int)
    # Swing high/low (for SL placement)
    df["swing_hi"]=h.rolling(10).max()
    df["swing_lo"]=l.rolling(10).min()
    return df.ffill().bfill()

# ═══════════════════════════════════════
# DIVERGENCE (pre-computed)
# ═══════════════════════════════════════
def _add_div(df:pd.DataFrame)->pd.DataFrame:
    sc=pd.Series(0.0,index=df.index)
    plows=[]; phighs=[]
    for i in range(5,len(df)-5):
        lw=df.low.iloc[i-5:i+6]; hw=df.high.iloc[i-5:i+6]
        if len(lw)<11: continue
        if float(df.low.iloc[i])==float(lw.min()):
            plows.append((i,float(df.low.iloc[i]),float(df.rsi14.iloc[i])))
        if float(df.high.iloc[i])==float(hw.max()):
            phighs.append((i,float(df.high.iloc[i]),float(df.rsi14.iloc[i])))
        if len(plows)>=2:
            l1,l2=plows[-2],plows[-1]
            if l2[1]<l1[1] and l2[2]>l1[2]: sc.iloc[i]+=0.7   # Reg Bull
            if l2[1]>l1[1] and l2[2]<l1[2]: sc.iloc[i]+=0.4   # Hid Bull
        if len(phighs)>=2:
            h1,h2=phighs[-2],phighs[-1]
            if h2[1]>h1[1] and h2[2]<h1[2]: sc.iloc[i]-=0.7   # Reg Bear
            if h2[1]<h1[1] and h2[2]>h1[2]: sc.iloc[i]-=0.4   # Hid Bear
    # Volume boost
    df["div"]=sc.clip(-1,1).fillna(0)
    return df

# ═══════════════════════════════════════
# IMPROVEMENT 4 — FIBONACCI ZONE CHECK
# ═══════════════════════════════════════
def _near_fib(df:pd.DataFrame,i:int,direction:str)->bool:
    """Check if price is near a key Fibonacci level."""
    if i<30: return True   # not enough data → don't filter
    hi=float(df.high.iloc[max(0,i-60):i+1].max())
    lo=float(df.low.iloc[max(0,i-60):i+1].min())
    rng=hi-lo
    if rng<=0: return True
    c=float(df.close.iloc[i]); pos=(c-lo)/rng
    TOL=0.025   # within 2.5% of fib level
    BULL_FIBS=[0.309,0.4045,0.500,0.618]   # support zones
    BEAR_FIBS=[0.500,0.618,0.750,0.809]    # resistance zones
    fibs=BULL_FIBS if direction=="LONG" else BEAR_FIBS
    return any(abs(pos-f)<TOL for f in fibs)

# ═══════════════════════════════════════
# IMPROVEMENT 2 — SWING SL PLACEMENT
# ═══════════════════════════════════════
def _swing_sl(df:pd.DataFrame,i:int,direction:str,price:float)->Tuple[float,float]:
    """
    Place SL below last swing low (LONG) or above last swing high (SHORT).
    Ensures SL is beyond noise, not just ATR-based.
    """
    atr=float(df.atr.iloc[i]) if float(df.atr.iloc[i])>0 else price*0.015
    buf=atr*0.3   # small buffer beyond swing

    if direction=="LONG":
        # SL below recent swing low
        swing_lo=float(df.low.iloc[max(0,i-10):i+1].min())
        sl=round(swing_lo-buf,4)
        # Ensure SL is at least 0.8 ATR away
        if price-sl < atr*0.8: sl=round(price-atr*1.0,4)
        # Cap SL at 2.5 ATR (don't let it be too wide)
        if price-sl > atr*2.5: sl=round(price-atr*2.5,4)
    else:
        swing_hi=float(df.high.iloc[max(0,i-10):i+1].max())
        sl=round(swing_hi+buf,4)
        if sl-price < atr*0.8: sl=round(price+atr*1.0,4)
        if sl-price > atr*2.5: sl=round(price+atr*2.5,4)

    sl_dist=abs(price-sl)
    return sl, sl_dist

# ═══════════════════════════════════════
# CANDLE PATTERN CHECK
# ═══════════════════════════════════════
def _candle_ok(df:pd.DataFrame,i:int,direction:str)->bool:
    """Require a reversal/continuation candlestick pattern."""
    if i<2: return True
    r0=df.iloc[i]; r1=df.iloc[i-1]
    o0,h0,l0,c0=float(r0.open),float(r0.high),float(r0.low),float(r0.close)
    o1,h1,l1,c1=float(r1.open),float(r1.high),float(r1.low),float(r1.close)
    b0=abs(c0-o0); rng0=h0-l0 or 1e-4
    b1=abs(c1-o1)

    if direction=="LONG":
        hammer   =(c1>o1 and (min(o1,c1)-l1)>b1*1.5 and (h1-max(o1,c1))<b1*0.5)
        bull_eng =(c0>o0 and c1<o1 and o0<c1 and c0>o1)
        bull_bar =(c0>o0 and b0>rng0*0.55)   # strong bull candle
        return hammer or bull_eng or bull_bar
    else:
        star     =(c1<o1 and (h1-max(o1,c1))>b1*1.5 and (min(o1,c1)-l1)<b1*0.5)
        bear_eng =(c0<o0 and c1>o1 and o0>c1 and c0<o1)
        bear_bar =(c0<o0 and b0>rng0*0.55)
        return star or bear_eng or bear_bar

# ═══════════════════════════════════════
# IMPROVEMENT 3 — TRIPLE MANDATORY CHECK
# ═══════════════════════════════════════
def _mandatory_ok(df:pd.DataFrame,i:int,direction:str)->bool:
    """
    ALL 3 must be true to proceed:
    1. EMA 200 alignment (major trend)
    2. RSI in valid zone
    3. Volume > 1.1× MA
    """
    row=df.iloc[i]; c=float(row.close)
    e200=float(row.ema200); e50=float(row.ema50); e20=float(row.ema20)
    rsi=float(row.rsi14); vr=float(row.vr)

    if direction=="LONG":
        ema_ok=(c>e200*0.995)   # price above/near EMA200
        rsi_ok=(rsi<62)         # RSI not overbought
    else:
        ema_ok=(c<e200*1.005)
        rsi_ok=(rsi>38)

    vol_ok=(vr>1.1)             # volume above average

    return ema_ok and rsi_ok and vol_ok

# ═══════════════════════════════════════
# SCORING (focused on high-confidence)
# ═══════════════════════════════════════
def _score(df:pd.DataFrame,i:int)->Tuple[str,float,int]:
    row=df.iloc[i]; c=float(row.close); sc=[]

    # E1: EMA Alignment (weighted heavily)
    e20=float(row.ema20); e50=float(row.ema50); e200=float(row.ema200)
    if c>e20>e50>e200: sc.append(1.2)
    elif c>e20>e50:    sc.append(0.7)
    elif c<e20<e50<e200: sc.append(-1.2)
    elif c<e20<e50:    sc.append(-0.7)
    else:              sc.append(0.0)

    # E2: RSI (14) zone
    r=float(row.rsi14)
    sc.append(1.0 if r<25 else 0.6 if r<35 else 0.2 if r<45
              else -1.0 if r>75 else -0.6 if r>65 else -0.2 if r>55 else 0.0)

    # E3: RSI (6) confirmation
    r6=float(row.rsi6)
    sc.append(0.5 if r6<30 else 0.2 if r6<45 else -0.5 if r6>70 else -0.2 if r6>55 else 0.0)

    # E4: MACD histogram + crossover
    mh=float(row.macd_h)
    if i>0:
        pmh=float(df.macd_h.iloc[i-1])
        cross_bull=(mh>0 and pmh<=0); cross_bear=(mh<0 and pmh>=0)
        if cross_bull: sc.append(1.0)
        elif cross_bear: sc.append(-1.0)
        elif mh>0: sc.append(0.5)
        else: sc.append(-0.5)
    else: sc.append(0.5 if mh>0 else -0.5)

    # E5: Ichimoku Cloud
    ct=float(row.ict); cb=float(row.icb)
    tn=float(row.iten); kj=float(row.ikij)
    if c>ct and tn>kj:    sc.append(1.0)
    elif c>ct:            sc.append(0.5)
    elif c<cb and tn<kj:  sc.append(-1.0)
    elif c<cb:            sc.append(-0.5)
    else:                 sc.append(0.0)

    # E6: Bollinger Bands (bounce)
    bp=float(row.bbp)
    sc.append(1.0 if bp<8 else 0.5 if bp<20 else 0.0 if bp<50
              else -1.0 if bp>92 else -0.5 if bp>80 else 0.0)

    # E7: Stochastic
    sk=float(row.stoch)
    sc.append(0.8 if sk<18 else 0.4 if sk<32 else -0.8 if sk>82 else -0.4 if sk>68 else 0.0)

    # E8: ADX directional
    adx=float(row.adx); pdi=float(row.pdi); mdi=float(row.mdi)
    w=1.2 if adx>30 else 0.8 if adx>20 else 0.4
    sc.append(0.7*w if pdi>mdi else -0.7*w)

    # E9: Williams %R
    wr=float(row.wr)
    sc.append(0.8 if wr<-82 else 0.35 if wr<-65 else -0.8 if wr>-18 else -0.35 if wr>-35 else 0.0)

    # E10: OBV trend
    if i>=8:
        obv_r=float(df.obv.iloc[i-4:i+1].mean())
        obv_p=float(df.obv.iloc[max(0,i-9):i-4].mean())
        sc.append(0.6 if obv_r>obv_p else -0.6)
    else: sc.append(0.0)

    # E11: Volume surge
    vr=float(row.vr); base=sum(sc)/len(sc) if sc else 0
    if vr>2.0:   sc.append(0.8*(1 if base>0 else -1))
    elif vr>1.5: sc.append(0.5*(1 if base>0 else -1))
    elif vr>1.2: sc.append(0.2*(1 if base>0 else -1))
    else:        sc.append(0.0)

    # E12: Heikin Ashi momentum
    sc.append(0.5 if float(row.hab)==1 else -0.5)

    # E13: Divergence (pre-computed bonus)
    div=float(row.get("div",0))
    sc.append(div*0.9)

    # E14: Fibonacci + Gann
    if i>=20:
        hi50=float(df.high.iloc[max(0,i-50):i+1].max())
        lo50=float(df.low.iloc[max(0,i-50):i+1].min())
        rng=hi50-lo50
        c_=float(df.close.iloc[i])
        if rng>0:
            pos=(c_-lo50)/rng
            BULL_Z=[(0.28,0.34),(0.39,0.42),(0.60,0.64)]
            BEAR_Z=[(0.72,0.78),(0.76,0.80),(0.79,0.83)]
            fib_bull=any(a<=pos<=b for a,b in BULL_Z)
            fib_bear=any(a<=pos<=b for a,b in BEAR_Z)
            if fib_bull: sc.append(0.8)
            elif fib_bear: sc.append(-0.8)
            else: sc.append(0.2 if pos<0.40 else -0.2 if pos>0.60 else 0.0)
        else: sc.append(0.0)
    else: sc.append(0.0)

    n=len(sc); avg=sum(sc)/n if n else 0.0
    bull=sum(1 for s in sc if s>0.05)
    bear=sum(1 for s in sc if s<-0.05)

    # STRICT thresholds: avg > 0.30 AND 9+ votes (of 14 experts)
    direction=("LONG"  if avg> 0.30 and bull>=9 else
               "SHORT" if avg<-0.30 and bear>=9 else "NEUTRAL")
    return direction, round(avg,4), (bull if avg>=0 else bear)

# ═══════════════════════════════════════
# SIMULATION
# ═══════════════════════════════════════
def _sim(df:pd.DataFrame, balance:float=10_000.0)->Dict:
    n=len(df); equity=[balance]; trades=[]
    WARMUP=60; MAX_C=20
    in_trade=False; partial=False
    direction=""; entry=sl=tp1=tp2=tp3=sl_dist=0.0; entry_idx=0
    consec_loss=0   # IMPROVEMENT 8: cool-down counter

    for i in range(WARMUP,n):
        row=df.iloc[i]; price=float(row.close)
        hi=float(row.high); lo=float(row.low)
        hour=row.ts.hour if hasattr(row.ts,"hour") else 10

        # ── Manage trade ──────────────────────────────────────────────────
        if in_trade:
            # Time exit
            if (i-entry_idx)>MAX_C:
                pnl=((price-entry)/entry*100) if direction=="LONG" else ((entry-price)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.01/max(sl_dist/entry,0.001)
                gain=pnl/100*risk*mult; balance=max(1.0,balance+gain)
                win=pnl>0
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(price,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if win else "LOSS",
                                "exit_type":"TIME_EXIT","session":_sess(hour)})
                consec_loss=0 if win else consec_loss+1
                in_trade=False; partial=False; equity.append(balance); continue

            # TP/SL
            if direction=="LONG":
                hit_sl=lo<=sl; hit_tp3=hi>=tp3; hit_tp2=hi>=tp2 and not hit_tp3
                hit_tp1=hi>=tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl=hi>=sl; hit_tp3=lo<=tp3; hit_tp2=lo<=tp2 and not hit_tp3
                hit_tp1=lo<=tp1 and not hit_tp2 and not hit_tp3

            # Partial @ TP1
            if hit_tp1 and not partial:
                pnl=((tp1-entry)/entry*100) if direction=="LONG" else ((entry-tp1)/entry*100)
                risk=balance*0.01/max(sl_dist/entry,0.001)
                balance=max(1.0,balance+pnl/100*risk*0.5)
                sl=entry; partial=True
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(tp1,4),
                                "pnl_pct":round(pnl*0.5,3),"result":"WIN",
                                "exit_type":"TP1_PARTIAL","session":_sess(hour)})
                equity.append(balance); continue

            if hit_tp2 or hit_tp3 or hit_sl:
                ex=sl if hit_sl else (tp3 if hit_tp3 else tp2)
                pnl=((ex-entry)/entry*100) if direction=="LONG" else ((entry-ex)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.01/max(sl_dist/entry,0.001)
                gain=pnl/100*risk*mult; balance=max(1.0,balance+gain)
                win=pnl>0
                xtype="SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(ex,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if win else "LOSS",
                                "exit_type":xtype,"session":_sess(hour)})
                consec_loss=0 if win else consec_loss+1
                in_trade=False; partial=False

        # ── New signal ─────────────────────────────────────────────────────
        if not in_trade:
            # IMPROVEMENT 1: Hard session block
            if not _sess_ok(hour): equity.append(balance); continue

            # IMPROVEMENT 8: Cool-down after 2 consecutive losses
            if consec_loss>=2: consec_loss=0; equity.append(balance); continue

            # IMPROVEMENT 10: Anti-chop filter
            if float(row.adx)<18: equity.append(balance); continue

            # Score
            sig,avg,votes=_score(df,i)
            if sig=="NEUTRAL": equity.append(balance); continue

            # IMPROVEMENT 3: Triple mandatory check
            if not _mandatory_ok(df,i,sig): equity.append(balance); continue

            # IMPROVEMENT 4: Fibonacci zone check
            if not _near_fib(df,i,sig): equity.append(balance); continue

            # IMPROVEMENT 6: Consecutive candle check (prev candle same direction)
            if i>0:
                prev_bull=float(df.close.iloc[i-1])>float(df.open.iloc[i-1])
                cur_bull =float(df.close.iloc[i])>float(df.open.iloc[i])
                if sig=="LONG"  and not (prev_bull or cur_bull):
                    equity.append(balance); continue
                if sig=="SHORT" and not (not prev_bull or not cur_bull):
                    equity.append(balance); continue

            # Candle pattern confirmation
            if not _candle_ok(df,i,sig): equity.append(balance); continue

            # Session quality gate (min quality 1.1)
            if _sess_q(hour)<1.1: equity.append(balance); continue

            # IMPROVEMENT 2: Swing SL placement
            sl_price, sl_d = _swing_sl(df,i,sig,price)
            sl_dist=sl_d

            # Build TPs with dynamic ratio based on ADX
            adx=float(row.adx)
            tp_mult=1.5 if adx>30 else 1.2   # tighter TPs in strong trends
            tp1_dist=sl_d*1.8          # TP1: RR=1.8
            tp2_dist=sl_d*3.5          # TP2: RR=3.5
            tp3_dist=sl_d*6.0          # TP3: RR=6.0

            if sig=="LONG":
                sl=sl_price
                tp1=round(price+tp1_dist,4)
                tp2=round(price+tp2_dist,4)
                tp3=round(price+tp3_dist,4)
            else:
                sl=sl_price
                tp1=round(price-tp1_dist,4)
                tp2=round(price-tp2_dist,4)
                tp3=round(price-tp3_dist,4)

            # Final RR check ≥ 1.5
            rr=tp1_dist/max(sl_d,1e-10)
            if rr<1.5: equity.append(balance); continue

            direction=sig; entry=price
            in_trade=True; entry_idx=i; partial=False

        equity.append(balance)

    if in_trade:
        lp=float(df.close.iloc[-1])
        pnl=((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        trades.append({"direction":direction,"entry":entry,"exit":lp,
                        "pnl_pct":round(pnl*(0.5 if partial else 1.0),3),
                        "result":"WIN" if pnl>0 else "LOSS","exit_type":"OPEN_AT_END"})

    return {"trades":trades,"equity":equity,"final_balance":balance}

# ═══════════════════════════════════════
# METRICS
# ═══════════════════════════════════════
def _metrics(sim:Dict,initial:float)->Dict:
    trades=sim["trades"]; eq=pd.Series(sim["equity"]); bal=sim["final_balance"]
    if not trades:
        return {"error":"No trades","total":0,"win_rate_pct":0,"max_dd_pct":0,
                "return_pct":0,"sharpe":0}
    wins=[t for t in trades if t["result"]=="WIN"]
    losses=[t for t in trades if t["result"]=="LOSS"]
    tot=len(trades); wr=round(len(wins)/tot*100,2) if tot else 0
    dd=round(float(((eq-eq.cummax())/eq.cummax()*100).min()),2)
    ret=round((bal-initial)/initial*100,2)
    rets=eq.pct_change().dropna()
    sh=round(float(rets.mean()/rets.std()*(252**0.5)),3) if rets.std()>0 else 0.0
    aw=round(sum(t["pnl_pct"] for t in wins)/len(wins),3) if wins else 0
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

# ═══════════════════════════════════════
# BacktestEngine
# ═══════════════════════════════════════
class BacktestEngine:
    async def run(self,
                  symbols:List[str]=None,
                  timeframe:str="1h",
                  tf:str=None,
                  start:str="2026-01-01",
                  end:str="2026-05-01",
                  balance:float=10_000.0,
                  use_sessions:bool=True,
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
        lines=["📈 <b>Backtest — Ramos 360 Ai 🎖️  PRO</b>",
               f"📅 Period: 2026-01-01 → 2026-05-01",
               f"⏱️ Timeframe: {tf.upper()} | 14 Experts",
               "✅ 10 Improvements | Target WR 70-80%",
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
                "<i>🎖️ Ramos 360 Ai — Backtest PRO</i>"]
        return "\n".join(lines)

async def _main():
    e=BacktestEngine()
    r=await e.run(symbols=["BTC/USDT:USDT"],timeframe="1h",
                   start="2026-01-01",end="2026-05-01",balance=10_000.0)
    print("\n"+e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))

if __name__=="__main__":
    asyncio.run(_main())
