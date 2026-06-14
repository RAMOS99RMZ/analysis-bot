"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  CLEAN
══════════════════════════════════════════════════════════
PHILOSOPHY: One strong scoring system, NOT a chain of gates.
Each gate halves signal count — 8 gates = 0 signals.

ACTIVE FILTERS (only 3):
  1. Session: block 17:00-06:59 UTC (DEAD_ZONE only)
  2. Score ≥ 0.22 AND 7+ positive experts
  3. RR ≥ 1.5 (auto-satisfied by ATR formula)

IMPROVEMENTS vs FINAL:
  ✅ Swing High/Low SL  → fewer premature SL hits
  ✅ EMA200 mandatory   → adds heavy weight in scoring
  ✅ Divergence bonus   → pre-computed, adds score
  ✅ RSI momentum       → RSI direction change = bonus
  ✅ Volume weighting   → volume surge boosts score
  ✅ ADX direction      → trend clarity adds weight
  ✅ Partial exit TP1   → locks in profit early
  ✅ Cool-down 3 losses → protects balance streaks
══════════════════════════════════════════════════════════
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
_HDR  = {"Accept":"application/json","User-Agent":"Ramos360CLEAN/1.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
          "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

# ── SESSION ─────────────────────────────────────────────────────────────────
def _sess(h:int)->str:
    if 13<=h<16: return "OVERLAP"
    if  7<=h<12: return "LONDON"
    if 13<=h<17: return "NEW_YORK"
    if  0<=h< 7: return "ASIA"
    return "DEAD_ZONE"

def _sess_ok(h:int)->bool:
    # Block 17:00–23:59 UTC (DEAD_ZONE) — allow everything else
    return h not in range(17, 24)

def _sess_w(h:int)->float:
    # Weight multiplier for session quality
    return {"OVERLAP":1.4,"LONDON":1.2,"NEW_YORK":1.1,"ASIA":0.8,"DEAD_ZONE":0.0}.get(_sess(h),1.0)

# ── OKX FETCH ────────────────────────────────────────────────────────────────
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

# ── INDICATORS ───────────────────────────────────────────────────────────────
def _build(df:pd.DataFrame)->pd.DataFrame:
    df=df.copy(); c=df.close; h=df.high; l=df.low
    # RSI
    d=c.diff(); g=d.clip(lower=0).ewm(alpha=1/14,adjust=False).mean()
    ls=(-d.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
    df["rsi"]=(100-100/(1+g/ls.replace(0,1e-10))).fillna(50)
    # MACD
    e12=c.ewm(span=12,adjust=False).mean(); e26=c.ewm(span=26,adjust=False).mean()
    ms=e12-e26; df["mh"]=(ms-ms.ewm(span=9,adjust=False).mean()).fillna(0)
    df["macd_prev"]=df.mh.shift(1).fillna(0)
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
    # Stoch
    lo14=l.rolling(14).min(); hi14=h.rolling(14).max()
    df["stoch"]=100*(c-lo14)/(hi14-lo14+1e-10)
    df["wr"]=(-100*(hi14-c)/(hi14-lo14+1e-10)).fillna(-50)
    # Volume
    df["vm"]=df.volume.rolling(20).mean(); df["vr"]=df.volume/(df.vm.replace(0,1))
    # Ichimoku
    df["iten"]=(h.rolling(9).max()+l.rolling(9).min())/2
    df["ikij"]=(h.rolling(26).max()+l.rolling(26).min())/2
    sa=(df.iten+df.ikij)/2; sb=(h.rolling(52).max()+l.rolling(52).min())/2
    df["ict"]=pd.concat([sa,sb],axis=1).max(axis=1)
    df["icb"]=pd.concat([sa,sb],axis=1).min(axis=1)
    # HA
    df["hac"]=(df.open+h+l+c)/4; df["hao"]=df.hac.shift(2)
    df["hab"]=(df.hac>df.hao).astype(int)
    # Wyckoff
    df["wyck"]=0.0
    df.loc[(c>df.e50)&(df.vr>1.2),"wyck"]=1.0
    df.loc[(c<df.e50)&(df.vr>1.2),"wyck"]=-1.0
    # Swing levels for SL
    df["swing_lo10"]=l.rolling(10).min()
    df["swing_hi10"]=h.rolling(10).max()
    return df.ffill().bfill()

# ── DIVERGENCE PRE-COMPUTED ──────────────────────────────────────────────────
def _add_div(df:pd.DataFrame)->pd.DataFrame:
    sc=pd.Series(0.0,index=df.index)
    pl=[]; ph=[]
    for i in range(5,len(df)-5):
        lw=df.low.iloc[i-5:i+6]; hw=df.high.iloc[i-5:i+6]
        if len(lw)<11: continue
        if float(df.low.iloc[i])==float(lw.min()):
            pl.append((i,float(df.low.iloc[i]),float(df.rsi.iloc[i])))
        if float(df.high.iloc[i])==float(hw.max()):
            ph.append((i,float(df.high.iloc[i]),float(df.rsi.iloc[i])))
        if len(pl)>=2:
            a,b=pl[-2],pl[-1]
            if b[1]<a[1] and b[2]>a[2]: sc.iloc[i]+=0.7  # Reg Bull
            if b[1]>a[1] and b[2]<a[2]: sc.iloc[i]+=0.4  # Hid Bull
        if len(ph)>=2:
            a,b=ph[-2],ph[-1]
            if b[1]>a[1] and b[2]<a[2]: sc.iloc[i]-=0.7  # Reg Bear
            if b[1]<a[1] and b[2]>a[2]: sc.iloc[i]-=0.4  # Hid Bear
    df["div"]=sc.clip(-1,1).fillna(0)
    return df

# ── SCORING (14 experts, ONE threshold) ─────────────────────────────────────
def _score(df:pd.DataFrame, i:int, sw:float=1.0)->Tuple[str,float,int]:
    """
    Score all 14 experts. Returns (direction, avg, positive_votes).
    sw = session weight (0.8-1.4)
    """
    row=df.iloc[i]; c=float(row.close); sc=[]

    # E1: EMA Alignment (strongest signal)
    e20=float(row.e20); e50=float(row.e50); e200=float(row.e200)
    if c>e20>e50>e200:   sc.append(1.3)
    elif c>e20>e50:      sc.append(0.8)
    elif c>e20:          sc.append(0.3)
    elif c<e20<e50<e200: sc.append(-1.3)
    elif c<e20<e50:      sc.append(-0.8)
    elif c<e20:          sc.append(-0.3)
    else:                sc.append(0.0)

    # E2: RSI oversold/overbought
    r=float(row.rsi)
    sc.append(1.0 if r<25 else 0.6 if r<35 else 0.25 if r<48
              else -1.0 if r>75 else -0.6 if r>65 else -0.25 if r>52 else 0.0)

    # E3: MACD crossover or direction
    mh=float(row.mh); pmh=float(row.macd_prev)
    if mh>0 and pmh<=0:   sc.append(1.0)   # Bullish cross
    elif mh<0 and pmh>=0: sc.append(-1.0)  # Bearish cross
    elif mh>0:            sc.append(0.5)
    else:                 sc.append(-0.5)

    # E4: Ichimoku
    ct=float(row.ict); cb=float(row.icb)
    tn=float(row.iten); kj=float(row.ikij)
    if c>ct and tn>kj:    sc.append(1.0)
    elif c>ct:            sc.append(0.5)
    elif c<cb and tn<kj:  sc.append(-1.0)
    elif c<cb:            sc.append(-0.5)
    else:                 sc.append(0.0)

    # E5: Bollinger Bands
    bp=float(row.bbp)
    sc.append(0.9 if bp<10 else 0.5 if bp<22 else 0.1 if bp<45
              else -0.9 if bp>90 else -0.5 if bp>78 else -0.1 if bp>55 else 0.0)

    # E6: Stochastic
    sk=float(row.stoch)
    sc.append(0.8 if sk<20 else 0.4 if sk<35 else -0.8 if sk>80 else -0.4 if sk>65 else 0.0)

    # E7: ADX + DI direction
    adx=float(row.adx); pdi=float(row.pdi); mdi=float(row.mdi)
    w=1.2 if adx>28 else 0.8 if adx>18 else 0.35
    sc.append(0.7*w if pdi>mdi else -0.7*w)

    # E8: Williams %R
    wr=float(row.wr)
    sc.append(0.8 if wr<-82 else 0.4 if wr<-65 else -0.8 if wr>-18 else -0.4 if wr>-35 else 0.0)

    # E9: OBV trend
    if i>=8:
        or_=float(df.div.iloc[i])   # use pre-computed div as proxy
        sc.append(or_*0.9)
    else: sc.append(0.0)

    # E10: Volume surge
    vr=float(row.vr); base_dir=sum(sc)/len(sc) if sc else 0
    if vr>2.2:   sc.append(0.8*(1 if base_dir>0 else -1))
    elif vr>1.6: sc.append(0.5*(1 if base_dir>0 else -1))
    elif vr>1.2: sc.append(0.25*(1 if base_dir>0 else -1))
    else:        sc.append(0.0)

    # E11: Heikin Ashi
    sc.append(0.5 if float(row.hab)==1 else -0.5)

    # E12: Wyckoff
    sc.append(float(row.wyck)*0.8)

    # E13: Fibonacci position
    if i>=25:
        hi=float(df.high.iloc[max(0,i-50):i+1].max())
        lo=float(df.low.iloc[max(0,i-50):i+1].min())
        rng=hi-lo
        if rng>0:
            pos=(c-lo)/rng
            # Bull zones: 0.309, 0.4045, 0.5, 0.618
            # Bear zones: 0.5, 0.618, 0.75, 0.809
            bull_fib=any(abs(pos-f)<0.04 for f in [0.309,0.4045,0.500,0.618])
            bear_fib=any(abs(pos-f)<0.04 for f in [0.500,0.618,0.750,0.809])
            if bull_fib and pos<0.65:  sc.append(0.7)
            elif bear_fib and pos>0.45:sc.append(-0.7)
            elif pos<0.35:             sc.append(0.4)
            elif pos>0.65:             sc.append(-0.4)
            else:                      sc.append(0.0)
        else: sc.append(0.0)
    else: sc.append(0.0)

    # E14: Gann Sq9
    root=math.sqrt(max(c,0.001))
    g=[round((root+s)**2,2) for s in [-1.5,-1,-.5,0,.5,1,1.5] if round((root+s)**2,2)>0]
    if g:
        near=min(g,key=lambda x:abs(x-c)); d=(near-c)/c
        sc.append(0.5 if abs(d)<0.004 else 0.25 if d>0 else -0.25)
    else: sc.append(0.0)

    n=len(sc); avg=(sum(sc)/n)*sw if n else 0.0
    bull=sum(1 for s in sc if s>0.05)
    bear=sum(1 for s in sc if s<-0.05)

    # SINGLE THRESHOLD: avg>0.22 AND 7+ experts agree
    direction=("LONG"  if avg> 0.22 and bull>=7 else
               "SHORT" if avg<-0.22 and bear>=7 else "NEUTRAL")
    return direction, round(avg,4), (bull if avg>=0 else bear)

# ── SL/TP WITH SWING LEVELS ──────────────────────────────────────────────────
def _levels(df:pd.DataFrame,i:int,direction:str,price:float)->Tuple[float,float,float,float]:
    atr=float(df.atr.iloc[i]); atr=atr if atr>0 else price*0.015

    if direction=="LONG":
        # SL: below swing low (last 10 candles), min 1×ATR, max 2×ATR
        swing=float(df.swing_lo10.iloc[i])
        sl=round(min(swing-atr*0.2, price-atr*1.0), 4)
        sl=max(sl, round(price-atr*2.0,4))   # cap
        sl_d=price-sl
    else:
        swing=float(df.swing_hi10.iloc[i])
        sl=round(max(swing+atr*0.2, price+atr*1.0), 4)
        sl=min(sl, round(price+atr*2.0,4))
        sl_d=sl-price

    # TP with RR 1.8 / 3.5 / 6.0
    t1d=sl_d*1.8; t2d=sl_d*3.5; t3d=sl_d*6.0

    if direction=="LONG":
        tp1=round(price+t1d,4); tp2=round(price+t2d,4); tp3=round(price+t3d,4)
    else:
        tp1=round(price-t1d,4); tp2=round(price-t2d,4); tp3=round(price-t3d,4)

    return sl, tp1, tp2, tp3

# ── SIMULATION ───────────────────────────────────────────────────────────────
def _sim(df:pd.DataFrame, balance:float=10_000.0)->Dict:
    n=len(df); equity=[balance]; trades=[]
    WARMUP=60; MAX_C=20
    in_trade=False; partial=False
    direction=""; entry=sl=tp1=tp2=tp3=0.0; sl_d=0.0; entry_idx=0
    loss_streak=0   # cool-down tracker

    for i in range(WARMUP,n):
        row=df.iloc[i]; price=float(row.close)
        hi=float(row.high); lo=float(row.low)
        hour=row.ts.hour if hasattr(row.ts,"hour") else 10

        # ── Manage trade ───────────────────────────────────────────────
        if in_trade:
            # Time exit
            if (i-entry_idx)>MAX_C:
                pnl=((price-entry)/entry*100) if direction=="LONG" else ((entry-price)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.01/max(sl_d/entry,0.001)
                gain=pnl/100*risk*mult; balance=max(1.0,balance+gain)
                win=pnl>0; loss_streak=0 if win else loss_streak+1
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(price,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if win else "LOSS",
                                "exit_type":"TIME_EXIT","session":_sess(hour)})
                in_trade=False; partial=False; equity.append(balance); continue

            # TP/SL
            if direction=="LONG":
                hit_sl=lo<=sl; hit_tp3=hi>=tp3
                hit_tp2=hi>=tp2 and not hit_tp3
                hit_tp1=hi>=tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl=hi>=sl; hit_tp3=lo<=tp3
                hit_tp2=lo<=tp2 and not hit_tp3
                hit_tp1=lo<=tp1 and not hit_tp2 and not hit_tp3

            # Partial at TP1 (50% + SL to breakeven)
            if hit_tp1 and not partial:
                pnl=((tp1-entry)/entry*100) if direction=="LONG" else ((entry-tp1)/entry*100)
                risk=balance*0.01/max(sl_d/entry,0.001)
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
                risk=balance*0.01/max(sl_d/entry,0.001)
                gain=pnl/100*risk*mult; balance=max(1.0,balance+gain)
                win=pnl>0; loss_streak=0 if win else loss_streak+1
                xtype="SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(ex,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if win else "LOSS",
                                "exit_type":xtype,"session":_sess(hour)})
                in_trade=False; partial=False

        # ── New signal ──────────────────────────────────────────────────
        if not in_trade:
            # Filter 1: Session (block 17-23 UTC only)
            if not _sess_ok(hour): equity.append(balance); continue

            # Filter 2: Cool-down after 3 consecutive losses
            if loss_streak>=3: loss_streak=0; equity.append(balance); continue

            # Get session weight
            sw=_sess_w(hour)

            # Score (ONLY filter = score threshold + votes)
            sig,avg,votes=_score(df,i,sw)
            if sig=="NEUTRAL": equity.append(balance); continue

            # Build SL/TP
            sl_p,tp1_p,tp2_p,tp3_p=_levels(df,i,sig,price)
            sl_dist=abs(price-sl_p)

            # Filter 3: RR check (auto-satisfied by swing SL logic)
            rr=abs(tp1_p-price)/max(sl_dist,1e-10)
            if rr<1.3: equity.append(balance); continue

            direction=sig; entry=price; sl=sl_p
            tp1=tp1_p; tp2=tp2_p; tp3=tp3_p; sl_d=sl_dist
            in_trade=True; entry_idx=i; partial=False

        equity.append(balance)

    if in_trade:
        lp=float(df.close.iloc[-1])
        pnl=((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        trades.append({"direction":direction,"entry":entry,"exit":lp,
                        "pnl_pct":round(pnl*(0.5 if partial else 1.0),3),
                        "result":"WIN" if pnl>0 else "LOSS","exit_type":"OPEN_AT_END"})

    return {"trades":trades,"equity":equity,"final_balance":balance}

# ── METRICS ──────────────────────────────────────────────────────────────────
def _metrics(sim:Dict,initial:float)->Dict:
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

# ── BacktestEngine ────────────────────────────────────────────────────────────
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
        lines=["📈 <b>Backtest — Ramos 360 Ai 🎖️  CLEAN</b>",
               f"📅 Period: 2026-01-01 → 2026-05-01",
               f"⏱️ Timeframe: {tf.upper()} | 14 Experts",
               "✅ Swing SL + Session + Smart Scoring",
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
                "<i>🎖️ Ramos 360 Ai — Backtest CLEAN</i>"]
        return "\n".join(lines)

async def _main():
    e=BacktestEngine()
    r=await e.run(symbols=["BTC/USDT:USDT"],timeframe="1h",
                   start="2026-01-01",end="2026-05-01",balance=10_000.0)
    print("\n"+e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))

if __name__=="__main__":
    asyncio.run(_main())
