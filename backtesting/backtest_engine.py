"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  v4
FIXED: Balanced filters — generates trades AND improves Win Rate
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
_HDR  = {"Accept":"application/json","User-Agent":"Ramos360BT/4.0"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
          "1h":"1H","2h":"2H","4h":"4H","1d":"1D","1w":"1W"}

# ── Session ──────────────────────────────────────────────────────────────────
def _sess(h:int)->str:
    if 13<=h<16: return "OVERLAP"
    if  7<=h<12: return "LONDON"
    if 13<=h<17: return "NEW_YORK"
    if  0<=h< 7: return "ASIA"
    return "DEAD_ZONE"

# FIXED: 7-17 UTC + keep ASIA for BTC (24h market)
def _sess_ok(h:int, strict:bool=False)->bool:
    if strict: return h in range(7,17)
    return _sess(h) in ("OVERLAP","LONDON","NEW_YORK","ASIA")

def _sess_q(h:int)->float:
    return {"OVERLAP":1.5,"LONDON":1.2,"NEW_YORK":1.1,"ASIA":0.7,"DEAD_ZONE":0.3}.get(_sess(h),1.0)

# ── OKX Fetch ────────────────────────────────────────────────────────────────
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

# ── Indicators ────────────────────────────────────────────────────────────────
def _rsi(s,p=14):
    if HAS_TA:
        try:
            v=ta.rsi(s,length=p)
            if v is not None: return v.fillna(50)
        except Exception: pass
    d=s.diff(); g=d.clip(lower=0).ewm(alpha=1/p,adjust=False).mean()
    l=(-d.clip(upper=0)).ewm(alpha=1/p,adjust=False).mean()
    return (100-100/(1+g/l.replace(0,1e-10))).fillna(50)

def _ema(s,p):
    if HAS_TA:
        try:
            v=ta.ema(s,length=p)
            if v is not None: return v.ffill().bfill()
        except Exception: pass
    return s.ewm(span=p,adjust=False).mean()

def _mh(s):
    e12=s.ewm(span=12,adjust=False).mean(); e26=s.ewm(span=26,adjust=False).mean()
    ms=e12-e26; return (ms-ms.ewm(span=9,adjust=False).mean()).fillna(0)

def _atr_s(df,p=14):
    hl=df.high-df.low; hpc=(df.high-df.close.shift()).abs(); lpc=(df.low-df.close.shift()).abs()
    return pd.concat([hl,hpc,lpc],axis=1).max(axis=1).ewm(alpha=1/p,adjust=False).mean()

def _adx_s(df,p=14):
    up=(df.high-df.high.shift()).clip(lower=0); dn=(df.low.shift()-df.low).clip(lower=0)
    pdi=up.ewm(alpha=1/p,adjust=False).mean(); mdi=dn.ewm(alpha=1/p,adjust=False).mean()
    return ((pdi-mdi).abs()/(pdi+mdi+1e-10)*100).ewm(alpha=1/p,adjust=False).mean().fillna(15)

def _build(df:pd.DataFrame)->pd.DataFrame:
    df=df.copy(); c=df.close
    df["rsi"]=_rsi(c,14); df["mh"]=_mh(c); df["atr"]=_atr_s(df,14)
    df["adx"]=_adx_s(df,14)
    df["wr"]=(-100*(df.high.rolling(14).max()-c)/(df.high.rolling(14).max()-df.low.rolling(14).min()+1e-10)).fillna(-50)
    df["obv"]=(c.diff().apply(lambda x:1 if x>0 else -1 if x<0 else 0)*df.volume).cumsum()
    df["vm"]=df.volume.rolling(20).mean(); df["vr"]=df.volume/(df.vm.replace(0,1))
    for p in [9,20,50,200]: df[f"e{p}"]=_ema(c,p)
    df["iten"]=(df.high.rolling(9).max()+df.low.rolling(9).min())/2
    df["ikij"]=(df.high.rolling(26).max()+df.low.rolling(26).min())/2
    sa=(df.iten+df.ikij)/2; sb=(df.high.rolling(52).max()+df.low.rolling(52).min())/2
    df["ict"]=pd.concat([sa,sb],axis=1).max(axis=1); df["icb"]=pd.concat([sa,sb],axis=1).min(axis=1)
    mid=c.rolling(20).mean(); sd=c.rolling(20).std()
    df["bbu"]=mid+2*sd; df["bbl"]=mid-2*sd
    df["bbp"]=(c-df.bbl)/(df.bbu-df.bbl+1e-10)*100
    lo14=df.low.rolling(14).min(); hi14=df.high.rolling(14).max()
    df["stoch"]=100*(c-lo14)/(hi14-lo14+1e-10)
    df["hac"]=(df.open+df.high+df.low+df.close)/4; df["hao"]=df["hac"].shift(2)
    df["hab"]=(df.hac>df.hao).astype(int)
    df["wyck"]=0.0
    df.loc[(c>df.e50)&(df.vr>1.2),"wyck"]=1.0
    df.loc[(c<df.e50)&(df.vr>1.2),"wyck"]=-1.0
    return df.ffill().bfill()

# ── HTF Bias ─────────────────────────────────────────────────────────────────
def _htf_bias_series(df4h:Optional[pd.DataFrame])->Optional[Dict]:
    if df4h is None or df4h.empty: return None
    df4=_build(df4h.copy()); bias={}
    for i in range(len(df4)):
        c=float(df4.close.iloc[i])
        e20=float(df4.e20.iloc[i]); e50=float(df4.e50.iloc[i])
        w=float(df4.wyck.iloc[i]); sc=0.0
        if c>e20>e50: sc+=0.7
        elif c<e20<e50: sc-=0.7
        sc+=w*0.5
        bias[df4.ts.iloc[i]]=(1 if sc>0.3 else -1 if sc<-0.3 else 0)
    return bias

def _get_htf(bias:Optional[Dict], ts)->int:
    if not bias: return 0
    past=[v for k,v in bias.items() if k<=ts]
    return past[-1] if past else 0

# ── Layer 1 (allows NEUTRAL) ──────────────────────────────────────────────────
def _l1(df,i)->Tuple[str,float]:
    if i<30: return "NEUTRAL",0.0
    row=df.iloc[i]; c=float(row.close)
    sc=[float(row.wyck)]
    e20,e50,e200=float(row.e20),float(row.e50),float(row.get("e200",row.e50))
    if c>e20>e50: sc.append(0.8)
    elif c<e20<e50: sc.append(-0.8)
    else: sc.append(0.0)
    wr=float(row.wr)
    sc.append(0.6 if wr<-80 else -0.6 if wr>-20 else 0.0)
    avg=sum(sc)/len(sc)
    return ("BULL" if avg>0.20 else "BEAR" if avg<-0.20 else "NEUTRAL"),round(avg,3)

# ── Divergence ────────────────────────────────────────────────────────────────
def _div(df,i)->float:
    if i<25: return 0.0
    ps=df.close.iloc[max(0,i-35):i+1]; rs=df.rsi.iloc[max(0,i-35):i+1]
    lows=[]; highs=[]
    for j in range(4,len(ps)-4):
        w=ps.iloc[j-4:j+5]
        if len(w)<9: continue
        v=ps.iloc[j]
        if v==w.min(): lows.append((j,float(v),float(rs.iloc[j])))
        if v==w.max(): highs.append((j,float(v),float(rs.iloc[j])))
    sc=0.0
    if len(lows)>=2:
        l1,l2=lows[-2],lows[-1]
        if l2[1]<l1[1] and l2[2]>l1[2]: sc+=0.6
        if l2[1]>l1[1] and l2[2]<l1[2]: sc+=0.35
    if len(highs)>=2:
        h1,h2=highs[-2],highs[-1]
        if h2[1]>h1[1] and h2[2]<h1[2]: sc-=0.6
        if h2[1]<h1[1] and h2[2]>h1[2]: sc-=0.35
    if float(df.vr.iloc[i])>1.5 and abs(sc)>0: sc*=1.2
    return round(max(-1.0,min(1.0,sc)),3)

# ── SMC ───────────────────────────────────────────────────────────────────────
def _smc(df,i)->float:
    if i<10: return 0.0
    sc=0.0; c=float(df.close.iloc[i])
    rh=float(df.high.iloc[max(0,i-5):i+1].max())
    rl=float(df.low.iloc[max(0,i-5):i+1].min())
    ph=float(df.high.iloc[max(0,i-10):i-5].max()) if i>=10 else rh
    pl=float(df.low.iloc[max(0,i-10):i-5].min())  if i>=10 else rl
    if rh>ph*1.002 and c>ph: sc+=0.7
    elif rl<pl*0.998 and c<pl: sc-=0.7
    if i>=6:
        plo=float(df.low.iloc[max(0,i-6):i].min()); phi=float(df.high.iloc[max(0,i-6):i].max())
        if float(df.low.iloc[i])<plo*0.999 and c>plo: sc+=0.4
        if float(df.high.iloc[i])>phi*1.001 and c<phi: sc-=0.4
    return round(max(-1.0,min(1.0,sc)),3)

# ── Fib ───────────────────────────────────────────────────────────────────────
def _fib(df,i)->float:
    if i<20: return 0.0
    hi=float(df.high.iloc[max(0,i-50):i+1].max()); lo=float(df.low.iloc[max(0,i-50):i+1].min())
    rng=hi-lo
    if rng<=0: return 0.0
    pos=(float(df.close.iloc[i])-lo)/rng
    for a,b in [(0.289,0.329),(0.384,0.424),(0.598,0.638)]:
        if a<=pos<=b: return 0.7
    for a,b in [(0.730,0.770),(0.760,0.800),(0.789,0.829)]:
        if a<=pos<=b: return -0.7
    return 0.4 if pos<0.35 else -0.4 if pos>0.65 else 0.0

# ── 14 Experts ────────────────────────────────────────────────────────────────
def _experts(df,i,l1)->Tuple[str,float,int]:
    row=df.iloc[i]; c=float(row.close); sc=[]
    # L1 bias
    if l1=="BULL": sc+=[0.7,0.7]
    elif l1=="BEAR": sc+=[-0.7,-0.7]
    else: sc+=[0.0,0.0]  # NEUTRAL still allowed
    # E1 RSI
    r=float(row.rsi); sc.append(0.8 if r<30 else 0.4 if r<42 else -0.8 if r>70 else -0.4 if r>58 else 0.0)
    # E2 MACD
    sc.append(0.6 if float(row.mh)>0 else -0.6)
    # E3 EMA
    e20,e50=float(row.e20),float(row.e50); e200=float(row.get("e200",row.e50))
    if c>e20>e50: sc.append(0.8)
    elif c<e20<e50: sc.append(-0.8)
    else: sc.append(0.0)
    # E4 Ichimoku
    ct,cb=float(row.ict),float(row.icb)
    if c>ct: sc.append(0.8)
    elif c<cb: sc.append(-0.8)
    else: sc.append(0.0)
    # E5 BB
    bp=float(row.bbp); sc.append(0.7 if bp<15 else 0.3 if bp<30 else -0.7 if bp>85 else -0.3 if bp>70 else 0.0)
    # E6 Stoch
    sk=float(row.stoch); sc.append(0.6 if sk<25 else -0.6 if sk>75 else 0.0)
    # E7 ADX directional
    adx=float(row.adx)
    up=(df.high-df.high.shift()).clip(lower=0)
    dn=(df.low.shift()-df.low).clip(lower=0)
    pdi=float(up.ewm(alpha=1/14,adjust=False).mean().iloc[i])
    mdi=float(dn.ewm(alpha=1/14,adjust=False).mean().iloc[i])
    if adx>15: sc.append(0.6 if pdi>mdi else -0.6)
    else: sc.append(0.0)
    # E8 SMC
    sc.append(_smc(df,i))
    # E9 Fibonacci
    sc.append(_fib(df,i))
    # E10 Divergence
    sc.append(_div(df,i))
    # E11 Gann
    root=math.sqrt(max(c,0.001))
    g=[round((root+s)**2,2) for s in [-1,-.5,0,.5,1] if round((root+s)**2,2)>0]
    near=min(g,key=lambda x:abs(x-c)) if g else c
    d=(near-c)/c; sc.append(0.4 if abs(d)<0.003 else 0.25 if d>0 else -0.25)
    # E12 Volume
    vr=float(row.vr); base=sum(sc)/len(sc) if sc else 0
    sc.append(0.4*(1 if base>0 else -1) if vr>1.5 else 0.15*(1 if base>0 else -1) if vr>1.2 else 0.0)
    # E13 Wyckoff
    sc.append(float(row.wyck)*0.7)
    # E14 WR + HA
    wr=float(row.wr); sc.append(0.5 if wr<-80 else -0.5 if wr>-20 else 0.0)
    sc.append(0.3 if float(row.hab)==1 else -0.3)

    n=len(sc); avg=sum(sc)/n if n else 0.0
    bull=sum(1 for s in sc if s>0); bear=sum(1 for s in sc if s<0)
    # RELAXED: 6 votes (was 8), avg 0.25 (was 0.35)
    direction=("LONG" if avg>0.25 and bull>=6 else "SHORT" if avg<-0.25 and bear>=6 else "NEUTRAL")
    return direction, round(avg,4), (bull if avg>0 else bear)

# ── Simulation ────────────────────────────────────────────────────────────────
def _sim(df:pd.DataFrame, balance:float=10_000.0,
          use_sessions:bool=True,
          htf_bias:Optional[Dict]=None)->Dict:
    n=len(df); equity=[balance]; trades=[]
    WARMUP=60; MAX_C=16   # time exit after 16 candles
    in_trade=False; partial=False
    direction=""; entry=sl=tp1=tp2=tp3=atr_v=0.0; entry_idx=0
    pend_sig=None; pend_idx=-1

    for i in range(WARMUP,n):
        row=df.iloc[i]; price=float(row.close)
        hi=float(row.high); lo=float(row.low)
        hour=row.ts.hour if hasattr(row.ts,"hour") else 10

        # ── Manage open trade ─────────────────────────────────────────────
        if in_trade:
            # Time-based exit
            if (i-entry_idx)>MAX_C:
                pnl=((price-entry)/entry*100) if direction=="LONG" else ((entry-price)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.01/max(abs(entry-sl)/entry,0.001)
                balance=max(1.0,balance+pnl/100*risk*mult)
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(price,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if pnl>0 else "LOSS",
                                "exit_type":"TIME_EXIT","session":_sess(hour)})
                in_trade=False; partial=False; equity.append(balance); continue

            # TP/SL hits
            if direction=="LONG":
                hit_sl=lo<=sl; hit_tp3=hi>=tp3; hit_tp2=hi>=tp2 and not hit_tp3
                hit_tp1=hi>=tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl=hi>=sl; hit_tp3=lo<=tp3; hit_tp2=lo<=tp2 and not hit_tp3
                hit_tp1=lo<=tp1 and not hit_tp2 and not hit_tp3

            # Partial exit at TP1 (50% + move SL to breakeven)
            if hit_tp1 and not partial:
                pnl=((tp1-entry)/entry*100) if direction=="LONG" else ((entry-tp1)/entry*100)
                risk=balance*0.01/max(abs(entry-sl)/entry,0.001)
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
                risk=balance*0.01/max(abs(entry-sl)/entry,0.001)
                balance=max(1.0,balance+pnl/100*risk*mult)
                xtype="SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction":direction,"entry":round(entry,4),"exit":round(ex,4),
                                "pnl_pct":round(pnl*mult,3),"result":"WIN" if pnl>0 else "LOSS",
                                "exit_type":xtype,"session":_sess(hour)})
                in_trade=False; partial=False

        # ── Confirmation candle entry ─────────────────────────────────────
        if pend_sig and i==pend_idx:
            direction=pend_sig; entry=price; pend_sig=None; pend_idx=-1
            atr_v=float(df.atr.iloc[max(0,i-1)])
            if atr_v<=0: atr_v=price*0.018
            # ATR × 2.0 SL, Fibonacci TPs
            rng=atr_v*5
            if direction=="LONG":
                sl=round(price-atr_v*2.0,4); tp1=round(price+atr_v*1.5,4)
                tp2=round(price+rng*0.618,4); tp3=round(price+rng*0.809,4)
            else:
                sl=round(price+atr_v*2.0,4); tp1=round(price-atr_v*1.5,4)
                tp2=round(price-rng*0.618,4); tp3=round(price-rng*0.809,4)
            rr=abs(tp1-price)/max(abs(sl-price),1e-10)
            if rr>=1.2:
                in_trade=True; entry_idx=i; partial=False
            equity.append(balance); continue

        # ── Generate new signal ───────────────────────────────────────────
        if not in_trade and pend_sig is None:
            # Session — relaxed (allow ASIA, block only DEAD_ZONE)
            if use_sessions and _sess(hour)=="DEAD_ZONE":
                equity.append(balance); continue

            # ADX > 15 (relaxed from 20)
            if float(row.adx)<15:
                equity.append(balance); continue

            # L1 (NEUTRAL allowed — just penalizes score)
            l1,l1sc=_l1(df,i)

            # HTF soft filter (bonus/penalty, not hard block)
            htf_val=_get_htf(htf_bias,row.ts) if htf_bias else 0
            htf_bonus=0.0
            if l1=="LONG" and htf_val==-1: htf_bonus=-0.15
            if l1=="SHORT" and htf_val==1: htf_bonus=-0.15
            if l1=="LONG" and htf_val==1:  htf_bonus=+0.15
            if l1=="SHORT" and htf_val==-1:htf_bonus=+0.15

            # 14 experts (6 votes minimum)
            sig,avg,votes=_experts(df,i,l1)
            if sig=="NEUTRAL": equity.append(balance); continue

            # Apply HTF bonus
            adj_avg=avg+htf_bonus
            if abs(adj_avg)<0.20: equity.append(balance); continue
            if votes<6: equity.append(balance); continue

            # Session quality gate (only block very low quality)
            if _sess_q(hour)<0.5: equity.append(balance); continue

            # Queue for next candle confirmation
            pend_sig=sig; pend_idx=i+1

        equity.append(balance)

    # Close remaining
    if in_trade:
        lp=float(df.close.iloc[-1])
        pnl=((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        trades.append({"direction":direction,"entry":entry,"exit":lp,
                        "pnl_pct":round(pnl*(0.5 if partial else 1.0),3),
                        "result":"WIN" if pnl>0 else "LOSS","exit_type":"OPEN_AT_END"})

    return {"trades":trades,"equity":equity,"final_balance":balance}

# ── Metrics ───────────────────────────────────────────────────────────────────
def _metrics(sim:Dict,initial:float)->Dict:
    trades=sim["trades"]; eq=pd.Series(sim["equity"]); bal=sim["final_balance"]
    if not trades:
        return {"error":"No trades","total":0,"win_rate_pct":0,
                "max_dd_pct":0,"return_pct":0,"sharpe":0}
    wins=[t for t in trades if t["result"]=="WIN"]; losses=[t for t in trades if t["result"]=="LOSS"]
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
                htf=None
                if resolved in ("1h","15m","5m"):
                    df4h=await _fetch(sym,"4h",sdt,edt)
                    if df4h is not None and len(df4h)>=30:
                        htf=_htf_bias_series(df4h)
                df=_build(df)
                sim=_sim(df,balance,use_sessions=use_sessions,htf_bias=htf)
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
        lines=["📈 <b>Backtest — Ramos 360 Ai 🎖️  v4</b>",
               f"📅 Period: 2026-01-01 → 2026-05-01",
               f"⏱️ Timeframe: {tf.upper()} | 14 Experts",
               "✅ 8 Improvements | Balanced Filters",
               "━━━━━━━━━━━━━━━━━━━━━━━━"]
        for sym,r in results.items():
            if "error" in r: lines.append(f"❌ {sym}: {r['error']}"); continue
            ei="🟢" if r.get("return_pct",0)>0 else "🔴"
            ex=" ".join(f"{k}:{v}" for k,v in r.get("exit_breakdown",{}).items())
            ss=" ".join(f"{k}:{v}" for k,v in r.get("session_breakdown",{}).items())
            lines+=[f"",f"{ei} <b>{sym}</b>",
                    f"  📊 {r['total']} trades  ({r.get('wins',0)}W / {r.get('losses',0)}L)",
                    f"  🎯 Win Rate:   {r['win_rate_pct']:.1f}%",
                    f"  💰 Return:     {r.get('return_pct',0):+.2f}%",
                    f"  📉 Max DD:     {r.get('max_dd_pct',0):.2f}%",
                    f"  ⚖️ Sharpe:     {r.get('sharpe',0):.3f}",
                    f"  🔮 Expectancy: {r.get('expectancy',0):+.3f}%",
                    f"  🏦 Balance:    ${r.get('final_balance',10000):,.2f}",
                    f"  📋 Exits:      {ex}",
                    f"  🕐 Sessions:   {ss}"]
        lines+=["━━━━━━━━━━━━━━━━━━━━━━━━",
                "<i>🎖️ Ramos 360 Ai — Backtest v4</i>"]
        return "\n".join(lines)

async def _main():
    e=BacktestEngine()
    r=await e.run(symbols=["BTC/USDT:USDT"],timeframe="1h",
                   start="2026-01-01",end="2026-05-01",balance=10_000.0)
    print("\n"+e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))

if __name__=="__main__":
    asyncio.run(_main())
