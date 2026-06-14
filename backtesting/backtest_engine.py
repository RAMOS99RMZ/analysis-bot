"""
backtesting/backtest_engine.py — Ramos 360 Ai 🎖️  FINAL
FIXED ROOT CAUSE: RR was 0.75 (SL 2×ATR, TP1 1.5×ATR) → no trades ever
NEW: SL 1.2×ATR, TP1 2.0×ATR → RR = 1.67 ✅
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
_HDR  = {"Accept":"application/json","User-Agent":"Ramos360BT/FINAL"}
_TFM  = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m",
          "1h":"1H","4h":"4H","1d":"1D","1w":"1W"}

# ── Session ────────────────────────────────────────────────────────────────
def _sess(h:int)->str:
    if 13<=h<16: return "OVERLAP"
    if  7<=h<12: return "LONDON"
    if 13<=h<17: return "NEW_YORK"
    if  0<=h< 7: return "ASIA"
    return "DEAD_ZONE"

def _sess_q(h:int)->float:
    return {"OVERLAP":1.5,"LONDON":1.2,"NEW_YORK":1.1,"ASIA":0.7,"DEAD_ZONE":0.2}.get(_sess(h),1.0)

# ── OKX Fetch ──────────────────────────────────────────────────────────────
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

# ── Indicators (all computed upfront) ────────────────────────────────────
def _build(df:pd.DataFrame)->pd.DataFrame:
    df=df.copy(); c=df.close; h=df.high; l=df.low
    # RSI
    if HAS_TA:
        try:
            v=ta.rsi(c,length=14)
            if v is not None: df["rsi"]=v.fillna(50)
            else: raise ValueError()
        except Exception:
            d=c.diff(); g=d.clip(lower=0).ewm(alpha=1/14,adjust=False).mean()
            ls=(-d.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
            df["rsi"]=(100-100/(1+g/ls.replace(0,1e-10))).fillna(50)
    else:
        d=c.diff(); g=d.clip(lower=0).ewm(alpha=1/14,adjust=False).mean()
        ls=(-d.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
        df["rsi"]=(100-100/(1+g/ls.replace(0,1e-10))).fillna(50)
    # MACD
    e12=c.ewm(span=12,adjust=False).mean(); e26=c.ewm(span=26,adjust=False).mean()
    ms=e12-e26; df["macd_h"]=(ms-ms.ewm(span=9,adjust=False).mean()).fillna(0)
    # EMA
    for p in [9,20,50,100,200]: df[f"ema{p}"]=c.ewm(span=p,adjust=False).mean()
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
    # Stochastic
    lo14=l.rolling(14).min(); hi14=h.rolling(14).max()
    df["stoch"]=100*(c-lo14)/(hi14-lo14+1e-10)
    # Williams %R
    df["wr"]=(-100*(hi14-c)/(hi14-lo14+1e-10)).fillna(-50)
    # OBV
    df["obv"]=(c.diff().apply(lambda x:1 if x>0 else -1 if x<0 else 0)*df.volume).cumsum()
    # Volume ratio
    df["vm"]=df.volume.rolling(20).mean(); df["vr"]=df.volume/(df.vm.replace(0,1))
    # Ichimoku
    df["iten"]=(h.rolling(9).max()+l.rolling(9).min())/2
    df["ikij"]=(h.rolling(26).max()+l.rolling(26).min())/2
    sa=(df.iten+df.ikij)/2; sb=(h.rolling(52).max()+l.rolling(52).min())/2
    df["ict"]=pd.concat([sa,sb],axis=1).max(axis=1)
    df["icb"]=pd.concat([sa,sb],axis=1).min(axis=1)
    # Heikin Ashi
    df["hac"]=(df.open+h+l+c)/4; df["hao"]=df.hac.shift(2)
    df["hab"]=(df.hac>df.hao).astype(int)
    # Wyckoff simplified
    df["wyck"]=0.0
    df.loc[(c>df.ema50)&(df.vr>1.2),"wyck"]=1.0
    df.loc[(c<df.ema50)&(df.vr>1.2),"wyck"]=-1.0
    return df.ffill().bfill()

# ── Divergence (pre-computed column) ─────────────────────────────────────
def _add_divergence(df:pd.DataFrame)->pd.DataFrame:
    """Add div_score column to avoid O(n²) computation in loop."""
    scores=pd.Series(0.0,index=df.index)
    pivot_lows=[]; pivot_highs=[]
    for i in range(4,len(df)-4):
        lo_w=df.low.iloc[i-4:i+5]
        hi_w=df.high.iloc[i-4:i+5]
        if len(lo_w)<9: continue
        if float(df.low.iloc[i])==float(lo_w.min()):
            pivot_lows.append((i,float(df.low.iloc[i]),float(df.rsi.iloc[i])))
        if float(df.high.iloc[i])==float(hi_w.max()):
            pivot_highs.append((i,float(df.high.iloc[i]),float(df.rsi.iloc[i])))
        # Check divergence at current pivot
        if len(pivot_lows)>=2:
            l1,l2=pivot_lows[-2],pivot_lows[-1]
            if l2[1]<l1[1] and l2[2]>l1[2]: scores.iloc[i]+=0.6    # Reg Bull
            if l2[1]>l1[1] and l2[2]<l1[2]: scores.iloc[i]+=0.35   # Hid Bull
        if len(pivot_highs)>=2:
            h1,h2=pivot_highs[-2],pivot_highs[-1]
            if h2[1]>h1[1] and h2[2]<h1[2]: scores.iloc[i]-=0.6    # Reg Bear
            if h2[1]<h1[1] and h2[2]>h1[2]: scores.iloc[i]-=0.35   # Hid Bear
    # Volume boost
    scores=scores*df.vr.clip(upper=2.0)
    df["div"]=scores.clip(-1,1).fillna(0)
    return df

# ── Scoring function ───────────────────────────────────────────────────────
def _score(df:pd.DataFrame, i:int)->Tuple[str,float,int]:
    """Score candle at index i. Returns (direction, avg, votes)."""
    row=df.iloc[i]; c=float(row.close); sc=[]

    # E1: EMA Alignment
    e20=float(row.ema20); e50=float(row.ema50); e200=float(row.ema200)
    if c>e20>e50>e200: sc.append(1.0)
    elif c>e20>e50:    sc.append(0.6)
    elif c<e20<e50<e200: sc.append(-1.0)
    elif c<e20<e50:    sc.append(-0.6)
    else:              sc.append(0.0)

    # E2: RSI
    r=float(row.rsi)
    sc.append(0.9 if r<28 else 0.5 if r<40 else 0.1 if r<50
              else -0.9 if r>72 else -0.5 if r>60 else -0.1)

    # E3: MACD
    sc.append(0.6 if float(row.macd_h)>0 else -0.6)

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
    sc.append(0.8 if bp<12 else 0.35 if bp<28 else -0.8 if bp>88 else -0.35 if bp>72 else 0.0)

    # E6: Stochastic
    sk=float(row.stoch)
    sc.append(0.65 if sk<22 else 0.25 if sk<38 else -0.65 if sk>78 else -0.25 if sk>62 else 0.0)

    # E7: ADX + Direction
    adx=float(row.adx); pdi=float(row.pdi); mdi=float(row.mdi)
    mult=1.0 if adx>25 else 0.5 if adx>15 else 0.2
    sc.append(0.7*mult if pdi>mdi else -0.7*mult)

    # E8: Wyckoff
    sc.append(float(row.wyck)*0.8)

    # E9: Williams %R
    wr=float(row.wr)
    sc.append(0.7 if wr<-80 else 0.3 if wr<-60 else -0.7 if wr>-20 else -0.3 if wr>-40 else 0.0)

    # E10: Volume confirmation
    vr=float(row.vr)
    base=sum(sc)/len(sc) if sc else 0
    sc.append(0.5 if vr>1.8 and base>0 else 0.25 if vr>1.3 and base>0
              else -0.5 if vr>1.8 and base<0 else -0.25 if vr>1.3 and base<0 else 0.0)

    # E11: Divergence (pre-computed)
    sc.append(float(row.get("div",0))*0.8)

    # E12: Heikin Ashi
    sc.append(0.4 if float(row.hab)==1 else -0.4)

    # E13: Fibonacci position
    if i>=20:
        hi60=float(df.high.iloc[max(0,i-50):i+1].max())
        lo60=float(df.low.iloc[max(0,i-50):i+1].min())
        rng=hi60-lo60
        if rng>0:
            pos=(c-lo60)/rng
            if pos<0.35:   sc.append(0.6)
            elif pos>0.65: sc.append(-0.6)
            else:          sc.append(0.0)
        else: sc.append(0.0)
    else: sc.append(0.0)

    # E14: Gann Square of 9
    root=math.sqrt(max(c,0.001))
    g=[round((root+s)**2,2) for s in [-1.5,-1,-.5,0,.5,1,1.5] if round((root+s)**2,2)>0]
    if g:
        near=min(g,key=lambda x:abs(x-c)); d=(near-c)/c
        sc.append(0.4 if abs(d)<0.004 else 0.2 if d>0 else -0.2)
    else: sc.append(0.0)

    n=len(sc); avg=sum(sc)/n if n else 0.0
    bull=sum(1 for s in sc if s>0.05)
    bear=sum(1 for s in sc if s<-0.05)

    # Relaxed thresholds: avg>0.18 AND 5+ votes
    direction=("LONG"  if avg> 0.18 and bull>=5 else
               "SHORT" if avg<-0.18 and bear>=5 else "NEUTRAL")
    return direction, round(avg,4), (bull if avg>=0 else bear)

# ── Simulation ────────────────────────────────────────────────────────────
def _sim(df:pd.DataFrame, balance:float=10_000.0, use_sessions:bool=True)->Dict:
    n=len(df); equity=[balance]; trades=[]
    WARMUP=60; MAX_C=18   # max candles in trade before time exit
    in_trade=False; partial=False
    direction=""; entry=sl=tp1=tp2=tp3=atr_v=0.0; entry_idx=0

    for i in range(WARMUP,n):
        row=df.iloc[i]; price=float(row.close)
        hi=float(row.high); lo=float(row.low)
        hour=row.ts.hour if hasattr(row.ts,"hour") else 10

        # ── Manage trade ───────────────────────────────────────────────────
        if in_trade:
            # Time exit
            if (i-entry_idx)>MAX_C:
                pnl=((price-entry)/entry*100) if direction=="LONG" else ((entry-price)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.01/max(abs(entry-sl)/entry,0.001)
                balance=max(1.0,balance+pnl/100*risk*mult)
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(price,4),"pnl_pct":round(pnl*mult,3),
                                "result":"WIN" if pnl>0 else "LOSS",
                                "exit_type":"TIME_EXIT","session":_sess(hour)})
                in_trade=False; partial=False; equity.append(balance); continue

            # Compute hits
            if direction=="LONG":
                hit_sl=lo<=sl; hit_tp3=hi>=tp3
                hit_tp2=hi>=tp2 and not hit_tp3
                hit_tp1=hi>=tp1 and not hit_tp2 and not hit_tp3
            else:
                hit_sl=hi>=sl; hit_tp3=lo<=tp3
                hit_tp2=lo<=tp2 and not hit_tp3
                hit_tp1=lo<=tp1 and not hit_tp2 and not hit_tp3

            # Partial exit at TP1 (50% + move SL to breakeven)
            if hit_tp1 and not partial:
                pnl=((tp1-entry)/entry*100) if direction=="LONG" else ((entry-tp1)/entry*100)
                risk=balance*0.01/max(abs(entry-sl)/entry,0.001)
                balance=max(1.0,balance+pnl/100*risk*0.5)
                sl=entry; partial=True
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(tp1,4),"pnl_pct":round(pnl*0.5,3),
                                "result":"WIN","exit_type":"TP1_PARTIAL","session":_sess(hour)})
                equity.append(balance); continue

            if hit_tp2 or hit_tp3 or hit_sl:
                ex=sl if hit_sl else (tp3 if hit_tp3 else tp2)
                pnl=((ex-entry)/entry*100) if direction=="LONG" else ((entry-ex)/entry*100)
                mult=0.5 if partial else 1.0
                risk=balance*0.01/max(abs(entry-sl)/entry,0.001)
                balance=max(1.0,balance+pnl/100*risk*mult)
                xtype="SL" if hit_sl else ("TP3" if hit_tp3 else "TP2")
                trades.append({"direction":direction,"entry":round(entry,4),
                                "exit":round(ex,4),"pnl_pct":round(pnl*mult,3),
                                "result":"WIN" if pnl>0 else "LOSS",
                                "exit_type":xtype,"session":_sess(hour)})
                in_trade=False; partial=False

        # ── New signal ─────────────────────────────────────────────────────
        if not in_trade:
            # Session: block only DEAD_ZONE for low quality
            sq=_sess_q(hour)
            if use_sessions and sq<0.3: equity.append(balance); continue

            # ADX must be at least weak trend (12+)
            if float(row.adx)<12: equity.append(balance); continue

            # Score all experts
            sig,avg,votes=_score(df,i)
            if sig=="NEUTRAL": equity.append(balance); continue

            # Adjust score by session quality
            if abs(avg)*sq < 0.15: equity.append(balance); continue

            # Build levels — FIXED RR
            atr_v=float(row.atr) if float(row.atr)>0 else price*0.015
            rng=atr_v*7   # wider projection range

            if sig=="LONG":
                sl  = round(price - atr_v*1.2, 4)   # SL = 1.2 × ATR
                tp1 = round(price + atr_v*2.0, 4)   # TP1 = 2.0 × ATR → RR=1.67 ✅
                tp2 = round(price + rng*0.618, 4)   # Fib 61.8%
                tp3 = round(price + rng*0.809, 4)   # Fib 80.9%
            else:
                sl  = round(price + atr_v*1.2, 4)
                tp1 = round(price - atr_v*2.0, 4)
                tp2 = round(price - rng*0.618, 4)
                tp3 = round(price - rng*0.809, 4)

            # Verify RR ≥ 1.5
            rr=abs(tp1-price)/max(abs(sl-price),1e-10)
            if rr<1.5: equity.append(balance); continue

            direction=sig; entry=price
            in_trade=True; entry_idx=i; partial=False

        equity.append(balance)

    # Close remaining
    if in_trade:
        lp=float(df.close.iloc[-1])
        pnl=((lp-entry)/entry*100) if direction=="LONG" else ((entry-lp)/entry*100)
        trades.append({"direction":direction,"entry":entry,"exit":lp,
                        "pnl_pct":round(pnl*(0.5 if partial else 1.0),3),
                        "result":"WIN" if pnl>0 else "LOSS","exit_type":"OPEN_AT_END"})

    return {"trades":trades,"equity":equity,"final_balance":balance}

# ── Metrics ────────────────────────────────────────────────────────────────
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

# ── BacktestEngine ──────────────────────────────────────────────────────────
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
                df=_build(df); df=_add_divergence(df)
                sim=_sim(df,balance,use_sessions=use_sessions)
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
        lines=["📈 <b>Backtest — Ramos 360 Ai 🎖️  FINAL</b>",
               f"📅 Period: 2026-01-01 → 2026-05-01",
               f"⏱️ Timeframe: {tf.upper()} | 14 Experts",
               "✅ Fixed RR(1.67) + Session + ATR SL + Partial Exit",
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
                "<i>🎖️ Ramos 360 Ai — Backtest FINAL</i>"]
        return "\n".join(lines)

async def _main():
    e=BacktestEngine()
    r=await e.run(symbols=["BTC/USDT:USDT"],timeframe="1h",
                   start="2026-01-01",end="2026-05-01",balance=10_000.0)
    print("\n"+e.format_report(r).replace("<b>","").replace("</b>","")
          .replace("<i>","").replace("</i>",""))

if __name__=="__main__":
    asyncio.run(_main())
