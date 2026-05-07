from . import (
    expert_01_classic_ta, expert_02_harmonic, expert_03_wyckoff,
    expert_04_smc, expert_05_onchain, expert_06_sessions,
    expert_07_fear_greed, expert_08_gann, expert_09_obv,
    expert_10_daily, expert_11_usdt,
)
EXPERTS = [
    expert_01_classic_ta, expert_02_harmonic, expert_03_wyckoff,
    expert_04_smc, expert_05_onchain, expert_06_sessions,
    expert_07_fear_greed, expert_08_gann, expert_09_obv,
    expert_10_daily, expert_11_usdt,
]
EXPERT_NAMES = ["ClassicTA","Harmonic","Wyckoff","SMC","OnChain",
                "Sessions","FearGreed","Gann","OBV","Daily","USDT"]
# Expert base weights (from GAS getConsensus)
EXPERT_WEIGHTS = [1.0,1.2,1.3,1.4,1.1,0.9,0.8,1.2,1.0,1.3,1.1]
