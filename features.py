"""Nivel 2-3 del embudo: features baratas y estimador de edge."""
import math
from collections import deque


class FeatureEngine:
    def __init__(self, horizon_s=30, n=600):
        self.h = horizon_s
        self.mids = deque(maxlen=n)      # 1 muestra por segundo
        self.times = deque(maxlen=n)
        self._t = 0.0
        self._vol1s = None

    def update(self, bids, asks, now):
        bb, bq = bids[0]
        ba, aq = asks[0]
        mid = (bb + ba) / 2
        if now - self._t >= 1.0:
            self.mids.append(mid)
            self.times.append(now)
            self._t = now
            self._vol1s = self._calc_vol()
        if self._vol1s is None:
            return None
        vb = sum(q for _, q in bids[:5])
        va = sum(q for _, q in asks[:5])
        micro = (bb * aq + ba * bq) / (bq + aq)
        return {
            "mid": mid, "bid": bb, "ask": ba,
            "spread_bps": (ba - bb) / mid * 1e4,
            "imb": (vb - va) / (vb + va),
            "micro_bps": (micro / mid - 1) * 1e4,
            "vol_1s_bps": self._vol1s,
            "vol_bps_h": self._vol1s * math.sqrt(self.h),
            "depth_usd": min(sum(p * q for p, q in bids[:5]), sum(p * q for p, q in asks[:5])),
        }

    def _calc_vol(self):
        if len(self.mids) < 60:
            return None
        m = list(self.mids)
        r = [math.log(m[i] / m[i - 1]) for i in range(1, len(m))]
        mean = sum(r) / len(r)
        return math.sqrt(sum((x - mean) ** 2 for x in r) / len(r)) * 1e4


def edge_estimate_bps(f, k):
    """Movimiento esperado (bps) en el horizonte. HEURISTICA INICIAL: spot es solo long,
    asi que solo hay edge si el libro presiona al alza. Calibra `k` con la matriz de senales
    (columnas imb/micro_bps/ret_30s), no la des por buena."""
    if f["imb"] <= 0:
        return 0.0
    return k * (f["imb"] * f["vol_bps_h"] + max(f["micro_bps"], 0.0))
