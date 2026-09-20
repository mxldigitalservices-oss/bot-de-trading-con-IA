import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _f(n, d): return float(os.getenv(n, d))
def _i(n, d): return int(os.getenv(n, d))
def _b(n, d="0"): return os.getenv(n, d) == "1"


def _load_keys():
    """Lee GEMINI_KEYS (separadas por coma) y/o GEMINI_KEYS1..GEMINI_KEYS10 (una por variable)."""
    raw = os.getenv("GEMINI_KEYS", "").split(",")
    raw += [os.getenv(f"GEMINI_KEYS{i}", "") for i in range(1, 11)]
    out = []
    for k in (x.strip() for x in raw):
        if k and k not in out:
            out.append(k)
    return out


@dataclass
class Config:
    symbol: str = os.getenv("SYMBOL", "BTCUSDT").upper()
    capital_usd: float = _f("CAPITAL_USD", 10)
    fee_bps: float = _f("FEE_BPS", 10)
    slip_bps: float = _f("SLIP_BPS", 1)
    safety_margin: float = _f("SAFETY_MARGIN", 1.5)
    max_spread_bps: float = _f("MAX_SPREAD_BPS", 2)
    min_depth_usd: float = _f("MIN_DEPTH_USD", 20000)
    edge_k: float = _f("EDGE_K", 0.5)
    horizon_s: int = 30

    latency_ms: int = _i("SIM_LATENCY_MS", 200)
    cooldown_s: int = _i("TRIGGER_COOLDOWN_S", 45)
    log_cooldown_s: int = 10
    time_stop_s: int = _i("TIME_STOP_S", 120)
    risk_pct: float = _f("RISK_PCT", 0.02)
    max_daily_loss_usd: float = _f("MAX_DAILY_LOSS_USD", 0.5)
    max_consecutive_errors: int = 5
    demo_mode: bool = _b("DEMO_MODE")

    use_gemini: bool = _b("USE_GEMINI")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    gemini_keys: list = field(default_factory=_load_keys)
    gemini_max_per_hour: int = _i("GEMINI_MAX_PER_HOUR", 60)
    gemini_ttl_s: float = 3.0
    gemini_min_conf: float = 0.6

    db_path: str = os.getenv("DB_PATH", "mxl.db")
