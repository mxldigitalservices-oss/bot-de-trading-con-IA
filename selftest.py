"""Prueba sintetica offline: alimenta el motor con libros aleatorios (DEMO_MODE) y
verifica que el pipeline completo (senal -> entrada -> salida -> etiquetas -> DB) funciona."""
import os, random, tempfile
os.environ.update(DEMO_MODE="1", MIN_DEPTH_USD="0", DB_PATH=os.path.join(tempfile.mkdtemp(), "t.db"),
                  TRIGGER_COOLDOWN_S="5", TIME_STOP_S="20")
from config import Config
from db import DB
from engine import Engine
from exchange import DEFAULT_FILTERS

cfg = Config()
db = DB(cfg.db_path)
eng = Engine(cfg, db, dict(DEFAULT_FILTERS))
random.seed(7)
mid, t = 60000.0, 1_700_000_000.0
for i in range(30000):
    t += 0.1
    mid *= 1 + random.gauss(0, 0.00002)
    bids = [(round(mid - 0.5 - j, 2), random.uniform(0.2, 2.0)) for j in range(5)]
    asks = [(round(mid + 0.5 + j, 2), random.uniform(0.2, 2.0)) for j in range(5)]
    eng.on_book(t, bids, asks, True)
db.flush(True)
print("stats:", eng.stats)
print("estado:", eng.state.value, "| cash:", round(eng.cash, 4), "| pnl dia:", round(eng.daily_pnl, 4))
print("senales:", db.rows("SELECT COUNT(*) n FROM signals")[0]["n"],
      "| etiquetadas:", db.rows("SELECT COUNT(*) n FROM signals WHERE ret_120s IS NOT NULL")[0]["n"],
      "| trades:", db.rows("SELECT COUNT(*) n FROM trades")[0]["n"])
assert eng.stats["signals"] > 0 and eng.stats["trades"] > 0, "el pipeline no genero senales/trades"
print("OK")
print(db.rows("SELECT * FROM trades")[0])
print("dust:", eng.dust)
