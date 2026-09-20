"""Backend: ingesta WebSocket + motor + Gemini opcional + API/WS para el dashboard."""
import asyncio, json, logging, time
from contextlib import asynccontextmanager
from statistics import mean

import uvicorn, websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from config import Config
from db import DB
from engine import Engine
from exchange import fetch_filters
from gemini import RotadorLlaves, estratega

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mxl")
ctx = {}


async def ingesta(cfg, engine, db):
    """Nivel 0: solo procesa trades de decision si cambio el mejor bid/ask.
    depth5@100ms envia snapshots completos => no hay libro que sincronizar."""
    url = f"wss://stream.binance.com:9443/ws/{cfg.symbol.lower()}@depth5@100ms"
    last_top, last_rec, backoff = None, 0.0, 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
                log.info("conectado a %s", url)
                backoff = 1
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)   # watchdog de datos
                    now = time.time()
                    d = json.loads(msg)
                    bids = [(float(p), float(q)) for p, q in d["bids"]]
                    asks = [(float(p), float(q)) for p, q in d["asks"]]
                    if not bids or not asks:
                        continue
                    engine.health["last_msg"] = now
                    top = (bids[0], asks[0])
                    changed = top != last_top
                    last_top = top
                    if changed and now - last_rec >= 1:               # grabacion 1/seg
                        db.insert_book(now, bids, asks)
                        last_rec = now
                    engine.on_book(now, bids, asks, changed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            engine.health["last_error"] = repr(e)
            log.warning("feed caido: %r; reintento en %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def flusher(db):
    while True:
        await asyncio.sleep(2)
        db.flush(True)


@asynccontextmanager
async def lifespan(app):
    cfg = Config()
    db = DB(cfg.db_path)
    filters = await fetch_filters(cfg.symbol)
    engine = Engine(cfg, db, filters)
    tasks = [asyncio.create_task(ingesta(cfg, engine, db)), asyncio.create_task(flusher(db))]
    if cfg.use_gemini and cfg.gemini_keys:
        rot = RotadorLlaves(cfg.gemini_keys, max_por_hora=cfg.gemini_max_per_hour)
        engine.llm_queue = asyncio.Queue(maxsize=5)
        engine.extra_status = lambda: {"enabled": True, **rot.info()}
        tasks.append(asyncio.create_task(estratega(engine, engine.llm_queue, rot, cfg)))
    else:
        engine.extra_status = lambda: {"enabled": False}
    ctx.update(cfg=cfg, db=db, engine=engine)
    yield
    for t in tasks:
        t.cancel()
    db.flush(True)


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


@app.get("/api/status")
def status():
    return ctx["engine"].status()


@app.get("/api/signals")
def signals(limit: int = 40):
    return ctx["db"].rows("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,))


@app.get("/api/trades")
def trades(limit: int = 40):
    return ctx["db"].rows("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))


@app.get("/api/stats")
def stats():
    """La matriz: que tan bien predijo el filtro, con o sin ejecucion (contrafactual)."""
    rows = ctx["db"].rows("SELECT * FROM signals WHERE ret_120s IS NOT NULL")
    tr = ctx["db"].rows("SELECT pnl_usd, pnl_bps FROM trades")

    def avg(rs, k):
        v = [r[k] for r in rs if r[k] is not None]
        return round(mean(v), 3) if v else None

    viable = [r for r in rows if r["viable"]]
    net30 = [r["ret_30s"] - r["breakeven_bps"] for r in viable]
    return {
        "senales_etiquetadas": len(rows), "senales_viables": len(viable),
        "todas": {k: avg(rows, k) for k in ("ret_5s", "ret_30s", "ret_120s")},
        "viables": {k: avg(viable, k) for k in ("ret_5s", "ret_30s", "ret_120s")},
        "viables_neto_30s_bps": round(mean(net30), 3) if net30 else None,
        "acierto_30s": round(sum(r["ret_30s"] > 0 for r in rows) / len(rows), 3) if rows else None,
        "trades": len(tr), "pnl_usd": round(sum(t["pnl_usd"] for t in tr), 4),
        "ganadores": sum(t["pnl_usd"] > 0 for t in tr),
    }


@app.post("/api/kill")
def kill():
    ctx["engine"].kill("manual")
    return {"ok": True}


@app.post("/api/resume")
def resume():
    ctx["engine"].resume()
    return {"ok": True}


@app.websocket("/ws")
async def ws(sock: WebSocket):
    await sock.accept()
    try:
        while True:
            await sock.send_json(ctx["engine"].status())
            await asyncio.sleep(1)
    except (WebSocketDisconnect, RuntimeError):
        pass


if __name__ == "__main__":
    # 127.0.0.1: /api/kill no tiene autenticacion, NO lo expongas a internet.
    uvicorn.run(app, host="127.0.0.1", port=8000)
