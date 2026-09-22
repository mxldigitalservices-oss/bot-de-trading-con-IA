"""Backend: ingesta WebSocket + motor + Gemini opcional + API/WS para el dashboard."""
import asyncio, base64, json, logging, os, secrets, time
from contextlib import asynccontextmanager
from statistics import mean

import uvicorn, websockets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from config import Config
from db import DB
from engine import Engine, St
from exchange import fetch_filters
from gemini import RotadorLlaves, estratega

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mxl")
ctx = {}
clients = set()          # una cola por navegador conectado al WebSocket


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


class BasicAuth:
    """Pide usuario/clave (DASHBOARD_USER / DASHBOARD_PASSWORD) a TODO: pagina, API y WebSocket.
    Imprescindible en Railway: /api/kill no debe quedar abierto a internet."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        pwd = os.getenv("DASHBOARD_PASSWORD")
        if not pwd or scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        user = os.getenv("DASHBOARD_USER", "admin")
        h = dict(scope["headers"]).get(b"authorization", b"").decode()
        ok = False
        if h.lower().startswith("basic "):
            try:
                u, _, p = base64.b64decode(h[6:]).decode().partition(":")
                ok = (secrets.compare_digest(u.encode(), user.encode())
                      and secrets.compare_digest(p.encode(), pwd.encode()))
            except Exception:
                ok = False
        if ok:
            return await self.app(scope, receive, send)
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        await send({"type": "http.response.start", "status": 401, "headers": [
            (b"www-authenticate", b'Basic realm="MXL"'), (b"content-type", b"text/plain; charset=utf-8")]})
        await send({"type": "http.response.body", "body": "Acceso restringido".encode()})


app.add_middleware(BasicAuth)


# ---------------------------------------------------------------------------
# Estado y control en tiempo real.
# IMPORTANTE: todo es `async def` => corre en el hilo del event loop, el mismo que procesa el
# libro de ordenes. Con `def` normal FastAPI usaria un hilo aparte y kill() modificaria el
# motor a la vez que on_book(): condicion de carrera.
# ---------------------------------------------------------------------------
def snapshot():
    st = ctx["engine"].status()
    st["last_action"] = ctx.get("last_action")
    return st


def push_now():
    """Empuja el estado a TODOS los navegadores conectados, sin esperar al ciclo de 1 s."""
    snap = snapshot()
    for q in list(clients):
        try:
            q.put_nowait(snap)
        except asyncio.QueueFull:
            pass


def _cancel_pending_entry(engine):
    """Al detener, una compra aun no ejecutada se cancela (evita abrir y cerrar pagando comisiones)."""
    p = engine.pending
    if p and p["kind"] == "buy":
        engine.pending = None
        engine.state = St.IDLE
        engine.db.update_signal(p["sid"], decision="cancelada_por_kill")
    elif engine.state == St.AWAITING_LLM:
        engine.state = St.IDLE


def _record(action, request):
    who = request.client.host if request.client else "?"
    ctx["last_action"] = {"action": action, "at": time.time(), "by": who}
    log.warning("CONTROL %s desde %s", action, who)


@app.get("/")
async def index():
    return FileResponse("static/index.html")


@app.get("/api/status")
async def status():
    return snapshot()


@app.get("/api/signals")
async def signals(limit: int = 40):
    return ctx["db"].rows("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,))


@app.get("/api/trades")
async def trades(limit: int = 40):
    return ctx["db"].rows("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))


@app.get("/api/stats")
async def stats():
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
async def kill(request: Request):
    """Detiene: no abre posiciones nuevas, cancela entradas en curso y cierra la posicion abierta."""
    eng = ctx["engine"]
    if not eng.halted:
        eng.kill("manual")
    _cancel_pending_entry(eng)
    _record("kill", request)
    push_now()
    return {"ok": True, "status": snapshot()}


@app.post("/api/resume")
async def resume(request: Request):
    eng, cfg = ctx["engine"], ctx["cfg"]
    if eng.halt_reason == "perdida_diaria" and eng.daily_pnl <= -cfg.max_daily_loss_usd:
        return JSONResponse(status_code=409, content={
            "ok": False,
            "error": "Se alcanzo el limite de perdida diaria. Se reanuda solo al cambiar el dia (UTC).",
            "status": snapshot()})
    eng.resume()
    _record("resume", request)
    push_now()
    return {"ok": True, "status": snapshot()}


@app.websocket("/ws")
async def ws(sock: WebSocket):
    await sock.accept()
    q = asyncio.Queue(maxsize=4)
    clients.add(q)
    try:
        await sock.send_json(snapshot())
        while True:
            try:
                snap = await asyncio.wait_for(q.get(), timeout=1.0)   # cambio de control: al instante
            except asyncio.TimeoutError:
                snap = snapshot()                                     # latido normal cada 1 s
            await sock.send_json(snap)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        clients.discard(q)


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    if host != "127.0.0.1" and not os.getenv("DASHBOARD_PASSWORD"):
        raise SystemExit("Seguridad: define DASHBOARD_PASSWORD antes de exponer el servidor "
                         "(HOST != 127.0.0.1). /api/kill controla el bot.")
    uvicorn.run(app, host=host, port=port)
