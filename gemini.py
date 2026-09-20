"""La cabeza estrategica: rotador round-robin + llamada REST a Gemini como VETO.
Gemini nunca decide precio ni cantidad. Solo permite o veta una senal ya calculada."""
import asyncio, json, time
from collections import deque
import httpx


class RateLimit(Exception):
    pass


class RotadorLlaves:
    def __init__(self, llaves, cooldown_429=60, max_por_hora=60):
        self.llaves = llaves
        self.libre_desde = {k: 0.0 for k in llaves}
        self.idx = 0
        self.lock = asyncio.Lock()
        self.cooldown = cooldown_429
        self.max_por_hora = max_por_hora
        self.llamadas = deque()

    async def siguiente(self):
        async with self.lock:
            now = time.time()
            while self.llamadas and now - self.llamadas[0] > 3600:
                self.llamadas.popleft()
            if len(self.llamadas) >= self.max_por_hora:
                return None                      # tope propio por hora
            for _ in range(len(self.llaves)):
                k = self.llaves[self.idx]
                self.idx = (self.idx + 1) % len(self.llaves)
                if now >= self.libre_desde[k]:
                    self.llamadas.append(now)
                    return k
            return None                          # todas en cooldown

    def penalizar(self, k):
        self.libre_desde[k] = time.time() + self.cooldown

    def info(self):
        now = time.time()
        return {"llaves": len(self.llaves),
                "disponibles": sum(1 for v in self.libre_desde.values() if now >= v),
                "llamadas_hora": len(self.llamadas), "tope_hora": self.max_por_hora}


PROMPT = """Eres un filtro de riesgo para un bot de trading spot long-only en {symbol}.
Datos de microestructura en este instante (bps = puntos base):
{datos}
Un estimador local propone COMPRAR con movimiento esperado de {edge:.1f} bps
(costo total ida y vuelta {be:.1f} bps). Solo con estos numeros, decide si la senal
parece un artefacto (libro fantasma, spread anomalo, volatilidad extrema) o razonable.
Responde SOLO JSON: {{"accion":"permitir"|"vetar","confianza":0.0-1.0}}"""


async def call_gemini(client, key, model, sig):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    f = sig["f"]
    datos = json.dumps({k: round(f[k], 3) for k in
                        ("imb", "micro_bps", "spread_bps", "vol_1s_bps", "depth_usd")})
    body = {"contents": [{"parts": [{"text": PROMPT.format(
                symbol=sig["symbol"], datos=datos, edge=sig["edge"], be=sig["be"])}]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json",
                                 "maxOutputTokens": 512}}
    r = await client.post(url, headers={"x-goog-api-key": key}, json=body)
    if r.status_code == 429:
        raise RateLimit()
    r.raise_for_status()
    text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
    d = json.loads(text)
    if d.get("accion") not in ("permitir", "vetar"):
        raise ValueError("esquema invalido")
    return d["accion"], max(0.0, min(1.0, float(d.get("confianza", 0))))


async def estratega(engine, cola, rot, cfg):
    async with httpx.AsyncClient(timeout=cfg.gemini_ttl_s + 1) as client:
        while True:
            sig = await cola.get()
            sid = sig["id"]
            if time.time() - sig["ts"] > cfg.gemini_ttl_s:
                engine.on_llm_result(sid, "expirado", 0)
                continue
            k = await rot.siguiente()
            if k is None:
                engine.on_llm_result(sid, "sin_llave", 0)
                continue
            try:
                accion, conf = await asyncio.wait_for(
                    call_gemini(client, k, cfg.gemini_model, sig), timeout=cfg.gemini_ttl_s)
                engine.on_llm_result(sid, accion, conf)
            except RateLimit:
                rot.penalizar(k)
                engine.on_llm_result(sid, "rate_limit", 0)
            except Exception:
                engine.on_llm_result(sid, "error", 0)     # ante la duda, NO operar
