"""Filtros publicos de Binance + cliente Testnet SOLO para probar el cableado.
La Testnet NO sirve para medir rentabilidad (liquidez y fills ficticios)."""
import asyncio, hashlib, hmac, os, time, uuid
from urllib.parse import urlencode
import httpx

DEFAULT_FILTERS = {"step": 0.00001, "min_qty": 0.00001, "min_notional": 5.0,
                   "tick": 0.01, "source": "default"}


async def fetch_filters(symbol):
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get("https://api.binance.com/api/v3/exchangeInfo",
                            params={"symbol": symbol})
            r.raise_for_status()
        out = dict(DEFAULT_FILTERS)
        for f in r.json()["symbols"][0]["filters"]:
            t = f["filterType"]
            if t == "LOT_SIZE":
                out["step"], out["min_qty"] = float(f["stepSize"]), float(f["minQty"])
            elif t in ("NOTIONAL", "MIN_NOTIONAL"):
                out["min_notional"] = float(f["minNotional"])
            elif t == "PRICE_FILTER":
                out["tick"] = float(f["tickSize"])
        out["source"] = "binance"
        return out
    except Exception:
        return dict(DEFAULT_FILTERS)


class TestnetClient:
    BASE = "https://testnet.binance.vision"

    def __init__(self, key, secret):
        self.key, self.secret = key, secret

    def _signed(self, params):
        params = dict(params, timestamp=int(time.time() * 1000), recvWindow=5000)
        qs = urlencode(params)
        sig = hmac.new(self.secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        return f"{qs}&signature={sig}"

    async def clock_offset_ms(self):
        async with httpx.AsyncClient(timeout=5) as c:
            t0 = time.time() * 1000
            r = await c.get(f"{self.BASE}/api/v3/time")
            t1 = time.time() * 1000
        return r.json()["serverTime"] - (t0 + t1) / 2

    async def test_order(self, symbol, side, qty):
        """/order/test valida firma, filtros y formato SIN ejecutar la orden."""
        qs = self._signed({"symbol": symbol, "side": side, "type": "MARKET",
                           "quantity": qty, "newClientOrderId": uuid.uuid4().hex[:32]})
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.post(f"{self.BASE}/api/v3/order/test?{qs}",
                             headers={"X-MBX-APIKEY": self.key})
        return r.status_code, r.text


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    async def main():
        cli = TestnetClient(os.getenv("BINANCE_TESTNET_KEY", ""), os.getenv("BINANCE_TESTNET_SECRET", ""))
        print("offset de reloj (ms):", await cli.clock_offset_ms())
        print("filtros:", await fetch_filters(os.getenv("SYMBOL", "BTCUSDT")))
        print("order/test:", await cli.test_order(os.getenv("SYMBOL", "BTCUSDT"), "BUY", 0.0001))
    asyncio.run(main())
