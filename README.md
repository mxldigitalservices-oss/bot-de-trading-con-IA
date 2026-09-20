# MXL: bot de trading spot (simulación con libro real)

## Arranque
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python selftest.py          # prueba offline del pipeline (datos sinteticos)
python main.py              # backend + dashboard en http://127.0.0.1:8000
```
Deja correr 1-2 semanas SIN operar dinero real. Todo se guarda en `mxl.db` (SQLite).

## Estructura
| Archivo | Rol |
|---|---|
| `main.py` | Ingesta WebSocket (nivel 0), API REST, WS del dashboard, arranque de tareas |
| `engine.py` | Embudo, maquina de estados, broker de papel pesimista, kill switch |
| `features.py` | Desbalance, microprecio, volatilidad, estimador de edge (heuristica a calibrar) |
| `costs.py` | Breakeven, filtros LOT_SIZE/NOTIONAL, recorrer libro (slippage real) |
| `gemini.py` | Rotador round-robin + veto de Gemini con TTL y tope por hora |
| `exchange.py` | Filtros publicos + cliente Testnet (`/order/test`, solo cableado) |
| `db.py` | Tablas `books`, `signals` (la matriz), `trades` |
| `static/index.html` | Dashboard |

## Decisiones de diseno importantes
- **Las ordenes reales NO estan conectadas.** El motor opera solo en papel. Conectar ordenes reales es el paso 7 y solo tiene sentido si `/api/stats` muestra `viables_neto_30s_bps > 0` de forma sostenida y con muestra grande.
- **Un solo dueno del estado:** el motor es sincrono (sin `await`), asi que no hay carreras entre ingesta, LLM y salidas.
- **Polvo por stepSize:** con $10 en BTC, un paso de 0.00001 BTC vale ~$0.60. El motor lo modela (`dust_qty`).
- **Gemini solo veta.** Precio y cantidad los calcula el codigo local. Sin contexto de noticias, su valor es dudoso: mide `veredicto` contra los contrafactuales antes de confiar en el.
- **Rotar llaves:** revisa los terminos de Gemini. Llaves del mismo proyecto comparten cuota.
- `/api/kill` no tiene autenticacion: el servidor escucha solo en `127.0.0.1`.
- `edge_estimate_bps` es una heuristica inicial: calibra `EDGE_K` con las columnas `imb`, `micro_bps` y `ret_30s`.
