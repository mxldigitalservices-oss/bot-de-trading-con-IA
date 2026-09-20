"""Motor: embudo de filtros + maquina de estados + broker de papel PESIMISTA.
Todo es sincrono (sin await) => con asyncio de un solo hilo no hay condiciones de carrera:
un unico dueno del estado."""
import logging, time
from enum import Enum

from costs import (breakeven_bps, meets_filters, quantize,
                   viable, walk_buy, walk_sell)
from features import FeatureEngine, edge_estimate_bps

log = logging.getLogger("mxl")


class St(str, Enum):
    IDLE = "IDLE"
    AWAITING_LLM = "AWAITING_LLM"
    ENTRY_PENDING = "ENTRY_PENDING"
    IN_POSITION = "IN_POSITION"
    EXIT_PENDING = "EXIT_PENDING"


class Labeler:
    """Etiqueta contrafactual: retorno posterior de CADA senal, se ejecute o no."""
    HORIZONS = (5, 30, 120)

    def __init__(self, db):
        self.db, self.pend = db, {}

    def add(self, sid, ts, mid):
        self.pend[sid] = {"ts": ts, "mid": mid, "r": {}}

    def tick(self, now, mid):
        done = []
        for sid, p in self.pend.items():
            for h in self.HORIZONS:
                if h not in p["r"] and now >= p["ts"] + h:
                    p["r"][h] = (mid / p["mid"] - 1) * 1e4
            if len(p["r"]) == len(self.HORIZONS):
                done.append(sid)
        for sid in done:
            r = self.pend.pop(sid)["r"]
            self.db.update_signal(sid, ret_5s=r[5], ret_30s=r[30], ret_120s=r[120])


class Engine:
    def __init__(self, cfg, db, filters):
        self.cfg, self.db, self.flt = cfg, db, filters
        self.fe = FeatureEngine(cfg.horizon_s)
        self.labeler = Labeler(db)
        self.state = St.IDLE
        self.cash = cfg.capital_usd
        self.pos = None
        self.pending = None
        self.halted, self.halt_reason = False, ""
        self.errors = 0
        self.dust = 0.0                  # restos por stepSize (no vendibles)
        self.daily_pnl, self.day = 0.0, ""
        self.last_logged = self.last_trigger = 0.0
        self.llm_queue = None            # lo asigna main.py si Gemini esta activo
        self.llm_sid, self.llm_deadline, self.cand = None, 0.0, None
        self.last_f, self.last_edge, self.last_be = None, 0.0, 0.0
        self.health = {"last_msg": 0.0, "last_error": ""}
        self.extra_status = lambda: {}
        self.stats = {"ticks": 0, "signals": 0, "entries": 0, "trades": 0, "vetos": 0}

    # ---------- entrada de datos ----------
    def on_book(self, now, bids, asks, changed=True):
        self.stats["ticks"] += 1
        f = self.fe.update(bids, asks, now)
        if f is None:
            return
        self.last_f = f
        self.last_edge = edge_estimate_bps(f, self.cfg.edge_k)
        self.last_be = breakeven_bps(self.cfg.fee_bps, f["spread_bps"], self.cfg.slip_bps)
        self.labeler.tick(now, f["mid"])
        self._roll_day(now)

        if self.pending and now >= self.pending["due"]:
            self._execute_pending(now, bids, asks)
        elif self.state == St.IN_POSITION:
            self._manage(now, bids, asks, f)
        elif self.state == St.AWAITING_LLM and now > self.llm_deadline:
            self.state = St.IDLE
        elif self.state == St.IDLE and changed:
            self._maybe_signal(now, f)

    # ---------- embudo ----------
    def _maybe_signal(self, now, f):
        c = self.cfg
        if f["spread_bps"] > c.max_spread_bps or f["depth_usd"] < c.min_depth_usd:
            return                                            # nivel 1
        if f["imb"] < 0.3 or now - self.last_logged < c.log_cooldown_s:
            return                                            # candidata?
        self.last_logged = now
        edge = self.last_edge
        ok, be = viable(edge, c.fee_bps, f["spread_bps"], c.slip_bps, c.safety_margin)
        reason = None
        if not ok and not c.demo_mode:
            reason = "edge<breakeven"                         # nivel 3
        elif self.halted:
            reason = "kill_switch"
        elif now - self.last_trigger < c.cooldown_s:
            reason = "cooldown"
        sid = self.db.insert_signal({
            "ts": int(now * 1000), "symbol": c.symbol, "mid": f["mid"], "imb": f["imb"],
            "micro_bps": f["micro_bps"], "spread_bps": f["spread_bps"],
            "vol_bps": f["vol_bps_h"], "depth_usd": f["depth_usd"], "edge_bps": edge,
            "breakeven_bps": be, "viable": int(ok), "decision": reason or "candidata"})
        self.labeler.add(sid, now, f["mid"])
        self.stats["signals"] += 1
        if reason:
            return
        self.last_trigger = now
        self.cand = {"id": sid, "edge": edge, "be": be, "f": f}
        if self.llm_queue is not None:                        # nivel 4 (opcional)
            self.state = St.AWAITING_LLM
            self.llm_sid, self.llm_deadline = sid, now + c.gemini_ttl_s + 1
            self.llm_queue.put_nowait({"id": sid, "ts": now, "symbol": c.symbol,
                                       "f": f, "edge": edge, "be": be})
        else:
            self._enter(now)

    def on_llm_result(self, sid, verdict, conf):
        if self.state != St.AWAITING_LLM or sid != self.llm_sid:
            return                                            # respuesta tardia: se ignora
        self.db.update_signal(sid, veredicto=f"{verdict}:{conf:.2f}")
        if verdict == "permitir" and conf >= self.cfg.gemini_min_conf:
            self._enter(time.time())
        else:
            self.stats["vetos"] += 1
            self.db.update_signal(sid, decision=f"veto_llm:{verdict}")
            self.state = St.IDLE

    # ---------- broker de papel ----------
    def _enter(self, now):
        c, cand = self.cfg, self.cand
        ask = cand["f"]["ask"]
        qty = quantize(self.cash * 0.995 / ask, self.flt["step"])
        if not meets_filters(qty, ask, self.flt):
            self.db.update_signal(cand["id"], decision="filtros_exchange")
            self.state = St.IDLE
            return
        self.pending = {"kind": "buy", "due": now + c.latency_ms / 1000, "qty": qty,
                        "ref_px": ask, "sid": cand["id"], "edge": cand["edge"], "be": cand["be"]}
        self.state = St.ENTRY_PENDING

    def _execute_pending(self, now, bids, asks):
        p, c = self.pending, self.cfg
        if p["kind"] == "buy":
            px = walk_buy(asks, p["qty"])
            if px is None:
                self.pending, self.state = None, St.IDLE
                return self._error("sin liquidez en entrada")
            qty_net = p["qty"] * (1 - c.fee_bps / 1e4)      # comision cobrada en el activo; exacto, se redondea al vender
            cost = px * p["qty"]
            self.cash -= cost
            tp = max(p["edge"], p["be"] * 1.1)
            self.pos = {"sid": p["sid"], "entry_px": px, "qty": qty_net, "cost": cost,
                        "ts": now, "deadline": now + c.time_stop_s, "extended": False,
                        "tp_bps": tp, "sl_bps": min(tp, c.risk_pct * 1e4), "move_bps": 0.0,
                        "slip_bps": (px / p["ref_px"] - 1) * 1e4}
            self.db.update_signal(p["sid"], decision="ejecutada")
            self.stats["entries"] += 1
            self.pending, self.state = None, St.IN_POSITION
        else:
            total = self.pos["qty"] + self.dust
            qty = quantize(total, self.flt["step"])          # solo se vende en multiplos de stepSize
            px = walk_sell(bids, qty) if qty >= self.flt["min_qty"] else None
            if px is None:
                p["due"] = now + 0.5                           # reintenta
                return self._error("sin liquidez / cantidad bajo el minimo en salida")
            new_dust = total - qty
            proceeds = px * qty * (1 - c.fee_bps / 1e4)
            # PnL a mercado: el polvo se valora al precio de salida pero queda en la cartera
            pnl = proceeds + px * (new_dust - self.dust) - self.pos["cost"]
            self.cash += proceeds
            self.dust = new_dust
            self.db.insert_trade({
                "signal_id": self.pos["sid"], "ts_entry": int(self.pos["ts"] * 1000),
                "ts_exit": int(now * 1000), "entry_px": self.pos["entry_px"], "exit_px": px,
                "qty": qty, "cost": self.pos["cost"], "proceeds": proceeds, "pnl_usd": pnl,
                "pnl_bps": pnl / self.pos["cost"] * 1e4, "reason": p["reason"],
                "slip_entry_bps": self.pos["slip_bps"]})
            self.stats["trades"] += 1
            self.daily_pnl += pnl
            self.errors = 0
            self.pos, self.pending, self.state = None, None, St.IDLE
            if self.daily_pnl <= -c.max_daily_loss_usd:
                self.kill("perdida_diaria")

    def _manage(self, now, bids, asks, f):
        pos, c = self.pos, self.cfg
        pos["move_bps"] = move = (bids[0][0] / pos["entry_px"] - 1) * 1e4
        if self.halted:
            return self._exit(now, "kill_switch")
        if move >= pos["tp_bps"]:
            return self._exit(now, "TP")
        if move <= -pos["sl_bps"]:
            return self._exit(now, "SL")
        if now >= pos["deadline"]:
            if pos["extended"]:
                return self._exit(now, "timestop")
            # REEVALUAR: prolongar solo si el edge restante supera el costo de salir
            cost_exit = c.fee_bps + f["spread_bps"]
            if edge_estimate_bps(f, c.edge_k) < cost_exit * 1.2:
                return self._exit(now, "reevaluar_salir")
            pos["extended"], pos["deadline"] = True, now + c.time_stop_s   # una vez; SL no se amplia

    def _exit(self, now, reason):
        self.pending = {"kind": "sell", "due": now + self.cfg.latency_ms / 1000, "reason": reason}
        self.state = St.EXIT_PENDING

    # ---------- control ----------
    def _error(self, msg):
        self.errors += 1
        log.warning("error: %s (%d)", msg, self.errors)
        if self.errors >= self.cfg.max_consecutive_errors:
            self.kill("errores_consecutivos")

    def kill(self, reason):
        self.halted, self.halt_reason = True, reason

    def resume(self):
        self.halted, self.halt_reason, self.errors = False, "", 0

    def _roll_day(self, now):
        d = time.strftime("%Y-%m-%d", time.gmtime(now))
        if d != self.day:
            self.day, self.daily_pnl = d, 0.0
            if self.halt_reason == "perdida_diaria":
                self.resume()

    # ---------- estado para la API ----------
    def status(self):
        now, f = time.time(), self.last_f
        bid = f["bid"] if f else 0
        pos = None
        if self.pos:
            p = self.pos
            pos = {"entry_px": p["entry_px"], "qty": p["qty"], "move_bps": p["move_bps"],
                   "tp_bps": p["tp_bps"], "sl_bps": p["sl_bps"], "age_s": now - p["ts"],
                   "extended": p["extended"]}
        age = now - self.health["last_msg"] if self.health["last_msg"] else None
        return {
            "ts": now, "symbol": self.cfg.symbol, "state": self.state.value,
            "demo": self.cfg.demo_mode, "halted": self.halted, "halt_reason": self.halt_reason,
            "feed_age_s": age, "feed_ok": age is not None and age < 5,
            "last_error": self.health["last_error"],
            "cash": self.cash, "equity": self.cash + ((self.pos["qty"] if self.pos else 0) + self.dust) * bid,
            "dust_qty": self.dust,
            "capital": self.cfg.capital_usd, "daily_pnl": self.daily_pnl,
            "mids": list(self.fe.mids)[-300:], "features": f,
            "edge_bps": self.last_edge, "breakeven_bps": self.last_be,
            "required_bps": self.last_be * self.cfg.safety_margin,
            "position": pos, "stats": self.stats, "errors": self.errors,
            "filters": self.flt, "gemini": self.extra_status(),
            "config": {"fee_bps": self.cfg.fee_bps, "margin": self.cfg.safety_margin,
                       "latency_ms": self.cfg.latency_ms},
        }
