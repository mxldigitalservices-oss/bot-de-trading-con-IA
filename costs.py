"""Matematica de costos y filtros del exchange. Todo en bps (1 bp = 0.01%)."""
from decimal import Decimal


def breakeven_bps(fee_bps, spread_bps, slip_bps):
    # comision de entrada + salida + cruzar el spread (ida y vuelta) + slippage por lado
    return 2 * fee_bps + spread_bps + 2 * slip_bps


def viable(edge_bps, fee_bps, spread_bps, slip_bps, margin):
    be = breakeven_bps(fee_bps, spread_bps, slip_bps)
    return edge_bps > be * margin, be


def quantize(qty, step):
    """Redondea hacia abajo al multiplo de stepSize (LOT_SIZE)."""
    q, s = Decimal(str(qty)), Decimal(str(step))
    return float((q // s) * s)


def net_qty_after_fee(qty, fee_bps, step):
    """La comision de compra se cobra en el activo recibido: hay que descontarla."""
    return quantize(qty * (1 - fee_bps / 1e4), step)


def meets_filters(qty, price, flt):
    return qty >= flt["min_qty"] and qty * price >= flt["min_notional"]


def walk_buy(asks, qty):
    """Precio promedio real de comprar `qty` recorriendo el libro. None si no hay liquidez."""
    need, cost = qty, 0.0
    for p, q in asks:
        t = min(q, need)
        cost += t * p
        need -= t
        if need <= 1e-12:
            return cost / qty
    return None


def walk_sell(bids, qty):
    need, proceeds = qty, 0.0
    for p, q in bids:
        t = min(q, need)
        proceeds += t * p
        need -= t
        if need <= 1e-12:
            return proceeds / qty
    return None
