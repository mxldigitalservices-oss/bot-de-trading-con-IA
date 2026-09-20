import json, sqlite3, time

SCHEMA = """
CREATE TABLE IF NOT EXISTS books(ts INTEGER, bids TEXT, asks TEXT);
CREATE TABLE IF NOT EXISTS signals(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, symbol TEXT, mid REAL,
  imb REAL, micro_bps REAL, spread_bps REAL, vol_bps REAL, depth_usd REAL,
  edge_bps REAL, breakeven_bps REAL, viable INTEGER, decision TEXT, veredicto TEXT,
  ret_5s REAL, ret_30s REAL, ret_120s REAL);
CREATE TABLE IF NOT EXISTS trades(
  id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id INTEGER, ts_entry INTEGER, ts_exit INTEGER,
  entry_px REAL, exit_px REAL, qty REAL, cost REAL, proceeds REAL,
  pnl_usd REAL, pnl_bps REAL, reason TEXT, slip_entry_bps REAL);
"""


class DB:
    def __init__(self, path):
        self.c = sqlite3.connect(path, check_same_thread=False)
        self.c.row_factory = sqlite3.Row
        self.c.execute("PRAGMA journal_mode=WAL")
        self.c.executescript(SCHEMA)
        self._last = time.time()

    def flush(self, force=False):
        if force or time.time() - self._last > 2:
            self.c.commit()
            self._last = time.time()

    def insert_book(self, ts, bids, asks):
        self.c.execute("INSERT INTO books VALUES(?,?,?)",
                       (int(ts * 1000), json.dumps(bids), json.dumps(asks)))
        self.flush()

    def insert_signal(self, d):
        cols = ",".join(d)
        cur = self.c.execute(f"INSERT INTO signals({cols}) VALUES({','.join('?' * len(d))})",
                             list(d.values()))
        self.flush()
        return cur.lastrowid

    def update_signal(self, sid, **kw):
        sets = ",".join(f"{k}=?" for k in kw)
        self.c.execute(f"UPDATE signals SET {sets} WHERE id=?", [*kw.values(), sid])
        self.flush()

    def insert_trade(self, d):
        cols = ",".join(d)
        self.c.execute(f"INSERT INTO trades({cols}) VALUES({','.join('?' * len(d))})",
                       list(d.values()))
        self.flush(True)

    def rows(self, sql, args=()):
        return [dict(r) for r in self.c.execute(sql, args).fetchall()]
