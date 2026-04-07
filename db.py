"""
SQLite database for NSE Screener
Stores scan results, historical scores, and metadata.
"""

import sqlite3
import json
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone, timedelta

log = logging.getLogger("db")

DB_PATH = Path(__file__).parent / "cache" / "screener.db"
_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Get a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        DB_PATH.parent.mkdir(exist_ok=True)
        _local.conn = sqlite3.connect(str(DB_PATH), timeout=10)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA synchronous=NORMAL")
    return _local.conn


def init_db():
    """Create tables if they don't exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scan_results (
            symbol TEXT PRIMARY KEY,
            data JSON NOT NULL,
            score INTEGER DEFAULT 0,
            high_conviction INTEGER DEFAULT 0,
            sector TEXT DEFAULT '',
            scan_date TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scan_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS score_history (
            symbol TEXT NOT NULL,
            score INTEGER NOT NULL,
            price REAL NOT NULL,
            rsi REAL,
            scan_date TEXT NOT NULL,
            PRIMARY KEY (symbol, scan_date)
        );

        CREATE TABLE IF NOT EXISTS custom_stocks (
            symbol TEXT PRIMARY KEY,
            exchange TEXT DEFAULT 'NSE',
            added_at TEXT NOT NULL,
            note TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            trade_type TEXT DEFAULT 'BUY',
            quantity INTEGER NOT NULL DEFAULT 1,
            buy_price REAL NOT NULL,
            buy_date TEXT NOT NULL,
            sell_price REAL,
            sell_date TEXT,
            stop_loss REAL,
            target REAL,
            status TEXT DEFAULT 'OPEN',
            notes TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_pos_portfolio ON positions(portfolio_id);
        CREATE INDEX IF NOT EXISTS idx_pos_symbol ON positions(symbol);
        CREATE INDEX IF NOT EXISTS idx_pos_status ON positions(status);

        CREATE INDEX IF NOT EXISTS idx_score ON scan_results(score DESC);
        CREATE INDEX IF NOT EXISTS idx_hc ON scan_results(high_conviction);
        CREATE INDEX IF NOT EXISTS idx_sector ON scan_results(sector);
        CREATE INDEX IF NOT EXISTS idx_history_sym ON score_history(symbol);
    """)
    conn.commit()
    log.info("Database initialized: %s", DB_PATH)


# ─── Scan Results ───

def save_results(results: list[dict], meta: dict = None):
    """Save scan results to DB. Updates existing, inserts new."""
    conn = _get_conn()
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    scan_date = datetime.now(ist).strftime("%Y-%m-%d")

    for r in results:
        conn.execute("""
            INSERT INTO scan_results (symbol, data, score, high_conviction, sector, scan_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                data=excluded.data, score=excluded.score,
                high_conviction=excluded.high_conviction, sector=excluded.sector,
                scan_date=excluded.scan_date, updated_at=excluded.updated_at
        """, (
            r["symbol"],
            json.dumps(r),
            r.get("score", 0),
            1 if r.get("high_conviction") else 0,
            r.get("sector", ""),
            scan_date,
            now,
        ))

        # Also save to history
        conn.execute("""
            INSERT OR REPLACE INTO score_history (symbol, score, price, rsi, scan_date)
            VALUES (?, ?, ?, ?, ?)
        """, (
            r["symbol"],
            r.get("score", 0),
            r.get("price", 0),
            r.get("rsi", 0),
            scan_date,
        ))

    if meta:
        for k, v in meta.items():
            conn.execute("""
                INSERT INTO scan_meta (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """, (k, json.dumps(v) if not isinstance(v, str) else v, now))

    conn.commit()
    log.info("Saved %d results to DB", len(results))


def load_results(limit: int = 750) -> list[dict]:
    """Load scan results from DB, ordered by score."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT data FROM scan_results ORDER BY score DESC LIMIT ?",
        (limit,)
    ).fetchall()
    return [json.loads(row["data"]) for row in rows]


def get_result_count() -> int:
    conn = _get_conn()
    row = conn.execute("SELECT COUNT(*) as cnt FROM scan_results").fetchone()
    return row["cnt"] if row else 0


def get_meta(key: str, default=None):
    """Get a metadata value."""
    conn = _get_conn()
    row = conn.execute("SELECT value FROM scan_meta WHERE key=?", (key,)).fetchone()
    if row:
        try:
            return json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            return row["value"]
    return default


def set_meta(key: str, value):
    """Set a metadata value."""
    conn = _get_conn()
    ist = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    v = json.dumps(value) if not isinstance(value, str) else value
    conn.execute("""
        INSERT INTO scan_meta (key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (key, v, now))
    conn.commit()


def get_stock(symbol: str) -> dict | None:
    """Get a single stock's scan data."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT data FROM scan_results WHERE symbol=?", (symbol.upper(),)
    ).fetchone()
    return json.loads(row["data"]) if row else None


def get_score_history(symbol: str, days: int = 30) -> list[dict]:
    """Get score history for a stock."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT symbol, score, price, rsi, scan_date
        FROM score_history WHERE symbol=?
        ORDER BY scan_date DESC LIMIT ?
    """, (symbol.upper(), days)).fetchall()
    return [dict(r) for r in rows]


def get_all_symbols() -> list[str]:
    """Get all symbols in the DB."""
    conn = _get_conn()
    rows = conn.execute("SELECT symbol FROM scan_results ORDER BY score DESC").fetchall()
    return [row["symbol"] for row in rows]


def get_sector_stats() -> list[dict]:
    """Get sector-wise stats."""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT sector, COUNT(*) as count,
               AVG(score) as avg_score,
               SUM(high_conviction) as hc_count
        FROM scan_results
        GROUP BY sector
        ORDER BY avg_score DESC
    """).fetchall()
    return [dict(r) for r in rows]


def clear_old_results(days: int = 7):
    """Remove results older than N days."""
    conn = _get_conn()
    conn.execute("""
        DELETE FROM scan_results
        WHERE julianday('now') - julianday(updated_at) > ?
    """, (days,))
    conn.commit()


# ─── Custom Stocks ───

def add_custom_stock(symbol: str, exchange: str = "NSE", note: str = "") -> bool:
    conn = _get_conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn.execute("""
            INSERT INTO custom_stocks (symbol, exchange, added_at, note)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET note=excluded.note
        """, (symbol.upper(), exchange.upper(), now, note))
        conn.commit()
        return True
    except Exception:
        return False


def remove_custom_stock(symbol: str) -> bool:
    conn = _get_conn()
    cursor = conn.execute("DELETE FROM custom_stocks WHERE symbol=?", (symbol.upper(),))
    conn.commit()
    return cursor.rowcount > 0


def get_custom_stocks() -> list[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT symbol, exchange, added_at, note FROM custom_stocks ORDER BY added_at DESC").fetchall()
    return [dict(r) for r in rows]


def is_custom_stock(symbol: str) -> bool:
    conn = _get_conn()
    row = conn.execute("SELECT 1 FROM custom_stocks WHERE symbol=?", (symbol.upper(),)).fetchone()
    return row is not None


# ─── Portfolios ───

def create_portfolio(name: str, description: str = "") -> int:
    conn = _get_conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor = conn.execute(
        "INSERT INTO portfolios (name, description, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (name, description, now, now))
    conn.commit()
    return cursor.lastrowid


def get_portfolios() -> list[dict]:
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM portfolios ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


def get_portfolio(pid: int) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM portfolios WHERE id=?", (pid,)).fetchone()
    return dict(row) if row else None


def update_portfolio(pid: int, name: str = None, description: str = None):
    conn = _get_conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if name:
        conn.execute("UPDATE portfolios SET name=?, updated_at=? WHERE id=?", (name, now, pid))
    if description is not None:
        conn.execute("UPDATE portfolios SET description=?, updated_at=? WHERE id=?", (description, now, pid))
    conn.commit()


def delete_portfolio(pid: int):
    conn = _get_conn()
    conn.execute("DELETE FROM positions WHERE portfolio_id=?", (pid,))
    conn.execute("DELETE FROM portfolios WHERE id=?", (pid,))
    conn.commit()


# ─── Positions (Trades) ───

def add_position(portfolio_id: int, symbol: str, quantity: int, buy_price: float,
                 buy_date: str, stop_loss: float = None, target: float = None,
                 notes: str = "") -> int:
    conn = _get_conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor = conn.execute("""
        INSERT INTO positions (portfolio_id, symbol, quantity, buy_price, buy_date,
                               stop_loss, target, notes, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)
    """, (portfolio_id, symbol.upper(), quantity, buy_price, buy_date,
          stop_loss, target, notes, now, now))
    conn.commit()
    return cursor.lastrowid


def close_position(position_id: int, sell_price: float, sell_date: str = None):
    conn = _get_conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not sell_date:
        sell_date = datetime.now().strftime("%Y-%m-%d")
    conn.execute("""
        UPDATE positions SET sell_price=?, sell_date=?, status='CLOSED', updated_at=?
        WHERE id=?
    """, (sell_price, sell_date, now, position_id))
    conn.commit()


def update_position(position_id: int, **kwargs):
    conn = _get_conn()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    allowed = {"quantity", "buy_price", "buy_date", "sell_price", "sell_date",
               "stop_loss", "target", "notes", "status"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    updates["updated_at"] = now
    set_clause = ", ".join(f"{k}=?" for k in updates)
    conn.execute(f"UPDATE positions SET {set_clause} WHERE id=?",
                 list(updates.values()) + [position_id])
    conn.commit()


def delete_position(position_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM positions WHERE id=?", (position_id,))
    conn.commit()


def get_positions(portfolio_id: int, status: str = None) -> list[dict]:
    conn = _get_conn()
    if status:
        rows = conn.execute(
            "SELECT * FROM positions WHERE portfolio_id=? AND status=? ORDER BY buy_date DESC",
            (portfolio_id, status.upper())).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM positions WHERE portfolio_id=? ORDER BY status ASC, buy_date DESC",
            (portfolio_id,)).fetchall()
    return [dict(r) for r in rows]


def get_position(position_id: int) -> dict | None:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM positions WHERE id=?", (position_id,)).fetchone()
    return dict(row) if row else None


def get_portfolio_summary(portfolio_id: int) -> dict:
    conn = _get_conn()
    open_pos = conn.execute(
        "SELECT COUNT(*) as cnt, SUM(quantity * buy_price) as invested FROM positions WHERE portfolio_id=? AND status='OPEN'",
        (portfolio_id,)).fetchone()
    closed_pos = conn.execute("""
        SELECT COUNT(*) as cnt,
               SUM((sell_price - buy_price) * quantity) as realized_pnl,
               SUM(quantity * buy_price) as total_cost
        FROM positions WHERE portfolio_id=? AND status='CLOSED'
    """, (portfolio_id,)).fetchone()
    return {
        "open_count": open_pos["cnt"] or 0,
        "invested": round(open_pos["invested"] or 0, 2),
        "closed_count": closed_pos["cnt"] or 0,
        "realized_pnl": round(closed_pos["realized_pnl"] or 0, 2),
        "total_traded": round(closed_pos["total_cost"] or 0, 2),
    }


def db_stats() -> dict:
    """Get DB statistics."""
    conn = _get_conn()
    results = conn.execute("SELECT COUNT(*) as cnt FROM scan_results").fetchone()["cnt"]
    history = conn.execute("SELECT COUNT(*) as cnt FROM score_history").fetchone()["cnt"]
    meta = conn.execute("SELECT COUNT(*) as cnt FROM scan_meta").fetchone()["cnt"]
    size = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    return {
        "results": results,
        "history_records": history,
        "meta_entries": meta,
        "db_size_kb": round(size / 1024, 1),
    }
