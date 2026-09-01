import os
from sqlite3 import Connection

from shared.db import get_connection, init_db

DB_PATH = os.getenv("INVENTORY_DB_PATH", "data/inventory.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (
    sku TEXT PRIMARY KEY,
    quantity INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);
"""

SEED_DATA = [
    ("WIDGET-1", 100),
    ("WIDGET-2", 0),
]


def init_inventory_db() -> Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = get_connection(DB_PATH)
    init_db(conn, SCHEMA)
    for sku, quantity in SEED_DATA:
        conn.execute(
            "INSERT OR IGNORE INTO stock (sku, quantity) VALUES (?, ?)",
            (sku, quantity),
        )
    conn.commit()
    return conn


def get_stock(conn: Connection, sku: str) -> int | None:
    row = conn.execute(
        "SELECT quantity FROM stock WHERE sku = ?",
        (sku,),
    ).fetchone()
    if row is None:
        return None
    return row["quantity"]


def reserve_stock(conn: Connection, sku: str, qty: int) -> bool:
    row = conn.execute(
        "SELECT quantity FROM stock WHERE sku = ?",
        (sku,),
    ).fetchone()
    if row is None or row["quantity"] < qty:
        return False
    conn.execute(
        "UPDATE stock SET quantity = quantity - ? WHERE sku = ?",
        (qty, sku),
    )
    conn.commit()
    return True
