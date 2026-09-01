import os
from datetime import datetime
from sqlite3 import Connection
from uuid import uuid4

from shared.db import get_connection, init_db

DB_PATH = os.getenv("PAYMENT_DB_PATH", "data/payment.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    amount REAL NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);
"""


def init_payment_db() -> Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = get_connection(DB_PATH)
    init_db(conn, SCHEMA)
    return conn


def insert_payment(conn: Connection, order_id: str, amount: float, status: str) -> str:
    payment_id = str(uuid4())
    created_at = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO payments (payment_id, order_id, amount, status, created_at) VALUES (?, ?, ?, ?, ?)",
        (payment_id, order_id, amount, status, created_at),
    )
    conn.commit()
    return payment_id
