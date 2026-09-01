import json
import os
from datetime import datetime
from sqlite3 import Connection
from uuid import uuid4

from shared.db import get_connection, init_db

DB_PATH = os.getenv("ORDER_DB_PATH", "data/orders.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    status TEXT NOT NULL,
    total_amount REAL NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS order_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);
"""

SKU_PRICES = {
    "WIDGET-1": 50.0,
    "WIDGET-2": 25.0,
}


def init_orders_db() -> Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = get_connection(DB_PATH)
    init_db(conn, SCHEMA)
    return conn


def compute_total_amount(items: list[dict]) -> float:
    total = 0.0
    for item in items:
        price = SKU_PRICES.get(item["sku"], 0.0)
        total += price * item["qty"]
    return total


def insert_order(
    conn: Connection,
    customer_id: str,
    items: list[dict],
    total_amount: float,
) -> str:
    order_id = str(uuid4())
    created_at = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO orders (order_id, customer_id, status, total_amount, created_at) VALUES (?, ?, ?, ?, ?)",
        (order_id, customer_id, "CREATED", total_amount, created_at),
    )
    for item in items:
        conn.execute(
            "INSERT INTO order_items (order_id, sku, qty) VALUES (?, ?, ?)",
            (order_id, item["sku"], item["qty"]),
        )
    conn.commit()
    return order_id


def get_order(conn: Connection, order_id: str) -> dict | None:
    row = conn.execute(
        "SELECT order_id, customer_id, status, total_amount, created_at FROM orders WHERE order_id = ?",
        (order_id,),
    ).fetchone()
    if row is None:
        return None

    items = conn.execute(
        "SELECT sku, qty FROM order_items WHERE order_id = ?",
        (order_id,),
    ).fetchall()

    events = conn.execute(
        "SELECT event_type, payload, created_at FROM order_events WHERE order_id = ? ORDER BY id",
        (order_id,),
    ).fetchall()

    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "status": row["status"],
        "total_amount": row["total_amount"],
        "created_at": row["created_at"],
        "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
        "events": [
            {
                "event_type": e["event_type"],
                "payload": json.loads(e["payload"]),
                "created_at": e["created_at"],
            }
            for e in events
        ],
    }


def update_order_status(conn: Connection, order_id: str, status: str) -> None:
    conn.execute(
        "UPDATE orders SET status = ? WHERE order_id = ?",
        (status, order_id),
    )
    conn.commit()


def append_order_event(
    conn: Connection,
    order_id: str,
    event_type: str,
    payload: dict,
) -> None:
    created_at = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO order_events (order_id, event_type, payload, created_at) VALUES (?, ?, ?, ?)",
        (order_id, event_type, json.dumps(payload), created_at),
    )
    conn.commit()
