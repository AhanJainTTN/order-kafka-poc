import os
from sqlite3 import Connection

from shared.db import already_processed, get_connection, init_db, mark_processed

DB_PATH = os.getenv("SHIPPING_DB_PATH", "data/shipping.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY
);
"""


def init_shipping_db() -> Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = get_connection(DB_PATH)
    init_db(conn, SCHEMA)
    return conn


def is_processed(conn: Connection, event_id: str) -> bool:
    return already_processed(conn, event_id)


def record_processed(conn: Connection, event_id: str) -> None:
    mark_processed(conn, event_id)
