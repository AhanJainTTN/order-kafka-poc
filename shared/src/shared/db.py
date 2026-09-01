import os
import sqlite3
from sqlite3 import Connection, Row


def get_connection(path: str) -> Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = Row
    return conn


def init_db(conn: Connection, sql_script: str) -> None:
    conn.executescript(sql_script)
    conn.commit()


def already_processed(conn: Connection, event_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM processed_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return row is not None


def mark_processed(conn: Connection, event_id: str) -> None:
    conn.execute(
        "INSERT INTO processed_events (event_id) VALUES (?)",
        (event_id,),
    )
    conn.commit()
