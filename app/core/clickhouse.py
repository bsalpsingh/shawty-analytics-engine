import os
import logging
from datetime import datetime, timezone
from typing import Any, List

import clickhouse_connect

log = logging.getLogger(__name__)

COLUMNS = ["url_id", "short_url", "long_url", "user_id",
           "ip_addr", "referrer", "clicked_at"]

DDL = """
CREATE TABLE IF NOT EXISTS {db}.url_clicks (
    url_id      UInt64,
    short_url   String,
    long_url    String,
    user_id     UInt64,
    ip_addr     String,
    referrer    LowCardinality(String),
    clicked_at  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(clicked_at)
ORDER BY (short_url, clicked_at)
"""


def _parse_ts(v: Any) -> datetime:
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


class ClickHouseSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._db = os.environ.get("CLICKHOUSE_DB", "analytics")
        self._client = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", "app"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "devpassword"),
            database=self._db,
        )
        self._client.command(DDL.format(db=self._db))
        self._initialized = True

    def insert_clicks(self, batch: List[dict]) -> None:
        rows = [
            [
                e.get("id", 0),
                e.get("shortURL", ""),
                e.get("url", ""),
                e.get("user_id", 0),
                e.get("ip_addr") or "unknown",
                e.get("ref") or "",
                _parse_ts(e.get("ts")),
            ]
            for e in batch
        ]
        self._client.insert("url_clicks", rows, column_names=COLUMNS)
        log.info("inserted %d rows into clickhouse", len(rows))