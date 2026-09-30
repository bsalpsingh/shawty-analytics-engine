
import os
import json
import logging
from kafka import KafkaConsumer
from typing import Any, List
import time
from app.core.clickhouse import ClickHouseSingleton
log = logging.getLogger(__name__)


def _deserialize(v: bytes | None) -> Any | None:
    if v is None:  # tombstone message
        return None
    try:
        return json.loads(v.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        log.warning("skipping malformed message")
        return None


class SubscriberSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            bootstrap_servers = os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ).split(",")
            topic = "analytics"
            group_id = "shawty-analytics"

            self._consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=_deserialize
            )
            self._initialized = True
            self._buffer = []
            # time since s/m reboot  or some event(fwd moving) unlike date time
            self._last_flush = time.monotonic()

    def consume(self):

        try:
            while True:
                records = self._consumer.poll(
                    max_records=500, timeout_ms=1000)
                for msgs in records.values():
                    new = [m.value for m in msgs if m.value is not None]
                    if new and not self._buffer:
                        self._last_flush = time.monotonic()
                    self._buffer.extend(new)

                if self._buffer and (
                    len(self._buffer) >= 500
                    or time.monotonic() - self._last_flush > 5
                ):
                    self.process(self._buffer)
                    self._consumer.commit()
                    self._buffer.clear()
                    self._last_flush = time.monotonic()

        finally:
            self._consumer.close()

    def process(self, batch: List[Any]):
        ClickHouseSingleton().insert_clicks(batch)
