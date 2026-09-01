import logging
import threading
from sqlite3 import Connection

from confluent_kafka import Consumer, Producer

from shared.db import already_processed, mark_processed
from shared.events import EventType, OrderEvent
from shared.kafka import get_kafka_config, make_consumer, parse_event, publish_event

from order_api.db import append_order_event, update_order_status

logger = logging.getLogger(__name__)

STATUS_MAP = {
    EventType.ORDER_CREATED: "CREATED",
    EventType.INVENTORY_RESERVED: "RESERVED",
    EventType.INVENTORY_REJECTED: "CANCELLED",
    EventType.PAYMENT_SUCCEEDED: "PAID",
    EventType.PAYMENT_FAILED: "CANCELLED",
    EventType.ORDER_SHIPPED: "SHIPPED",
    EventType.ORDER_CANCELLED: "CANCELLED",
}

GROUP_ID = "order-api"


class OrderConsumer:
    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._consumer: Consumer | None = None
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._consumer is not None:
            self._consumer.close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        config = get_kafka_config()
        self._consumer = make_consumer(GROUP_ID)
        self._consumer.subscribe([config["topic"]])
        logger.info("Order consumer started, group=%s", GROUP_ID)

        while self._running:
            msg = self._consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            try:
                event = parse_event(msg)
                self._handle_event(event)
            except Exception:
                logger.exception("Failed to handle event")

    def _handle_event(self, event: OrderEvent) -> None:
        if already_processed(self._conn, event.event_id):
            return

        status = STATUS_MAP.get(event.event_type)
        if status is not None:
            update_order_status(self._conn, event.order_id, status)

        append_order_event(
            self._conn,
            event.order_id,
            event.event_type,
            event.payload,
        )
        mark_processed(self._conn, event.event_id)
        logger.info("Processed %s for order %s", event.event_type, event.order_id)
