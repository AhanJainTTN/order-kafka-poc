import logging
import threading
from sqlite3 import Connection

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel

from shared.events import EventType, OrderEvent
from shared.kafka import get_kafka_config, make_consumer, parse_event, publish_event

from shipping_service.db import is_processed, record_processed

logger = logging.getLogger(__name__)

GROUP_ID = "shipping-service"


class OrderShippedPayload(BaseModel):
    tracking_number: str


class ShippingConsumer:
    def __init__(self, conn: Connection, producer: Producer) -> None:
        self._conn = conn
        self._producer = producer
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
        logger.info("Shipping consumer started, group=%s", GROUP_ID)

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
        if event.event_type != EventType.PAYMENT_SUCCEEDED:
            return

        if is_processed(self._conn, event.event_id):
            return

        tracking_number = f"TRK-{event.order_id[:8]}"
        logger.info("Shipped order %s with tracking %s", event.order_id, tracking_number)

        shipped = OrderShippedPayload(tracking_number=tracking_number)
        out_event = OrderEvent.create(
            event_type=EventType.ORDER_SHIPPED,
            order_id=event.order_id,
            payload=shipped,
        )
        record_processed(self._conn, event.event_id)
        publish_event(self._producer, out_event)
