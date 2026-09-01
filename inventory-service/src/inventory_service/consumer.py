import logging
import threading
from sqlite3 import Connection

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel

from shared.db import already_processed, mark_processed
from shared.events import EventType, OrderEvent
from shared.kafka import get_kafka_config, make_consumer, parse_event, publish_event

from inventory_service.db import get_stock, reserve_stock

logger = logging.getLogger(__name__)

GROUP_ID = "inventory-service"


class InventoryRejectedPayload(BaseModel):
    reason: str
    customer_id: str
    items: list[dict]
    total_amount: float


class InventoryReservedPayload(BaseModel):
    customer_id: str
    items: list[dict]
    total_amount: float


class InventoryConsumer:
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
        logger.info("Inventory consumer started, group=%s", GROUP_ID)

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
        if event.event_type != EventType.ORDER_CREATED:
            return

        if already_processed(self._conn, event.event_id):
            return

        payload = event.payload
        items = payload.get("items", [])
        customer_id = payload.get("customer_id", "")
        total_amount = payload.get("total_amount", 0.0)

        for item in items:
            sku = item["sku"]
            qty = item["qty"]
            stock_qty = get_stock(self._conn, sku)
            if stock_qty is None or stock_qty < qty:
                rejected = InventoryRejectedPayload(
                    reason=f"insufficient stock for {sku}",
                    customer_id=customer_id,
                    items=items,
                    total_amount=total_amount,
                )
                out_event = OrderEvent.create(
                    event_type=EventType.INVENTORY_REJECTED,
                    order_id=event.order_id,
                    payload=rejected,
                )
                mark_processed(self._conn, event.event_id)
                publish_event(self._producer, out_event)
                logger.info("Rejected order %s: insufficient stock for %s", event.order_id, sku)
                return

        for item in items:
            reserve_stock(self._conn, item["sku"], item["qty"])

        reserved = InventoryReservedPayload(
            customer_id=customer_id,
            items=items,
            total_amount=total_amount,
        )
        out_event = OrderEvent.create(
            event_type=EventType.INVENTORY_RESERVED,
            order_id=event.order_id,
            payload=reserved,
        )
        mark_processed(self._conn, event.event_id)
        publish_event(self._producer, out_event)
        logger.info("Reserved inventory for order %s", event.order_id)
