import logging
import threading
from sqlite3 import Connection

from confluent_kafka import Consumer, Producer
from pydantic import BaseModel

from shared.db import already_processed, mark_processed
from shared.events import EventType, OrderEvent
from shared.kafka import get_kafka_config, make_consumer, parse_event, publish_event

from payment_service.db import insert_payment

logger = logging.getLogger(__name__)

GROUP_ID = "payment-service"


class PaymentSucceededPayload(BaseModel):
    payment_id: str
    amount: float
    customer_id: str


class PaymentFailedPayload(BaseModel):
    payment_id: str
    amount: float
    customer_id: str
    reason: str


class PaymentConsumer:
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
        logger.info("Payment consumer started, group=%s", GROUP_ID)

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
        if event.event_type != EventType.INVENTORY_RESERVED:
            return

        if already_processed(self._conn, event.event_id):
            return

        payload = event.payload
        customer_id = payload.get("customer_id", "")
        total_amount = payload.get("total_amount", 0.0)

        should_fail = total_amount > 1000 or customer_id.endswith("fail")

        if should_fail:
            reason = "amount exceeds limit" if total_amount > 1000 else "customer flagged for failure"
            payment_id = insert_payment(self._conn, event.order_id, total_amount, "FAILED")
            failed = PaymentFailedPayload(
                payment_id=payment_id,
                amount=total_amount,
                customer_id=customer_id,
                reason=reason,
            )
            out_event = OrderEvent.create(
                event_type=EventType.PAYMENT_FAILED,
                order_id=event.order_id,
                payload=failed,
            )
            mark_processed(self._conn, event.event_id)
            publish_event(self._producer, out_event)
            logger.info("Payment failed for order %s: %s", event.order_id, reason)
            return

        payment_id = insert_payment(self._conn, event.order_id, total_amount, "SUCCEEDED")
        succeeded = PaymentSucceededPayload(
            payment_id=payment_id,
            amount=total_amount,
            customer_id=customer_id,
        )
        out_event = OrderEvent.create(
            event_type=EventType.PAYMENT_SUCCEEDED,
            order_id=event.order_id,
            payload=succeeded,
        )
        mark_processed(self._conn, event.event_id)
        publish_event(self._producer, out_event)
        logger.info("Payment succeeded for order %s", event.order_id)
