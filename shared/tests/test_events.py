from datetime import datetime

from shared.events import EventType, OrderCreatedPayload, OrderEvent, OrderItem


def test_order_event_round_trip():
    payload = OrderCreatedPayload(
        customer_id="cust-1",
        items=[OrderItem(sku="WIDGET-1", qty=2)],
        total_amount=100.0,
    )
    event = OrderEvent.create(
        event_type=EventType.ORDER_CREATED,
        order_id="order-123",
        payload=payload,
    )

    assert event.event_id
    assert event.timestamp
    assert event.event_type == EventType.ORDER_CREATED
    assert event.order_id == "order-123"
    assert event.payload["customer_id"] == "cust-1"
    assert event.payload["total_amount"] == 100.0

    restored = OrderEvent.model_validate_json(event.model_dump_json())
    assert restored.event_id == event.event_id
    assert restored.event_type == event.event_type
    assert restored.order_id == event.order_id
    assert restored.payload == event.payload
