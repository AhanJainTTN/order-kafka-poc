from fastapi import APIRouter, HTTPException
from confluent_kafka import Producer

from shared.events import EventType, OrderCreatedPayload, OrderEvent
from shared.kafka import publish_event

from order_api.db import compute_total_amount, get_order, insert_order
from order_api.models import CreateOrderRequest, OrderResponse

router = APIRouter()


def create_routes(conn, producer: Producer) -> APIRouter:
    @router.get("/health")
    def health():
        return {"status": "ok"}

    @router.post("/orders", response_model=OrderResponse)
    def create_order(request: CreateOrderRequest):
        items = [item.model_dump() for item in request.items]
        total_amount = compute_total_amount(items)
        order_id = insert_order(conn, request.customer_id, items, total_amount)

        payload = OrderCreatedPayload(
            customer_id=request.customer_id,
            items=request.items,
            total_amount=total_amount,
        )
        event = OrderEvent.create(
            event_type=EventType.ORDER_CREATED,
            order_id=order_id,
            payload=payload,
        )
        publish_event(producer, event)

        order = get_order(conn, order_id)
        if order is None:
            raise HTTPException(status_code=500, detail="Failed to create order")
        return OrderResponse(**order)

    @router.get("/orders/{order_id}", response_model=OrderResponse)
    def fetch_order(order_id: str):
        order = get_order(conn, order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="Order not found")
        return OrderResponse(**order)

    return router
