from datetime import datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(StrEnum):
    ORDER_CREATED = "OrderCreated"
    INVENTORY_RESERVED = "InventoryReserved"
    INVENTORY_REJECTED = "InventoryRejected"
    PAYMENT_SUCCEEDED = "PaymentSucceeded"
    PAYMENT_FAILED = "PaymentFailed"
    ORDER_SHIPPED = "OrderShipped"
    ORDER_CANCELLED = "OrderCancelled"


class OrderItem(BaseModel):
    sku: str
    qty: int


class OrderCreatedPayload(BaseModel):
    customer_id: str
    items: list[OrderItem]
    total_amount: float


class OrderEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    order_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    payload: dict

    @classmethod
    def create(cls, event_type: EventType, order_id: str, payload: BaseModel) -> "OrderEvent":
        return cls(
            event_type=event_type,
            order_id=order_id,
            payload=payload.model_dump(),
        )
