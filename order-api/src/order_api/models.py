from pydantic import BaseModel

from shared.events import OrderItem


class CreateOrderRequest(BaseModel):
    customer_id: str
    items: list[OrderItem]


class OrderItemResponse(BaseModel):
    sku: str
    qty: int


class OrderEventResponse(BaseModel):
    event_type: str
    payload: dict
    created_at: str


class OrderResponse(BaseModel):
    order_id: str
    customer_id: str
    status: str
    total_amount: float
    created_at: str
    items: list[OrderItemResponse]
    events: list[OrderEventResponse]
