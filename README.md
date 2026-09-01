# Order Kafka POC

Python + FastAPI + SQLite + uv monorepo demonstrating event-driven order processing with Kafka (Redpanda).

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker

## Bootstrap

```bash
uv sync --all-packages
docker compose up -d
```

## Environment

All services read:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=orders.events
```

## Run services

Start each service in a separate terminal:

```bash
uv run --package order-api uvicorn order_api.main:app --reload --port 8000
uv run --package inventory-service uvicorn inventory_service.main:app --reload --port 8001
uv run --package payment-service uvicorn payment_service.main:app --reload --port 8002
uv run --package shipping-service uvicorn shipping_service.main:app --reload --port 8003
```

## Kafka UI

Redpanda Console: http://localhost:8080

## Happy path

```bash
# Create order
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "cust-1", "items": [{"sku": "WIDGET-1", "qty": 2}]}'

# Poll status (after a few seconds)
curl http://localhost:8000/orders/<order_id>
# Expect: CREATED → RESERVED → PAID → SHIPPED
```

## Failure paths

- Out of stock: order `WIDGET-2` → status `CANCELLED`
- Payment fail: `customer_id` ending in `fail` or `total_amount > 1000` → `CANCELLED`

## Tests

```bash
uv run pytest shared/tests
```
