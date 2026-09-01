import logging
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, HTTPException

from inventory_service.consumer import InventoryConsumer
from inventory_service.db import get_stock, init_inventory_db
from shared.kafka import make_producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn = init_inventory_db()
producer = make_producer()
consumer = InventoryConsumer(conn, producer)

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/stock/{sku}")
def fetch_stock(sku: str):
    quantity = get_stock(conn, sku)
    if quantity is None:
        raise HTTPException(status_code=404, detail="SKU not found")
    return {"sku": sku, "quantity": quantity}


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer.start()
    logger.info("inventory-service started")
    yield
    consumer.stop()
    logger.info("inventory-service stopped")


app = FastAPI(title="inventory-service", lifespan=lifespan)
app.include_router(router)
