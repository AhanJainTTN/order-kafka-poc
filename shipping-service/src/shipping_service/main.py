import logging
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from shipping_service.consumer import ShippingConsumer
from shipping_service.db import init_shipping_db
from shared.kafka import make_producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn = init_shipping_db()
producer = make_producer()
consumer = ShippingConsumer(conn, producer)

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer.start()
    logger.info("shipping-service started")
    yield
    consumer.stop()
    logger.info("shipping-service stopped")


app = FastAPI(title="shipping-service", lifespan=lifespan)
app.include_router(router)
