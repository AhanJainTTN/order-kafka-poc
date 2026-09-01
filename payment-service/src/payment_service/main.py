import logging
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI

from payment_service.consumer import PaymentConsumer
from payment_service.db import init_payment_db
from shared.kafka import make_producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn = init_payment_db()
producer = make_producer()
consumer = PaymentConsumer(conn, producer)

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer.start()
    logger.info("payment-service started")
    yield
    consumer.stop()
    logger.info("payment-service stopped")


app = FastAPI(title="payment-service", lifespan=lifespan)
app.include_router(router)
