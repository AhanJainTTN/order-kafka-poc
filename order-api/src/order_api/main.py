import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from shared.kafka import make_producer

from order_api.consumer import OrderConsumer
from order_api.db import init_orders_db
from order_api.routes import create_routes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

conn = init_orders_db()
producer = make_producer()
consumer = OrderConsumer(conn)


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer.start()
    logger.info("order-api started")
    yield
    consumer.stop()
    logger.info("order-api stopped")


app = FastAPI(title="order-api", lifespan=lifespan)
app.include_router(create_routes(conn, producer))
