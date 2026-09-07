import os

from confluent_kafka import Consumer, Message, Producer

from shared.events import OrderEvent


def get_kafka_config() -> dict[str, str]:
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic": os.getenv("KAFKA_TOPIC", "orders.events"),
    }


def make_producer() -> Producer:
    config = get_kafka_config()
    return Producer(
        {
            "bootstrap.servers": config["bootstrap_servers"],
        }
    )


def make_consumer(group_id: str) -> Consumer:
    config = get_kafka_config()
    return Consumer(
        {
            "bootstrap.servers": config["bootstrap_servers"],
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )


def publish_event(producer: Producer, event: OrderEvent) -> None:
    config = get_kafka_config()
    producer.produce(
        topic=config["topic"],
        key=event.order_id,
        value=event.model_dump_json(),
    )
    producer.flush()


def parse_event(msg: Message) -> OrderEvent:
    value = msg.value()
    if value is None:
        raise ValueError("Message value is empty")
    return OrderEvent.model_validate_json(value)
