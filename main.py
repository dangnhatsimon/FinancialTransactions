import os
from confluent_kafka import Producer, KafkaError, Message
from confluent_kafka.admin import AdminClient, NewTopic
import json
from datetime import datetime
import logging
import random
import uuid
import time
from typing import List, Dict, Sequence, Optional, Any
from importlib.metadata import metadata
import threading

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)


def create_topic(
    topic_name: str,
    bootstrap_servers: List[str],
    num_partitions: int,
    replication_factor: int
) -> None:
    admin_client = AdminClient({"bootstrap.servers": ",".join(bootstrap_servers)})
    try:
        metadata = admin_client.list_topics()
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )

            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logging.info(f"Topic {topic_name} created.")
                except Exception as e:
                    logging.error(f"Failed to create topic {topic_name}.")
            else:
                logging.info(f"Topic {topic_name} already exists.")
    except Exception as e:
        logging.error(f"Error creating topic: {e}.")


def generate_transaction() -> Dict[str, Any]:
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 100)}",
        "amount": round(random.uniform(50000, 150000), 2),
        "transaction_time": int(time.time()),
        "merchant_id": random.choice(["merchant_1", "merchant_2", "merchant_3"]),
        "transaction_type": random.choice(["purchase", "refund"]),
        "location": f"location_{random.randint(1, 50)}",
        "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
        "is_international": random.choice(["True", "False"]),
        "currency": random.choice(["USD", "EUR", "GBP"]),
    }


def delivery_report(err: KafkaError, msg: Message):
    if err is not None:
        logging.info(f"Message delivery failed: {msg.key()}.")
    else:
        logging.info(f"Message {msg.key()} delivered.")


def produce_transactions(
    producer: Producer,
    topic_name: str,
    thread_id: int
) -> None:
    while True:
        transaction = generate_transaction()
        try:
            producer.produce(
                topic=topic_name,
                key=transaction["user_id"],
                value=json.dumps(transaction).encode("utf-8"),
                on_delivery=delivery_report
            )
            logging.info(f"Thread {thread_id} produced transaction: {transaction}.")
            producer.flush()
        except Exception as e:
            logging.error(f"Error sending transaction: {e}.")


def producer_data_in_parallel(
    num_threads: int,
    producer: Producer,
    topic_name: str,
):
    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transactions, args=(producer,topic_name, i), daemon=True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
    except Exception as e:
        logging.error(f"Unexpected errors: {e}.")


if __name__ == "__main__":
    KAFKA_BROKER = ["localhost:29092", "localhost:39092", "localhost:49092"]
    NUM_PARTITIONS = 5
    REPLICATION_FACTOR = 3
    TOPIC_NAME = "financial_transactions"
    producer_config = {
        "bootstrap.servers": ",".join(KAFKA_BROKER),
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 512000,
        "batch.num.messages": 1000,
        "linger.ms": 50,
        "acks": 1,
        "compression.type": "gzip"
    }

    create_topic(
        topic_name=TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )
    producer = Producer(producer_config)

    producer_data_in_parallel(
        num_threads=3,
        producer=producer,
        topic_name=TOPIC_NAME
    )
