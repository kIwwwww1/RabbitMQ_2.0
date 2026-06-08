import pika
from loguru import logger

RMQ_HOST = "0.0.0.0"
RMQ_PORT = 5672

RMQ_USER = "guest"
RMQ_PASSWORD = "guest"

MQ_EXCHANGE = ""
MQ_ROUTING_KEY = "hello"

connections_params = pika.ConnectionParameters(
    host=RMQ_HOST,
    port=RMQ_PORT,
    credentials=pika.PlainCredentials(username=RMQ_USER, password=RMQ_PASSWORD),
)


def get_connection() -> pika.BlockingConnection:
    return pika.BlockingConnection(parameters=connections_params)


def send_log(message: str):
    logger.info(message)
