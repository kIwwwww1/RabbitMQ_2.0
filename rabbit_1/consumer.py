import random
import time
from typing import TYPE_CHECKING

from loguru import logger

from advanced.advanced_connection import MQ_ROUTING_KEY, send_log
from rabbit import RabbitBase

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties


def process_new_message(
    ch: "BlockingChannel",
    method: "Basic.Deliver",
    properties: "BasicProperties",
    body: bytes,
):
    start = time.time()
    # logger.warning(f"Данные {ch}")
    # logger.warning(f"Данные {method}")
    # logger.warning(f"Данные {properties}")
    # logger.info(f"Данные {body}")

    number = int(body[-2:])
    # is_odd = number % 2
    is_odd = 0

    time.sleep(1 + is_odd * 2)

    end = time.time() - start
    if random.random() > 0.7:
        logger.info(
            f"--- NACK! не обработано Сообщение обработано (no requeue) {body.decode()}"
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    else:
        logger.info(f"+++ Сообщение обработано {body.decode()} за {end:05f}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_message(channel: "BlockingChannel") -> None:
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(MQ_ROUTING_KEY)
    channel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,
        # auto_ack=True,
    )
    logger.info("Ждем сообщения")
    channel.start_consuming()


def main():
    with RabbitBase() as rabbit:
        consume_message(channel=rabbit.channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        send_log("Пока!")
