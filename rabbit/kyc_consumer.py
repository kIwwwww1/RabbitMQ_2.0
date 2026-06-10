import time
from typing import TYPE_CHECKING

from loguru import logger

from advanced.advanced_connection import MQ_EMAIL_NAME_KYC_EMAIL_UPDATES, send_log
from rabbit.common import EmailUpdatesRabbit

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
    is_odd = number % 2

    logger.info(f"[ ] Начали обрабатывать email пользваотеля {body.decode()}")

    time.sleep(2)

    end = time.time() - start

    logger.info(f"[X] email обработано {body.decode()} за {end:05f}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    with EmailUpdatesRabbit() as rabbit:
        rabbit.consume_messages(
            message_callback=process_new_message,
            queue_name=MQ_EMAIL_NAME_KYC_EMAIL_UPDATES,
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        send_log("Пока!")
