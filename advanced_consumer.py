import time
from typing import TYPE_CHECKING

from loguru import logger

from advanced_connection import MQ_ROUTING_KEY, get_connection, send_log

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
    logger.warning(f"Данные {ch}")
    logger.warning(f"Данные {method}")
    logger.warning(f"Данные {properties}")
    # logger.info(f"Данные {body}")
    end = time.time() - start

    time.sleep(0.5)

    logger.info(f"Сообщение обработано {body.decode()} за {end:05f}")
    # ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_message(channel: "BlockingChannel") -> None:
    channel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,
        auto_ack=True,
    )
    logger.info("Ждем сообщения")
    channel.start_consuming()


def main():
    send_log("Запуск")
    with get_connection() as connection:
        logger.info(f"Создали соединение {connection}")
        with connection.channel() as channel:
            logger.info(f"Создали канала {channel}")
            consume_message(channel=channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        send_log("Пока!")
