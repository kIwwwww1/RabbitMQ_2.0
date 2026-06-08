import time
from typing import TYPE_CHECKING

from loguru import logger

from advanced_connection import MQ_EXCHANGE, MQ_ROUTING_KEY, get_connection, send_log

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel


def produce_message(channel: "BlockingChannel") -> None:
    queue = channel.queue_declare(queue=MQ_ROUTING_KEY)
    logger.info(f"Обявление очереди {MQ_ROUTING_KEY}, {queue},{time.time()}")
    message = f"Hello World! {time.time()}"
    logger.info(f"Отправка {message}")
    channel.basic_publish(
        exchange=MQ_EXCHANGE,
        routing_key=MQ_ROUTING_KEY,
        body=message,
    )


def main():
    send_log("Запуск")
    with get_connection() as connection:
        logger.info(f"Создали соединение {connection}")
        with connection.channel() as channel:
            logger.info(f"Создали канала {channel}")
            produce_message(channel=channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        send_log("Пока!")
