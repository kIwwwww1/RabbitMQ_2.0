import time
from typing import TYPE_CHECKING

from loguru import logger

from advanced.advanced_connection import (
    MQ_EXCHANGE,
    MQ_ROUTING_KEY,
    get_connection,
    send_log,
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel


def produce_message(channel: "BlockingChannel", idx: int) -> None:
    queue = channel.queue_declare(queue=MQ_ROUTING_KEY)
    logger.info(f"Обявление очереди {MQ_ROUTING_KEY}, {queue},{time.time()}")
    message = f"Hello World! {time.time()}, Новое сообщение #{idx:02d}"
    logger.info(f"Отправка {message}")
    channel.basic_publish(
        exchange=MQ_EXCHANGE,
        routing_key=MQ_ROUTING_KEY,
        body=message,
    )


def declare_queue(channel: "BlockingChannel") -> None:
    channel.queue_declare(queue=MQ_ROUTING_KEY)


def main():
    with get_connection() as connection:
        logger.info(f"Создали соединение {connection}")
        with connection.channel() as channel:
            logger.info(f"Создали канала {channel}")
            declare_queue(channel=channel)
            for idx in range(1, 11):
                produce_message(channel=channel, idx=idx)
                time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        send_log("Пока!")
