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
    logger.info(f"Данные {ch}")
    logger.info(f"Данные {method}")
    logger.info(f"Данные {properties}")
    logger.info(f"Данные {body}")

    logger.info(f"Сообщение обработано {body}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_message(channel: "BlockingChannel") -> None:
    channel.basic_consume(queue=MQ_ROUTING_KEY, on_message_callback=process_new_message)
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
