import time
from typing import TYPE_CHECKING

from loguru import logger

from advanced.advanced_connection import (
    MQ_EMAIL_UPDATES_EXCHANGE_NAME,
    send_log,
)
from rabbit.common import EmailUpdatesRabbit

if TYPE_CHECKING:
    pass


class Producer(EmailUpdatesRabbit):
    def produce_message(self, idx: int) -> None:
        message = f"Hello World! {time.time()}, Новое сообщение #{idx:02d}"
        logger.info(f"Отправка {message}")
        self.channel.basic_publish(
            exchange=MQ_EMAIL_UPDATES_EXCHANGE_NAME,
            routing_key="",
            body=message,
        )


def main():
    with Producer() as producer:
        producer.declare_email_updates_exchange()
        for idx in range(1, 6):
            producer.produce_message(idx=idx)
            time.sleep(0.5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        send_log("Пока!")
