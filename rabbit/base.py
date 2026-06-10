import pika
from pika.adapters.blocking_connection import BlockingChannel

from advanced.advanced_connection import connections_params

from .exc import RabbitException


class RabbitBase:
    def __init__(
        self, connections_params: pika.ConnectionParameters = connections_params
    ) -> None:
        self.connections_params = connections_params
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    def get_connection(self):
        return pika.BlockingConnection(self.connections_params)

    @property
    def channel(self):
        if self._channel is None:
            raise RabbitException(
                "Пожалейста используйте контекстный менеджер для Rabbit помощника"
            )
        return self._channel

    def __enter__(self):
        self._connection = self.get_connection()
        self._channel = self._connection.channel()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._channel.is_open:
            self._channel.close()

        if self._connection.is_open:
            self._connection.close()
