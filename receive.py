import os
import sys

import pika


def main():
    # 1. Подключаемся к серверу
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    # 2. Объявляем очередь (на случай, если получатель запустился раньше отправителя)
    channel.queue_declare(queue="hello")

    # 3. Функция обратного вызова (callback), которая срабатывает при получении сообщения
    def callback(ch, method, properties, body):
        print(f" [x] Получено: {body.decode('utf-8')}")

    # 4. Указываем RabbitMQ, из какой очереди брать сообщения и какую функцию вызывать
    channel.basic_consume(
        queue="hello",
        on_message_callback=callback,
        auto_ack=True,  # Автоматическое подтверждение прочтения
    )

    print(" [*] Ожидание сообщений. Для выхода нажмите CTRL+C")

    # 5. Запускаем бесконечный цикл ожидания
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Программа остановлена.")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
