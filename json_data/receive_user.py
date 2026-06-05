import json

import pika


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="user_registrations")

    def callback(ch, method, properties, body):
        # 1. Декодируем байты в строку JSON
        decoded_body = body.decode("utf-8")

        # 2. Превращаем JSON-строку обратно в Python-словарь
        user = json.loads(decoded_body)

        # 3. Теперь мы можем работать с объектом как обычно
        print("\n [x] Получено новое сообщение о регистрации:")
        print(f"    - ID: {user['id']}")
        print(f"    - Имя: {user['username']}")
        print(f"    - Почта: {user['email']}")
        print(f"    - Роли: {', '.join(user['roles'])}")

        # Здесь могла быть логика: send_welcome_email(user['email'])

    channel.basic_consume(
        queue="user_registrations", on_message_callback=callback, auto_ack=True
    )

    print(" [*] Ожидание данных пользователей. Нажмите CTRL+C для выхода", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Программа остановлена.")
