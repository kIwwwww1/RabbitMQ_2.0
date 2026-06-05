import json

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.queue_declare(queue="user_registrations")

# 1. Данные нашего пользователя (объект/словарь)
user_data = {
    "id": 42,
    "username": "ivan_dev",
    "email": "ivan@example.com",
    "is_active": True,
    "roles": ["admin", "user"],
}

# 2. Превращаем Python-словарь в JSON-строку
json_message = json.dumps(user_data)

# 3. Отправляем сообщение, указывая тип контента application/json
channel.basic_publish(
    exchange="",
    routing_key="user_registrations",
    body=json_message,
    properties=pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2,  # Делает сообщение устойчивым (сохраняет на диск)
    ),
)

print(f" [x] Данные пользователя {user_data['username']} отправлены в очередь!")
connection.close()
