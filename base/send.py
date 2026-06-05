import pika

# 1. Устанавливаем соединение с сервером RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

# 2. Создаем очередь с именем 'hello' (если она уже есть, ничего не произойдет)
channel.queue_declare(queue="hello")

# 3. Отправляем сообщение
# Мы используем дефолтный Exchange (пустая строка ''), который шлет напрямую в очередь с тем же именем

channel.basic_publish(
    exchange="",
    routing_key="hello",  # Имя очереди
    body="Привет, RabbitMQ!",  # Текст сообщения
)

print(" [x] Сообщение отправлено: 'Привет, RabbitMQ!'")

# 4. Закрываем соединение
connection.close()
