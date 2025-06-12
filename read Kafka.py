import json
from kafka import KafkaConsumer

bootstrap_servers_value = '172.17.0.13:9092'

# Конфигурация Consumer
consumer = KafkaConsumer(
    ('24_rakitski_clients'),
    bootstrap_servers=bootstrap_servers_value,
    auto_offset_reset='earliest',  # Начать чтение с самого начала
    group_id='my_group'  # Группа потребителей
)

count = 0
# Чтение сообщений из Kafka
for message in consumer:
    print(message.value.decode('utf-8'))
    count += 1
    print(count)
