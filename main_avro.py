import fastavro

from client_activity_generator import get_client_activity_info
from clients_generator import get_client_info
from logins_generator import get_logins_info
from payments_generator import get_payments_info
from transactions_generator import get_transactions_info
from fastavro import writer, parse_schema
from datetime import datetime
import numpy as np
import pandas as pd

from confluent_kafka import Producer
import io


client_activity_df = get_client_activity_info()
clients_df = get_client_info()
payments_df = get_payments_info()  # date_time
logins_df = get_logins_info() #date_time
transactions_df = get_transactions_info() #date_time 2


# Конвертация дат в timestamp (требование Avro)
client_activity_df['activity_date'] = client_activity_df['activity_date'].astype('int64') // 10 ** 6
logins_df['login_date'] = logins_df['login_date'].astype('int64') // 10 ** 6
payments_df['payment_date'] = payments_df['payment_date'].astype('int64') // 10 ** 6
transactions_df['transaction_date'] = transactions_df['transaction_date'].astype('int64') // 10 ** 6
transactions_df['record_saved_at'] = transactions_df['record_saved_at'].astype('int64') // 10 ** 6

# Преобразуем столбец clients_df['client_birthday'] в строковый формат
clients_df['client_birthday'] = clients_df['client_birthday'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)


# Схема Avro (соответствует структуре DataFrame)
schema_client_activity = {
    "type": "record",
    "name": "ClientActivity",
    "fields": [
        {"name": "client_id", "type": ["null", "double"], "default": None},
        {"name": "activity_date", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
         "default": None},
        {"name": "activity_type", "type": ["null", "string"], "default": None},
        {"name": "activity_location", "type": ["null", "string"], "default": None},
        {"name": "ip_address", "type": ["null", "string"], "default": None},
        {"name": "device", "type": ["null", "string"], "default": None}
    ]
}
schema_clients = {
    "type": "record",
    "name": "Clients",
    "fields": [
        {"name": "client_id", "type": ["null", "double"], "default": None},
        {"name": "client_first_name", "type": ["null", "string"], "default": None},
        {"name": "client_last_name", "type": ["null", "string"], "default": None},
        {"name": "client_email", "type": ["null", "string"], "default": None},
        {"name": "client_phone", "type": ["null", "string"], "default": None},
        {"name": "client_address", "type": ["null", "string"], "default": None},
        {"name": "client_birthday", "type": ["null", "string"], "default": None}
    ]
}

schema_logins = {
    "type": "record",
    "name": "Logins",
    "fields": [
        {"name": "client_id", "type": ["null", "double"], "default": None},
        {"name": "login_date", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
         "default": None},
        {"name": "ip_address", "type": ["null", "string"], "default": None},
        {"name": "location", "type": ["null", "string"], "default": None},
        {"name": "device", "type": ["null", "string"], "default": None}
    ]
}

schema_payments = {
    "type": "record",
    "name": "Payments",
    "fields": [
        {"name": "client_id", "type": ["null", "double"], "default": None},
        {"name": "payment_id", "type": ["null", "double"], "default": None},
        {"name": "payment_date", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
         "default": None},
        {"name": "currency", "type": ["null", "string"], "default": None},
        {"name": "amount", "type": ["null", "double"], "default": None},
        {"name": "payment_method", "type": ["null", "string"], "default": None}
    ]
}

schema_transactions = {
    "type": "record",
    "name": "Transactions",
    "fields": [
        {"name": "client_id", "type": ["null", "double"], "default": None},
        {"name": "transaction_id", "type": ["null", "double"], "default": None},
        {"name": "transaction_date", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
         "default": None},
        {"name": "transaction_type", "type": ["null", "string"], "default": None},
        {"name": "account_number", "type": ["null", "string"], "default": None},
        {"name": "currency", "type": ["null", "string"], "default": None},
        {"name": "amount", "type": ["null", "double"], "default": None},
        {"name": "record_saved_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}],
         "default": None},
    ]
}



# Конвертация DataFrame в список словарей (подходящий для Avro)
# Замена NaN на None (так как Avro понимает только None как null)
records_client_activity = client_activity_df.replace({np.nan: None}).to_dict("records")
records_clients = clients_df.replace({np.nan: None}).to_dict("records")
records_logins = logins_df.replace({np.nan: None}).to_dict("records")
records_payments = payments_df.replace({np.nan: None}).to_dict("records")
records_transactions = transactions_df.replace({np.nan: None}).to_dict("records")


# Сохранение в Avro-файл
with open("client_activity_data.avro", "wb") as avro_file:
    writer(avro_file, parse_schema(schema_client_activity), records_client_activity)
with open("clients_data.avro", "wb") as avro_file:
    writer(avro_file, parse_schema(schema_clients), records_clients)
with open("logins_data.avro", "wb") as avro_file:
    writer(avro_file, parse_schema(schema_logins), records_logins)
with open("payments_data.avro", "wb") as avro_file:
    writer(avro_file, parse_schema(schema_payments), records_payments)
with open("transactions_data.avro", "wb") as avro_file:
    writer(avro_file, parse_schema(schema_transactions), records_transactions)



# Настройки подключения к Kafka
conf = {
    'bootstrap.servers': '172.17.0.13:9092',
    'client.id': 'learning24-producer'
}
# Создаёт объект Kafka-производителя с указанной конфигурацией.
producer = Producer(conf)

for record in records_client_activity: # высылаем построчно тк размеры всего превышает лимит
    # Создаёт буфер в памяти (временное хранилище), куда будет записан Avro-файл.
    buffer = io.BytesIO()
    # Сериализует данные (records) в формате Avro с использованием заданной схемы (schema).
    fastavro.writer(buffer,parse_schema(schema_client_activity), [record]) # список из одного элемента Потому что fastavro.writer() ожидает список записей
    # Извлекает байтовое представление сериализованных данных из буфера. Эти байты можно теперь отправить в Kafka.
    client_activity_avro_data = buffer.getvalue()
    # Отправляет сообщение в Kafka:
    producer.produce('24_wave_rakitski_test', client_activity_avro_data)


# Гарантирует, что все сообщения, находящиеся в очереди, действительно отправлены.
producer.flush()

