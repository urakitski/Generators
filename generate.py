import json
from kafka import KafkaProducer

from client_activity_generator import get_client_activity_info
from clients_generator import get_client_info
from logins_generator import get_logins_info
from payments_generator import get_payments_info
from transactions_generator import get_transactions_info
import pandas as pd

# Настройки подключения к Kafka
bootstrap_servers_value = '172.17.0.13:9092'


def send_jsons_to_kafka(data_frame, posfix):
    # Создаёт объект Kafka-производителя с указанной конфигурацией.
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers_value,
                             value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
                             )
    for _, row in data_frame.iterrows():
        message = row.where(pd.notna(row), None).to_dict()  # Заменяем NaN на None
        producer.send(f'24_rakitski_{posfix}', message)
    producer.flush()


clients_df = get_client_info()

client_activity_df = get_client_activity_info()
payments_df = get_payments_info()  # date_time
logins_df = get_logins_info()  # date_time
transactions_df = get_transactions_info()  # date_time 2

# преобразуем даты к string
clients_df['client_birthday'] = pd.to_datetime(clients_df['client_birthday']).astype(str)
client_activity_df['activity_date'] = pd.to_datetime(client_activity_df['activity_date']).astype(str)
payments_df['payment_date'] = pd.to_datetime(payments_df['payment_date']).astype(str)
logins_df['login_date'] = pd.to_datetime(logins_df['login_date']).astype(str)
transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date']).astype(str)
transactions_df['record_saved_at'] = pd.to_datetime(transactions_df['record_saved_at']).astype(str)

# print(transactions_df.info())
# json_client_activity = client_activity_df.to_json('client_activity.json',orient='records', lines = True)


#
send_jsons_to_kafka(payments_df, 'payments')
send_jsons_to_kafka(client_activity_df, 'client_activity')
send_jsons_to_kafka(clients_df, 'clients')
send_jsons_to_kafka(logins_df, 'logins')
send_jsons_to_kafka(transactions_df, 'transactions')

print("Все строки отправлены в Kafka.")
