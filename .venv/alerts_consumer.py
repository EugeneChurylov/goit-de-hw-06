from kafka import KafkaConsumer
import json
from config import kafka_config

consumer = KafkaConsumer(
    kafka_config["output_topic"],
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Отримання алертів із Kafka:")

try:
    for message in consumer:
        alert = message.value
        print(f"Алерт отримано: {alert}")
except KeyboardInterrupt:
    print("\nЗавершення отримання алертів.")
finally:
    consumer.close()
