import time
import random
from kafka import KafkaProducer
import json
from config import kafka_config

sensor_id = random.randint(1000, 9999)  # Унікальний ID сенсора

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # сериалізація JSON
)

print(f"Початок роботи датчика ID: {sensor_id}")

try:
    while True:
        temperature = random.uniform(25, 45)
        humidity = random.uniform(15, 85)
        timestamp = time.time()

        message = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2)
        }

        producer.send(kafka_config["input_topic"], value=message)
        print(f"Дані відправлено: {message}")

        time.sleep(5)
except KeyboardInterrupt:
    print("\nЗавершення роботи датчика.")
finally:
    producer.close()
