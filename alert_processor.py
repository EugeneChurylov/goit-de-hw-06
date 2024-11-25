import json
from datetime import datetime, timedelta
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from config import kafka_config

# Ініціалізація Kafka
consumer = KafkaConsumer(
    kafka_config["input_topic"],
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Зчитування умов алертів
# Зчитування умов алертів із CSV
alert_conditions = pd.read_csv("alerts_conditions.csv", skipinitialspace=True)

# Перевірка, чи всі необхідні стовпці є в файлі
required_columns = ["temperature_min", "temperature_max", "humidity_min", "humidity_max", "code", "message"]
if not all(col in alert_conditions.columns for col in required_columns):
    raise ValueError(f"Відсутні необхідні стовпці у файлі alerts_conditions.csv. Знайдені стовпці: {list(alert_conditions.columns)}")

print("Файл alerts_conditions.csv успішно завантажено!")

# Параметри Sliding Window
WINDOW_SIZE = 60  # 1 хвилина
SLIDING_INTERVAL = 30  # 30 секунд

# Збереження даних для Sliding Window
data_buffer = []

def calculate_average(data):
    """Обчислення середніх значень температури та вологості"""
    df = pd.DataFrame(data)
    avg_temp = df["temperature"].mean()
    avg_humidity = df["humidity"].mean()
    return avg_temp, avg_humidity

def check_alerts(avg_temp, avg_humidity, current_time):
    """Перевірка середніх значень за умовами алертів"""
    alerts = []
    for _, condition in alert_conditions.iterrows():
        if (
            (condition["temperature_min"] == -999 or avg_temp >= condition["temperature_min"]) and
            (condition["temperature_max"] == -999 or avg_temp <= condition["temperature_max"]) and
            (condition["humidity_min"] == -999 or avg_humidity >= condition["humidity_min"]) and
            (condition["humidity_max"] == -999 or avg_humidity <= condition["humidity_max"])
        ):
            alert = {
                "window": {
                    "start": (current_time - timedelta(seconds=WINDOW_SIZE)).isoformat(),
                    "end": current_time.isoformat()
                },
                "t_avg": avg_temp,
                "h_avg": avg_humidity,
                "code": condition["code"],
                "message": condition["message"],
                "timestamp": datetime.now().isoformat()
            }
            alerts.append(alert)
    return alerts


# Обробка потоку даних
for message in consumer:
    record = message.value
    record["timestamp"] = datetime.fromtimestamp(record["timestamp"])
    data_buffer.append(record)

    # Видалення старих записів із буфера
    current_time = record["timestamp"]
    data_buffer = [r for r in data_buffer if r["timestamp"] > current_time - timedelta(seconds=WINDOW_SIZE)]

    # Кожні 30 секунд обробка Sliding Window
    if len(data_buffer) > 0 and (current_time - data_buffer[0]["timestamp"]).seconds >= SLIDING_INTERVAL:
        avg_temp, avg_humidity = calculate_average(data_buffer)
        alerts = check_alerts(avg_temp, avg_humidity, current_time)
        for alert in alerts:
            producer.send(kafka_config["output_topic"], value=alert)
            print(f"Відправлено алерт: {alert}")
