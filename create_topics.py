from kafka.admin import KafkaAdminClient, NewTopic
from config import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

topics = [
    "building_sensors_churylov_eugene_2",
    "alert_Kafka_topic_churylov_eugene_2"
]

new_topics = [
    NewTopic(name=topics[0], num_partitions=3, replication_factor=1),
    NewTopic(name=topics[1], num_partitions=1, replication_factor=1)
]

try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Топіки успішно створені!")
except Exception as e:
    print(f"Деякі топіки вже існують або виникла помилка: {e}")

print("Список топіків:")
[print(topic) for topic in admin_client.list_topics() if "churylov_eugene" in topic]

admin_client.close()