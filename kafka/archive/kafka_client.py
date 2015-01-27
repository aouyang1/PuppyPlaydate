from kafka.client import KafkaClient

client = KafkaClient("localhost:9092")
client.reset_all_metadata()