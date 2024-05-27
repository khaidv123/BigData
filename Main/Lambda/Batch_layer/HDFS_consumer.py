
import ast
from kafka import KafkaConsumer
import json
from put_data_hdfs import store_data_in_hdfs

def consum_hdfs():
    bootstrap_servers = "127.0.0.1:9092" # 'localhost:9092'
    topic = 'smartphoneTopic'

    consumer = KafkaConsumer(topic,
                             group_id='my_consumer_group',
                             auto_offset_reset='latest',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))

    for message in consumer:
        try:
            data = message.value
            print(f"Received message from Kafka: {data}")
            data = ast.literal_eval(data)
            store_data_in_hdfs(data)
            print(f"Consumed and stored: {data}")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error decoding JSON: {e}")
            continue
        except Exception as e:
            print(f"Error storing data to HDFS: {e}")
            continue




