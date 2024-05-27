from kafka import KafkaProducer

bootstrap_servers = "127.0.0.1:9092" #'localhost:9092'
topic = 'smartphoneTopic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_message(message):
    try:
        producer.send(topic,str(message).encode('utf-8'))
        print(f"Produced: {message} to Kafka topic: {topic}")
    except Exception as error:
        print(f"Error: {error}")


