from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient
import json
import logging

logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer = None
        self.admin_client = None
    
    def get_producer(self):
        if not self.producer:
            self.producer = Producer({
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'trading-platform'
            })
        return self.producer
    
    def get_consumer(self, group_id, topics):
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(topics)
        return consumer
    
    def get_admin_client(self):
        if not self.admin_client:
            self.admin_client = AdminClient({
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'trading-platform-admin'
            })
        return self.admin_client
    
    def produce_message(self, topic, message, key=None):
        producer = self.get_producer()
        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(message).encode('utf-8')
        )
        producer.flush()
        logger.info(f"Message sent to {topic}: {message}")
