import json, os

from scrapy.utils.project import get_project_settings
from kafka import KafkaProducer
from urllib.parse import urlparse

class GeneralSenderPipeline:
    def open_spider(self, spider):
        self.default_kafka_server = ''
        self.default_kafka_topic = ''
        self.settings = get_project_settings()
        kafka_servers = self.settings.get('KAFKA_BOOTSTRAP_SERVERS', self.default_kafka_server)
        self.kafka_topic = self.settings.get('KAFKA_TOPIC', self.default_kafka_topic)
        output_file = getattr(spider, 'output_file', None)
        self.output_destination_file = getattr(spider, 'output_destination_file', None)

        self.producer = KafkaProducer(
            bootstrap_servers = kafka_servers,
            value_serializer = lambda v: json.dumps(v).encode('utf-8'),
            key_serializer = lambda k: str(k).encode('utf-8')
        )
        
        spider.logger.info(f"Kafka producer connected to: {kafka_servers}, topic: {self.kafka_topic}")

        if not output_file:
            raise ValueError('output_file must be specified')

        self.output_file = output_file
        self.first_item = True

        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            with open(output_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()

            if content.endswith(']'):
                content = content[:-1]
                if content.strip()[-1] == ',':
                    content = content[:-1]

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(content)
                self.first_item = False
        else:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('[')

    def process_item(self, item, spider):
        if self.output_destination_file == 'kafka':
            try:
                self.producer.send(self.kafka_topic, value = dict(item))
                self.producer.flush()
                spider.logger.info(f"Item sent to Kafka topic '{self.kafka_topic}': {item}")
            except Exception as e:
                spider.logger.error(f"Failed to send item to Kafka: {e}")

            with open(self.output_file, 'a', encoding='utf-8') as f:
                if not self.first_item:
                    f.write(',\n')
                else:
                    self.first_item = False

                line = json.dumps(urlparse(item['url']).path, indent = None)
                f.write(line)  

        elif self.output_destination_file == 'local':    
            with open(self.output_file, 'a', encoding='utf-8') as f:
                if not self.first_item:
                    f.write(',\n')
                else:
                    self.first_item = False

                line = json.dumps(dict(item), indent = None)
                f.write(line)
        else:
             self.logger.info(f"No have output destination")   

        return item

    def close_spider(self, spider):
        with open(self.output_file, 'a', encoding='utf-8') as f:
            f.write(']')
            
        if self.producer:
            self.producer.close()
            spider.logger.info("Kafka producer closed")    
