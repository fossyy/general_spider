import hashlib
import json, os, sys, logging, requests
from collections import Counter

from datetime import datetime
from threading import Thread, Event
from kafka import KafkaProducer
from dotenv import load_dotenv
from urllib.parse import urlparse
from logging.handlers import RotatingFileHandler
from scrapy.utils.log import configure_logging
from pathlib import Path


class GeneralSenderPipeline:
    mapping = {
        "media_thumbnail": "",
        "friend_count": 0,
        "follower_count": 0,
        "sentiment": "",
        "timestamp": 0,
        "lang": "en",
        "emotion": "",
        "user_location": [],
        "user_geo_location": [],
        "ann_geo_location": [],
        "workspace": "",
        "ann_person": [],
        "ann_location": [],
        "ann_organization": [],
        "content_image": [],
        "list_category": [],
        "hashtags": [],
        "group_member_count": 0,
        "page_member_count": 0,
        "reach": 0,
        "source": "",
        "content_stat": {
            "diggCount": 0,
            "shareCount": 0,
            "commentCount": 0,
            "playCount": 0
        },
        "detail_feature": {
            "product_type": "",
            "origin_country": "",
            "quantity_left": "",
            "ships_to": "",
            "payment": "",
            "category": "",
            "currency_symbol": "",
            "currency_value": 0,
            "currency_description": "",
            "unit": "",
            "unit_value": 0,
            "unit_description": ""
        },
        "global": {
            "view_count": 0,
            "share_count": 0,
            "reply_count": 0,
            "reach": 0,
            "quote_count": 0,
            "like_count": 0,
            "following_count": 0,
            "follower_count": 0,
            "engagement": 0,
            "timestamp": 0,
            "id": "",
            "user_id": "",
            "media_url": [],
            "media_type": "",
            "username": "",
            "name": "",
            "content": ""
        },
        "parent": {
            "view_count": 0,
            "share_count": 0,
            "reply_count": 0,
            "reach": 0,
            "quote_count": 0,
            "like_count": 0,
            "following_count": 0,
            "follower_count": 0,
            "engagement": 0,
            "timestamp": 0,
            "id": "",
            "user_id": "",
            "media_url": [],
            "media_type": "",
            "username": "",
            "name": "",
            "content": ""
        },
        "id": "",
        "content": "",
        "link": "",
        "user_image": "",
        "username": "",
        "name": "",
        "user_id": "",
        "media_url": [],
        "content_type": "",
        "media_type": "",
        "crawler_timestamp": 0,
    }

    def open_spider(self, spider):
        load_dotenv()
        self.kafka_servers = getattr(spider, 'KAFKA_BOOTSTRAP_SERVERS', None)
        self.kafka_topic = getattr(spider, 'KAFKA_TOPIC', None)
        self.output_file = getattr(spider, 'output_file', None)
        self.output_dst = getattr(spider, 'output_dst', None)
        self.preview = getattr(spider, 'preview', None)
        self.base_url = getattr(spider, 'base_url', None)
        self.url_parse = urlparse(self.base_url).netloc if self.base_url is not None else 'default_output'
        self.job_id = getattr(spider, 'job_id', 'default_job_id')
        self.crawl_count = 0
        self.last_logged = datetime.now()

        if self.preview == "yes":
            self.dashAddr: str = os.environ.get('DASHBOARD_ADDRESS', None)
            if self.dashAddr is None:
                raise ValueError("Missing required environment variables for dashboard")
            self.preview_is_send = False
        else:
            self.stop_event = Event()
            self.thread = Thread(target=self._log_crawl_count_periodically, args=(spider,))
            self.thread.daemon = True
            self.thread.start()

        configure_logging(install_root_handler=False)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
        log_file = 'log.log'
        log_folder = Path(f"logs/{self.url_parse}/{self.job_id}")
        log_folder.mkdir(parents=True, exist_ok=True)

        rotating_file_log = RotatingFileHandler(log_folder / log_file, maxBytes=1024 * 1024 * 10, backupCount=10,
                                                encoding='utf-8')
        rotating_file_log.setLevel(logging.INFO)
        rotating_file_log.setFormatter(formatter)

        stdout_log = logging.StreamHandler(sys.stdout)
        stdout_log.setLevel(logging.INFO)
        stdout_log.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.addHandler(rotating_file_log)
        root_logger.addHandler(stdout_log)

        if self.output_dst == "kafka":
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8')
            )

            spider.logger.info(f"Kafka producer connected to: {self.kafka_servers}, topic: {self.kafka_topic}")

        if not self.output_file and self.preview != "yes":
            raise ValueError('output_file must be specified')

        self.first_item = True
        if self.preview != "yes":
            if os.path.exists(self.output_file) and os.path.getsize(self.output_file) > 0:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()

                if content.endswith(']'):
                    content = content[:-1]
                    if content.strip()[-1] == ',':
                        content = content[:-1]

                with open(self.output_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                    self.first_item = False
            else:
                with open(self.output_file, 'w', encoding='utf-8') as f:
                    f.write('[')

    def map_data_to_mapping(self, data, mapping_template):
        def detect_file_type(url):
            parsed_url = urlparse(url)
            path = parsed_url.path.lower()
            if path.endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg')):
                return 'image'
            elif path.endswith(('.mp4', '.mkv', '.webm', '.avi', '.mov', '.flv', '.wmv')):
                return 'video'
            else:
                return 'text'

        for key, value in data.items():
            if key in mapping_template:
                if isinstance(mapping_template[key], dict) and isinstance(value, dict):
                    self.map_data_to_mapping(value, mapping_template[key])
                elif key == "media_url" and isinstance(value, list):
                    file_types = [detect_file_type(url) for url in value]
                    most_common_type = Counter(file_types).most_common(1)[0][0] if file_types else "text"
                    mapping_template[key] = value
                    mapping_template["media_type"] = most_common_type
                else:
                    mapping_template[key] = value
            else:
                mapping_template[key] = value

        link = data.get("link")
        if link:
            unique_id_source = link
        else:
            unique_id_source = json.dumps(data, sort_keys=True)

        mapping_template["id"] = hashlib.md5(unique_id_source.encode('utf-8')).hexdigest()

        return mapping_template

    def process_item(self, item, spider):
        self.crawl_count += 1

        mapped_item = self.mapping
        mapped_item = self.map_data_to_mapping(item, mapped_item)
        mapped_item["crawler_timestamp"] = int(datetime.now().timestamp() * 1000)

        if self.preview is not None and self.preview == 'yes':
            requests.post(f"{self.dashAddr}/api/preview/{self.job_id}", headers={'Content-Type': 'application/json'},
                          data=json.dumps(dict(item)))
            self.preview_is_send = True
            os._exit(0)

        if self.output_dst == 'kafka':
            if self.kafka_servers is None or self.kafka_topic is None:
                raise ValueError('kafka servers and topic must be specified')

            try:
                self.producer.send(self.kafka_topic, value=dict(mapped_item))
                self.producer.flush()
                spider.logger.debug(f"Item sent to Kafka topic '{self.kafka_topic}': {mapped_item}")
            except Exception as e:
                spider.logger.error(f"Failed to send item to Kafka: {e}")

            with open(self.output_file, 'a', encoding='utf-8') as f:
                if not self.first_item:
                    f.write(',\n')
                else:
                    self.first_item = False

                line = json.dumps(item['link'], indent=None)
                f.write(line)

        elif self.output_dst == 'local':
            with open(self.output_file, 'a', encoding='utf-8') as f:
                if not self.first_item:
                    f.write(',\n')
                else:
                    self.first_item = False

                line = json.dumps(dict(mapped_item), indent=None)
                f.write(line)
        else:
            spider.logger.info(f"No output destination")

        return mapped_item

    def _log_crawl_count_periodically(self, spider):
        while not self.stop_event.is_set():
            now = datetime.now()
            if (now - self.last_logged).seconds >= 10:
                try:
                    spider.logger.info(f"[CrawlCount] | {self.crawl_count}")
                    spider.logger.info(f"[StatusCodeCounts] | {getattr(spider, 'status_codes', None)}")
                    self.last_logged = now
                except Exception as e:
                    spider.logger.info(f"Error logging crawl stats: {e}")
            self.stop_event.wait(5)

    def close_spider(self, spider):
        if self.preview == "yes":
            if self.preview_is_send == False:
                requests.post(f"{self.dashAddr}/api/preview/{self.job_id}",
                              headers={'Content-Type': 'application/json'},
                              data=json.dumps(dict({"error": "Request might need additional Cookies"})))
        else:
            with open(self.output_file, 'a', encoding='utf-8') as f:
                f.write(']')
            self.stop_event.set()
            self.thread.join()
        if self.output_dst == 'kafka':
            if self.producer:
                self.producer.close()
                spider.logger.info("Kafka producer closed")

