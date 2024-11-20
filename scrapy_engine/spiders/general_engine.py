import json, psycopg2, os

from typing import Any
from scrapy import Request, Spider
from scrapy.http import Response
from twisted.web.http import urlparse
from psycopg2._psycopg import connection, cursor as cursortype

class GeneralEngineSpider(Spider):
    name = "general_engine"

    current_proxy = 0
    proxies = [
        "http://localhost:8118",
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0',
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        'Accept-Language': 'en-US,en;q=0.5',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1'
    }

    def get_proxy(self):
        current_proxy_now = self.current_proxy % len(self.proxies)
        self.current_proxy += 1
        return self.proxies[current_proxy_now]

    def __init__(self, config_id = None, output_dst = "local", kafka_server = None, kafka_topic = None, *args, **kwargs):
        self.conn = None
        self.cursor = None
        self.config: list[dict[str, Any]] = [{}]
        self.cookies: dict[str, str] = {}
        self.scraped_urls: list[str]= []
        self.output_destination: str = output_dst

        super().__init__(*args, **kwargs)

        conn_str = "dbname=config user=postgres password=stagingpass host=103.47.227.82 port=5432"
        data: bytes | None = None
        conn: connection | None = None
        cursor: cursortype | None = None
        try:
            conn = psycopg2.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT convert_from(data, 'UTF8') FROM configs WHERE id = %s", (config_id,))
            data: bytes = cursor.fetchone()[0]
            if data is None:
                raise ValueError("Config not found")

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while connecting to PostgreSQL", error)

        try:
            self.config = json.loads(data)
            self.cookies = self.config.get('cookies', {})

        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
            return

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        self.base_url: str = self.config.get('base_url', '')
        if not self.base_url:
            raise ValueError("No base URL configured")

        domain: str = urlparse(self.base_url.encode('utf-8')).netloc.decode('utf-8')
        self.output_file: str = f"{domain}_output.json"

        if os.path.exists(self.output_file):
            with open(self.output_file, "r") as f:
                data = json.load(f)
            self.scraped_urls = [item["url"] for item in data]
        else:
            self.scraped_urls = []

        self.items_collected: dict[str, Any] = {}
        self.cookies = self.config.get('cookies', {})

        if self.output_destination == "kafka":
            self.KAFKA_BOOTSTRAP_SERVERS = kafka_server
            self.KAFKA_TOPIC = kafka_topic

    def start_requests(self):
        yield Request(url=self.config['base_url'], callback=self.parse_structure, headers=self.headers, cookies=self.cookies, cb_kwargs={"structure": self.config["structure"]}, meta={'proxy': self.get_proxy()})

    def parse_structure(self, response: Response, structure):
        url: str = response.url
        if url not in self.items_collected:
            self.items_collected[url] = {"url": url}

        for key, value in structure.items():
            if key == "_element":
                continue

            elif "_list" in key:
                list_xpath: str = value.get("_element")
                if list_xpath:
                    for link_url in map(response.urljoin, response.xpath(list_xpath).getall()):
                        self.log(f"Found link: {link_url}")
                        yield response.follow(link_url, self.parse_structure, headers=self.headers, cookies=self.cookies, cb_kwargs={"structure": value}, meta={'proxy': self.get_proxy()})

            elif "_loop" in key:
                loop_elements: list[Response] = response.xpath(value["_element"])
                loop_keys: list[str] = [k for k in value.keys() if k not in ["_element", "_key", "_pagination"]]
                result: list[dict[str, Any]] = []

                for element in loop_elements:
                    data: dict[str, Any] = {}

                    for loop_key in loop_keys:
                        if isinstance(loop_key, str) and isinstance(value[loop_key], str):
                            extracted_data: list[str] | str = [item.strip() for item in element.xpath(value[loop_key]).getall() if item.strip()]
                            if len(extracted_data) == 1:
                                extracted_data = element.xpath(value[loop_key]).get()
                            if loop_key.startswith(('.', '/')) and (custom_key := response.xpath(loop_key).get()):
                                if custom_key:
                                    data[custom_key] = extracted_data
                            else:
                                if extracted_data:
                                    data[loop_key.rstrip("*")] = extracted_data
                                elif not loop_key.endswith("*"):
                                    self.log(f"Required key 1 '{loop_key}' not found in {url}")

                        elif isinstance(value[loop_key], dict):
                            sub_loop_data: list[dict[str, str]] = []
                            sub_elements: list[Response] = element.xpath(value[loop_key]["_element"])
                            for sub_element in sub_elements:
                                sub_data: dict[str, str] = {}
                                for sub_key, sub_value in value[loop_key].items():
                                    if sub_key in ["_element", "_key", "_pagination"]:
                                        continue
                                    extracted_sub_data: list[str] | str = [item.strip() for item in sub_element.xpath(sub_value).getall() if item.strip()]
                                    if len(extracted_sub_data) == 1:
                                        extracted_sub_data = sub_element.xpath(sub_value).get()
                                    if extracted_sub_data:
                                        sub_data[sub_key.rstrip("*")] = extracted_sub_data
                                if sub_data:
                                    sub_loop_data.append(sub_data)

                                data[value[loop_key].get("_key", "nested_loop")] = sub_loop_data

                    if data:
                        result.append(data)
                    try:
                        self.items_collected[url][value.get("_key", "loop_data")] = result
                    except KeyError:
                        pass

            if "_pagination" in value:
                next_page = response.xpath(value["_pagination"]).get()
                if next_page:
                    next_page_url = response.urljoin(next_page)
                    self.log(f"Following pagination to: {next_page_url}")
                    yield response.follow(url=next_page_url, callback=self.parse_structure, headers=self.headers, cookies=self.cookies, cb_kwargs={"structure": structure}, meta={'proxy': self.get_proxy()})

            if isinstance(value, dict):
                yield from self.parse_structure(response, value)

            elif isinstance(value, str) and key != "_pagination":
                extracted_data: list[str] = [item.strip() for item in response.xpath(value).getall() if item.strip()]
                if len(extracted_data) == 1:
                    extracted_data = response.xpath(value).get()
                if extracted_data:
                    try:
                        self.items_collected[url][key if key[len(key) - 1] != "*" else key[:len(key) - 1]] = extracted_data
                    except KeyError:
                        pass
                elif not key.endswith("*") and key != "_pagination" and value != "_pagination":
                    self.log(f"Required key 2 '{key}' not found in {url}")
            try:
                collected_data: dict[str, Any] = self.items_collected[url]
                if self._is_data_complete(collected_data, structure, response.url):
                    if any(key != "url" for key in collected_data.keys()):
                        yield self.items_collected.pop(url)
                    else:
                        self.items_collected.pop(url)
            except KeyError:
                pass
            except Exception as e:
                self.logger.exception(e)

    def _is_data_complete(self, collected_data, structure, url):
        for key, value in structure.items():
            if key.startswith(("_", "@")):
                continue

            is_optional: str = key.endswith("*")
            key_base: str = key.rstrip("*") if is_optional else key

            if key_base not in collected_data or collected_data[key_base] is None:
                if not is_optional:
                    self.log(f"Required key 3 '{key_base}' not in collected_data for url {url}")
                return False

            if isinstance(value, dict):
                if not self._is_data_complete(collected_data.get(key_base, {}), value, url):
                    return False

        return True