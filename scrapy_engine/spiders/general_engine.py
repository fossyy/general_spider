import json, psycopg2, os, random, string, base64, re

from typing import Any
from scrapy import Request, Spider
from scrapy.http import Response
from twisted.web.http import urlparse
from dotenv import load_dotenv
from psycopg2._psycopg import connection, cursor as cursortype
from pathlib import Path
from urllib.parse import urlparse

class GeneralEngineSpider(Spider):
    name: str = "general_engine"
    
    def __init__(self, config_id = None, output_dst = "local", kafka_server = None, kafka_topic = None, preview = "no", preview_config = None, proxies = None, preview_proxies = None, cookies = None, *args, **kwargs):
        self.conn: connection | None = None
        self.cursor: cursortype | None = None
        self.config: list[dict[str, Any]] = [{}]
        self.cookies: dict[str, str] = {}
        self.scraped_urls: list[str]= []
        self.output_dst: str = output_dst
        self.crawled_count: int = 0
        self.status_codes: dict[str, int] = {}
        self.job_id: str | None = kwargs.get('_job')
        self.preview: str = preview
        self.headers: dict[str, str] = {}           
        self.proxies: bytes[list[str]] = proxies
        self.cookies: bytes[dict[str, str]] = cookies
        
        if self.proxies is None:
            raise ValueError("Proxies cannot be None")
        self.proxies = json.loads(base64.b64decode(self.proxies).decode("utf-8"))    
        
        if self.cookies is not None:
            self.cookies = json.loads(base64.b64decode(self.cookies).decode("utf-8"))

        if self.job_id is None:
            self.job_id = ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(24))

        super().__init__(*args, **kwargs)
        if self.preview == "yes":
            self.preview_config = preview_config
            self.preview_proxies = preview_proxies
            if self.preview_config is None:
                raise ValueError("preview_config cannot be None for preview run")
            if self.preview_proxy is None:
                raise ValueError("preview_proxies cannot be None for preview run")
            self.config = json.loads(base64.b64decode(self.preview_config).decode("utf-8"))
            self.proxies = json.loads(base64.b64decode(self.preview_proxies).decode("utf-8"))
        else:
            load_dotenv()
            dbHost: str = os.environ.get('DB_HOST', None)
            dbPort: str = os.environ.get('DB_PORT', None)
            dbName: str = os.environ.get('DB_NAME', None)
            dbUser: str = os.environ.get('DB_USERNAME', None)
            dbPass: str = os.environ.get('DB_PASSWORD', None)
            if dbHost is None or dbPort is None or dbName is None or dbUser is None or dbPass is None:
                raise ValueError("Missing required environment variables for database")

            conn_str: str = f"dbname={dbName} user={dbUser} password={dbPass} host={dbHost} port={dbPort}"
            try:
                self.conn = psycopg2.connect(conn_str)
                self.cursor = self.conn.cursor()
                self.cursor.execute("SELECT convert_from(data, 'UTF8') FROM configs WHERE id = %s", (config_id,))
                data: bytes = self.cursor.fetchone()[0]
                if data is None:
                    raise ValueError("Config not found")

            except (Exception, psycopg2.DatabaseError) as error:
                raise ConnectionError("Error while connecting to PostgreSQL", error)

            try:
                self.config = json.loads(data)
                self.headers = self.config.get('headers', {})

            except json.JSONDecodeError as e:
                raise ValueError("Config not found")
            
            finally:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()

            self.base_url: str = self.config.get('base_url', '')
            if not self.base_url:
                raise ValueError("No base URL configured")

            self.result_folder = Path(f"results/{urlparse(self.base_url).netloc if self.base_url is not None else 'default_output'}") 
            self.result_folder.mkdir(parents=True, exist_ok=True)    
            self.output_file = self.result_folder/"result.json"
            
            if self.output_file.exists():
                try:
                    with open(self.output_file, "r") as f:
                        json_content = re.sub(r'},\s*]', '}]',
                                       re.sub(r'},\s*}', '}}',  
                                       re.sub(r',\s*]', ']', 
                                       re.sub(r',\s*}', '}',   
                                       re.sub(r'},\s*$', '}', 
                                       f.read())))))
                        json_content += ']' if not json_content.strip().endswith(']') else ''
                        data = json.loads(json_content)
                except json.JSONDecodeError as e:
                    if "Expecting ',' delimiter" in str(e):
                        raise ValueError(f"Invalid JSON format: {e}")
                    else:
                        raise
                except Exception as e:
                    raise RuntimeError("Unknown error while reading output file")   
                if isinstance(data, dict):
                    self.scraped_urls = [item["url"] for item in data]
                elif isinstance(data, list):
                    for sub_data in data:
                        if isinstance(sub_data, dict) and "url" in sub_data:
                            self.scraped_urls.append(sub_data["url"])
                        else:
                            self.scraped_urls.append(sub_data)
                else:
                    raise ValueError("Invalid data format in output file")
            else:
                self.scraped_urls = []

        self.items_collected: dict[str, Any] = {}

        if self.output_dst == "kafka":
            self.KAFKA_BOOTSTRAP_SERVERS = kafka_server
            self.KAFKA_TOPIC = kafka_topic
    
    current_proxy: int = 0    
    def get_proxy(self):
        current_proxy_now = self.current_proxy % len(self.proxies)
        self.current_proxy += 1
        return self.proxies[current_proxy_now]

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
                            # if loop_key.startswith(('.', '/')) and (custom_key := response.xpath(loop_key).get()):
                            #     if custom_key:
                            #         data[custom_key] = extracted_data
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
                next_page: str = response.xpath(value["_pagination"]).get()
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