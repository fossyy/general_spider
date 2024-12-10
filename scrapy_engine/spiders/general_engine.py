import json, psycopg2, os, random, string, base64, re, dateparser

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

    def __init__(self, config_id=None, output_dst="local", kafka_server=None, kafka_topic=None, preview="no", preview_config=None, proxies=None, preview_proxies=None, cookies=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.conn: connection | None = None
        self.cursor: cursortype | None = None
        self.config: list[dict[str, Any]] = [{}]
        self.scraped_urls: list[str] = []
        self.output_dst: str = output_dst
        self.crawled_count: int = 0
        self.status_codes: dict[str, int] = {}
        self.job_id: str = kwargs.get('_job') or self._generate_job_id()
        self.headers: dict[str, str] = {}
        self.proxies: list[str] = self._decode_base64(proxies) if proxies else []
        self.preview_proxies: list[str] = self._decode_base64(preview_proxies) if preview_proxies else []
        self.cookies: dict[str, str] = self._decode_base64(cookies) if cookies else {}
        self.preview: str = preview
        self.data: tuple | None = None
        self.items_collected: dict[str, Any] = {}

        if self.preview == "yes":
            self._initialize_preview(preview_config, preview_proxies)
        else:
            self._initialize_database(config_id)

        if self.output_dst == "kafka":
            self.KAFKA_BOOTSTRAP_SERVERS = self._decode_base64(kafka_server)
            self.KAFKA_TOPIC = kafka_topic

    def _initialize_preview(self, preview_config, preview_proxies):
        if preview_config is None or preview_proxies is None:
            raise ValueError("`preview_config` and `preview_proxies` cannot be None for preview run")
        self.config = self._decode_base64(preview_config)
        self.proxies = self._decode_base64(preview_proxies)

    def _initialize_database(self, config_id):
        load_dotenv()
        required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USERNAME', 'DB_PASSWORD']
        db_config = {var: os.environ.get(var) for var in required_vars}
        if None in db_config.values():
            raise ValueError("Missing required environment variables for database")

        conn_str = f"dbname={db_config['DB_NAME']} user={db_config['DB_USERNAME']} password={db_config['DB_PASSWORD']} host={db_config['DB_HOST']} port={db_config['DB_PORT']}"

        try:
            self.conn = psycopg2.connect(conn_str)
            self.cursor = self.conn.cursor()
            self.cursor.execute("SELECT id, name, convert_from(data, 'UTF8') FROM configs WHERE id = %s", (config_id,))
            self._load_config_data()
        except psycopg2.DatabaseError as e:
            raise ConnectionError(f"Error connecting to database: {e}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

    def _load_config_data(self):
        self.data = self.cursor.fetchone()
        if not self.data:
            raise ValueError("Configuration data not found")
        self.config = json.loads(self.data[2])
        self.base_url = self.config.get('base_url', '')
        if not self.base_url:
            raise ValueError("Base URL is required")

        self.result_folder = self._prepare_result_folder()
        self.output_file = self._prepare_output_file()

        if self.output_file.exists():
            try:
                with open(self.output_file, "r") as f:
                    json_content = f.read()
                patterns_replacements = [
                    (r'},\s*]', '}]'),      
                    (r'},\s*}', '}}'),      
                    (r',\s*]', ']'),        
                    (r',\s*}', '}'),        
                    (r'},\s*$', '}'),       
                    (r'^\[\s*,', '['),      
                ]
                for pattern, replacement in patterns_replacements:
                    json_content = re.sub(pattern, replacement, json_content)
                
                json_content = json_content.strip()
                if json_content.startswith("[,") or json_content == "[,\n":
                    json_content = "[" 
                if not json_content.endswith(']'):
                    json_content += ']'

                data = json.loads(json_content)
            except json.JSONDecodeError as e:
                if "Expecting ',' delimiter" in str(e):
                    raise ValueError(f"Invalid JSON format: {e}")
                else:
                    raise
            except Exception as e:
                raise RuntimeError("Unknown error while reading output file")
            if isinstance(data, dict):
                self.scraped_urls = [item["link"] for item in data]
            elif isinstance(data, list):
                for sub_data in data:
                    link = sub_data["link"] if isinstance(sub_data, dict) and "link" in sub_data else sub_data
                    if link not in self.scraped_urls:
                        self.scraped_urls.append(link)
            else:
                raise ValueError("Invalid data format in output file")
        else:
            self.scraped_urls = []

    def _prepare_result_folder(self):
        folder = Path(f"results/{urlparse(self.base_url).netloc}")
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _prepare_output_file(self):
        prefix = "local" if self.output_dst == "local" else "kafka"
        filename = f"{prefix}-{self.data[1]}-{self.data[0]}-result.json"
        return self.result_folder / filename

    def _decode_base64(self, data: str):
        return json.loads(base64.b64decode(data).decode("utf-8"))

    def _generate_job_id(self) -> str:
        return ''.join(random.choices(string.ascii_lowercase + string.digits, k=24))

    current_proxy: int = 0

    def _get_proxy(self):
        current_proxy_now = self.current_proxy % len(self.proxies)
        self.current_proxy += 1
        return self.proxies[current_proxy_now]

    def start_requests(self):
        yield Request(url=self.config['base_url'], callback=self.parse_structure, headers=self.config.get('headers', {}), cookies=self.cookies, cb_kwargs={"structure": self.config["structure"]}, meta={'proxy': self._get_proxy()})

    def handle_data_extraction(self, response: Response, key: str, value: dict, tag: str, result: dict, loop_data: dict):
        extracted_data = None
        value_type = value.get('type')
        xpath_value = value.get('value')

        try:
            if value_type in [None, 'str', 'int']:
                raw_data = response.xpath(xpath_value).get()
                extracted_data = int(raw_data) if value_type == 'int' and raw_data else raw_data
            elif value_type == 'timestamp':
                date = response.xpath(xpath_value).get()
                extracted_data = int(dateparser.parse(date).timestamp() * 1000) if date else None
            elif value_type == 'list':
                extracted_data = response.xpath(xpath_value).getall()
            elif value_type == "constraint":
                extracted_data = xpath_value
        except Exception as e:
            self.logger.exception(f"Error extracting data for key {key}: {e}")
            return result

        if tag == 'root':
            result[key] = extracted_data
            result['global'][key] = extracted_data
            result['parent'][key] = extracted_data
        elif tag == 'global':
            if loop_data:
                loop_data['global'][key] = extracted_data
            result['global'][key] = extracted_data
        elif tag == 'parent':
            if loop_data:
                loop_data['parent'][key] = extracted_data
            result['parent'][key] = extracted_data

        return result

    def handle_loop(self, response: Response, loop_structure: dict, base_url: str, parent_data: dict):
        loop_elements = response.xpath(loop_structure["_element"])
        tag = loop_structure.get("_tag", None)
        filtered_root_data = {}

        for idx, element in enumerate(loop_elements):
            loop_result = {"link": base_url, "global": {}, "parent": {}}

            for sub_key, sub_value in loop_structure.items():
                if sub_key in ["_element", "_key", "_tag", "_pagination"]:
                    continue

                if isinstance(sub_value, dict) and {"value", "type"}.issubset(sub_value.keys()):
                    extracted_data = None
                    data_type = sub_value["type"]
                    xpath_value = sub_value["value"]
                    if parent_data is not loop_result:
                        if base_url in self.items_collected:
                            root_data = self.items_collected[base_url]
                            filtered_root_data = {
                                key: value for key, value in root_data.items()
                                if key not in ["global", "parent", "content_stat", "detail_feature"]
                            }
                            parent_data.update(filtered_root_data)

                    try:
                        if data_type == "str":
                            extracted_data = element.xpath(xpath_value).get()
                        elif data_type == "int":
                            raw_data = element.xpath(xpath_value).get()
                            extracted_data = int(raw_data) if raw_data else None
                        elif data_type == "timestamp":
                            raw_data = element.xpath(xpath_value).get()
                            extracted_data = int(
                                dateparser.parse(raw_data).timestamp() * 1000) if raw_data else None
                        elif data_type == "list":
                            extracted_data = element.xpath(xpath_value).getall()
                        elif value_type == "constraint":
                            extracted_data = xpath_value
                    except (ValueError, TypeError):
                        extracted_data = None

                    if tag == "root":
                        loop_result[sub_key] = extracted_data
                        loop_result["global"][sub_key] = extracted_data
                        loop_result["parent"][sub_key] = extracted_data
                    elif tag == "global":
                        loop_result["global"][sub_key] = extracted_data
                        loop_result["parent"][sub_key] = extracted_data
                    elif tag == "parent":
                        loop_result["parent"][sub_key] = extracted_data
                        loop_result["global"] = parent_data.get("global", {}).copy()

                elif sub_key == "_loop":
                    yield from self.handle_loop(element, sub_value, base_url, loop_result)
            if filtered_root_data:
                loop_result.update(filtered_root_data)
            yield loop_result

        if parent_data is not None and "_loop" not in loop_structure.keys():
            try:
                self.items_collected.pop(base_url)
            except KeyError:
                pass
    def parse_structure(self, response: Response, structure, nested=False, url=None, loop_data=None, caller_key=None):
        if url is None:
            url: str = response.url

        result = {"link": url, "global": {}, "parent": {}}
        for key, value in structure.items():
            if key == "_element":
                continue

            elif "_list" in key:
                list_xpath: str = value.get("_element")
                if list_xpath:
                    for link_url in map(response.urljoin, response.xpath(list_xpath).getall()):
                        yield response.follow(link_url, self.parse_structure, headers=self.config.get('headers', {}), cookies=self.cookies, cb_kwargs={"structure": value}, meta={'proxy': self._get_proxy()})

            elif "_loop" in key:
                if "_element" not in value:
                    raise ValueError("Missing '_element' key in loop configuration.")
                yield from self.handle_loop(response, value, url, self.items_collected[url] or result)

            if "_pagination" in key:
                next_page: str = response.xpath(value).get()
                if next_page:
                    if response.url == f"{urlparse(url).scheme}://{urlparse(url).netloc}":
                        if next_page.startswith('?'):
                            next_page_url = response.urljoin("/" + next_page)
                        else:
                            next_page_url = response.urljoin(next_page)
                    else:
                        next_page_url = response.urljoin(next_page)
                    self.logger.info(f"Following pagination to: {next_page_url}")
                    yield response.follow(url=next_page_url, callback=self.parse_structure, headers=self.config.get('headers', {}), cookies=self.cookies, cb_kwargs={"structure": structure}, meta={'proxy': self._get_proxy()})

            if isinstance(value, dict) and {'value', 'type'}.issubset(value.keys()) and key != "_loop" and caller_key != "_loop":
                tag = structure.get("_tag", None)
                result = self.handle_data_extraction(response=response, key=key, value=value, tag=tag, result=result, loop_data=loop_data)
                self.items_collected[url] = result

            if isinstance(value, dict):
                if not nested:
                    yield from self.parse_structure(response, value, nested=True, caller_key=key)

        if result != {"link": url, "global": {}, "parent": {}}:
            try:
                yield result
            except Exception as e:
                self.logger.exception(f"Error yielding result: {e}")