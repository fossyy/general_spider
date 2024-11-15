import json

import psycopg2
from scrapy import Request, Spider
from scrapy.http import Response

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

    def __init__(self, config_path=None, *args, **kwargs):
        self.conn = None
        self.cursor = None
        self.config = {}
        self.items_collected = {}
        self.cookies = {}

        super().__init__(*args, **kwargs)

        conn_str = "dbname=test user=postgres password=admin host=localhost"

        try:
            self.conn = psycopg2.connect(conn_str)
            self.cursor = self.conn.cursor()

            self.cursor.execute("SELECT data_column FROM test WHERE id = %s", (config_path,))
            row = self.cursor.fetchone()

            if row is None:
                print(f"No data found for id {config_path}")
                return

            data = row[0]
            print("Retrieved data (as bytes):", data)

            if isinstance(data, bytes):
                data = data.decode('utf-8')
            elif not isinstance(data, str):
                print(f"Unexpected data type: {type(data)}")
                return

            try:
                self.config = json.loads(data)
                print("Decoded JSON:", self.config)

                self.cookies = self.config.get('cookies', {})
            except json.JSONDecodeError as e:
                print("Error decoding JSON:", e)
                return

        except Exception as e:
            print(f"Error connecting to the database: {e}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

    def start_requests(self):
        print("config : ", self.config)
        yield Request(url=self.config['base_url'], callback=self.parse_structure, headers=self.headers, cookies=self.cookies, cb_kwargs={"structure": self.config["structure"]}, meta={'proxy': self.get_proxy()})

    def parse_structure(self, response: Response, structure):
        url = response.url
        if url not in self.items_collected:
            self.items_collected[url] = {"url": url}

        for key, value in structure.items():
            if key == "_element":
                continue

            if "_pagination" in value and (next_page := response.xpath(value["_pagination"]).get()):
                next_page_url = response.urljoin(next_page)
                self.log(f"Following pagination to: {next_page_url}")
                yield response.follow(next_page_url, self.parse_structure, headers=self.headers, cookies=self.cookies, cb_kwargs={"structure": structure}, meta={'proxy': self.get_proxy()})

            if "_list" in key and (list_xpath := value.get("_element")):
                for link_url in map(response.urljoin, response.xpath(list_xpath).getall()):
                    self.log(f"Found link: {link_url}")
                    yield response.follow(link_url, self.parse_structure, headers=self.headers, cookies=self.cookies, cb_kwargs={"structure": value}, meta={'proxy': self.get_proxy()})

            elif key == "_loop":
                loop_elements = response.xpath(value["_element"])
                loop_keys = [k for k in value.keys() if k not in ["_element", "_key", "_pagination"]]
                result = []
                for element in loop_elements:
                    data = {}
                    for loop_key in loop_keys:
                        if isinstance(loop_key, str):
                            if isinstance(value[loop_key], str):
                                extracted_data = element.xpath(value[loop_key]).getall()
                                stripped_data = [item.strip() for item in extracted_data if item.strip()]
                                if len(stripped_data) == 1:
                                    extracted_data = element.xpath(value[loop_key]).get()
                                if extracted_data:
                                    data[loop_key.rstrip("*")] = extracted_data
                                else:
                                    is_optional = loop_key.endswith("*")

                                    if not is_optional:
                                        self.log(f"Required key '{loop_key}' not found in {url}")

                            elif isinstance(value[loop_key], dict):
                                sub_loop_data = []
                                sub_elements = element.xpath(value[loop_key]["_element"])
                                for sub_element in sub_elements:
                                    sub_data = {}
                                    for sub_key, sub_value in value[loop_key].items():
                                        if sub_key in ["_element", "_key"]:
                                            continue
                                        extracted_sub_data = sub_element.xpath(sub_value).getall()
                                        stripped_sub_data = [item.strip() for item in extracted_sub_data if item.strip()]
                                        if len(stripped_sub_data) == 1:
                                            extracted_sub_data = sub_element.xpath(sub_value).get()
                                        if extracted_sub_data:
                                            sub_data[sub_key.rstrip("*")] = extracted_sub_data
                                    if sub_data:
                                        sub_loop_data.append(sub_data)
                                data[value[loop_key].get("_key", "nested_loop")] = sub_loop_data

                    if data:
                        result.append(data)
                self.items_collected[url][value.get("_key", "loop_data")] = result

            elif isinstance(value, dict):
                yield from self.parse_structure(response, value)

            elif isinstance(value, str):
                extracted_data = response.xpath(value).getall()
                stripped_data = [item.strip() for item in extracted_data if item.strip()]
                if len(stripped_data) == 1:
                    extracted_data = response.xpath(value).get()
                if extracted_data:
                    self.items_collected[url][key if key[len(key) - 1] != "*" else key[:len(key) - 1]] = extracted_data
                else:
                    is_optional = key.endswith("*")
                    if not is_optional:
                        self.log(f"Required key '{key}' not in collected_data for url {url}")

        try:
            collected_data = self.items_collected[url]
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
            if key.startswith("_") or key.startswith("@"):
                continue

            is_optional = key.endswith("*")
            key_base = key.rstrip("*") if is_optional else key

            if isinstance(value, dict):
                if key_base not in collected_data or not self._is_data_complete(collected_data.get(key_base, {}),
                                                                                value, url):
                    if not is_optional:
                        self.log(f"Required key '{key_base}' not in collected_data for url {url}")
                        return False
            elif key_base not in collected_data:
                if not is_optional:
                    self.log(f"Required key '{key_base}' not in collected_data for url {url}")
                    return False

            elif collected_data[key_base] is None:
                if not is_optional:
                    self.log(f"Required key '{key_base}' not in collected_data for url {url}")
                    return False

        return True