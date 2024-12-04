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

    def __init__(self, config_id = None, output_dst = "local", kafka_server = None, kafka_topic = None, preview = "no", preview_config = None, proxies = None, preview_proxies = None, cookies = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.conn: connection | None = None
        self.cursor: cursortype | None = None
        self.config: list[dict[str, Any]] = [{}]
        self.scraped_urls: list[str]= []
        self.output_dst: str = output_dst
        self.crawled_count: int = 0
        self.status_codes: dict[str, int] = {}
        self.job_id: str = kwargs.get('_job') or self._generate_job_id()
        self.headers: dict[str, str] = {}
        self.proxies: list[str] = self._decode_base64(proxies) if proxies else []
        self.preview_proxies: list[str] = self._decode_base64(preview_proxies) if preview_proxies else []
        self.cookies: dict[str, str] = self._decode_base64(cookies) if cookies else {}
        self.preview : str  = preview
        self.data: tuple | None = None
        self.items_collected: dict[str, Any] = {}
        
        
        if  self.preview == "yes":
            self._initialize_preview(preview_config, preview_proxies)
        else:
            self._initialize_database(config_id)

        if self.output_dst == "kafka":
            self.KAFKA_BOOTSTRAP_SERVERS = kafka_server
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
                    self.scraped_urls = [item["link"] for item in data]
                elif isinstance(data, list):
                    for sub_data in data:
                        if isinstance(sub_data, dict) and "link" in sub_data:
                            self.scraped_urls.append(sub_data["link"])
                        else:
                            self.scraped_urls.append(sub_data)
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
        yield Request(url=self.config['base_url'] + '/', callback=self.parse_structure, headers=self.config.get('headers', {}), cookies=self.cookies, cb_kwargs={"structure": self.config["structure"]}, meta={'proxy': self._get_proxy()})

    def parse_structure(self, response: Response, structure, nested=False):
        url: str = response.url
        if url not in self.items_collected:
            self.items_collected[url] = {"link": url, "global": {}, "parent": {}}

        for key, value in structure.items():
            if key == "_element":
                continue

            elif "_list" in key:
                list_xpath: str = value.get("_element")
                if list_xpath:
                    for link_url in map(response.urljoin, response.xpath(list_xpath).getall()):
                        self.logger.info(f"Found link: {link_url}")
                        yield response.follow(link_url, self.parse_structure, headers=self.config.get('headers', {}), cookies=self.cookies, cb_kwargs={"structure": value}, meta={'proxy': self._get_proxy()})

            elif "_loop" in key:
                loop_elements: list[Response] = response.xpath(value["_element"])
                loop_keys: list[str] = [k for k in value.keys() if k not in ["_element", "_key", "_pagination", "_tag"]]
                result: list[dict[str, Any]] = []
                
                for element in loop_elements:
                    data: dict[str, Any] = {}

                    for loop_key in loop_keys:
                        if isinstance(value[loop_key], str) and isinstance(value[loop_key], str):
                            extracted_data: list[str] | str = [item.strip() for item in element.xpath(value[loop_key]).getall() if item.strip()]
                            if len(extracted_data) == 1:
                                extracted_data = element.xpath(value[loop_key]).get()
                            else:
                                if extracted_data:
                                    data[loop_key] = extracted_data
                                # elif not loop_key.endswith("*"):
                                #     self.log(f"Required key 1 '{loop_key}' not found in {url}")

                        elif isinstance(value[loop_key], dict):
                            sub_value = value[loop_key]
                            if {'value', 'type'}.issubset(sub_value.keys()):
                                extracted_data = None
                            
                                value_type = sub_value['type']
                                if value_type is None:
                                    extracted_data = response.xpath(sub_value['value']).get()
                                    
                                elif value_type in ['str', 'int', 'timestamp'] or value_type is None:
                                    if 'timestamp' in value_type:
                                        date = response.xpath(sub_value['value']).get()
                                        extracted_data = int(dateparser.parse(date).timestamp()*1000)
                                    else:
                                        extracted_data = response.xpath(sub_value['value']).get()
                                        
                                elif 'list' in value_type:
                                    extracted_data = response.xpath(sub_value['value']).getall()
                                        
                                tag = structure.get("_tag", None)
                                if tag is not None and tag != "":
                                    try:
                                        self.items_collected[url][tag].update({key: extracted_data})
                                    except KeyError:
                                        self.items_collected[url][tag] = ({key: extracted_data})
                        try:
                            collected_data: dict[str, Any] = self.items_collected[url]
                            if self._is_data_complete(collected_data, structure, response.url):
                                if any(key != "link" for key in collected_data.keys()):
                                    yield self.items_collected.pop(url)
                                else:
                                    self.items_collected.pop(url)
                        except KeyError:
                            pass
                        except Exception as e:
                            self.logger.exception(e)
                                
                            # for sub_key, sub_value in value[loop_key].items():
                            #     print(f" sub_key :  {sub_key} sub_value : {sub_value}")
                            #     if {'value', 'type'}.issubset(sub_value.keys()):
                            #         extracted_data = None
                            
                            #         value_type = sub_value['type']
                            #         if value_type is None:
                            #             extracted_data = response.xpath(sub_value['value']).get()
                                        
                            #         elif value_type in ['str', 'int', 'timestamp'] or value_type is None:
                            #             if 'timestamp' in value_type:
                            #                 date = response.xpath(sub_value['value']).get()
                            #                 extracted_data = int(dateparser.parse(date).timestamp()*1000)
                            #             else:
                            #                 extracted_data = response.xpath(sub_value['value']).get()
                                        
                            #         elif 'list' in value_type:
                            #             extracted_data = response.xpath(sub_value['value']).getall()
                                            
                            #         tag = structure.get("_tag", None)
                            #         if tag is not None and tag != "":
                            #             try:
                            #                 self.items_collected[url][tag].update({key: extracted_data})
                            #             except KeyError:
                            #                 self.items_collected[url][tag] = ({key: extracted_data})
                            #         else:
                            #             self.items_collected[url][key] = extracted_data
                            # sub_loop_data: list[dict[str, str]] = []
                            # sub_elements: list[Response] = element.xpath(value[loop_key]["_element"])
                            # for sub_element in sub_elements:
                            #     sub_data: dict[str, str] = {}
                            #     for sub_key, sub_value in value[loop_key].items():
                            #         if sub_key in ["_element", "_key", "_pagination"]:
                            #             continue
                            #         extracted_sub_data: list[str] | str = [item.strip() for item in sub_element.xpath(sub_value).getall() if item.strip()]
                            #         if len(extracted_sub_data) == 1:
                            #             extracted_sub_data = sub_element.xpath(sub_value).get()
                            #         if extracted_sub_data:
                            #             sub_data[sub_key.rstrip("*")] = extracted_sub_data
                            #     if sub_data:
                            #         sub_loop_data.append(sub_data)

                            #     data[value[loop_key].get("_key", "nested_loop")] = sub_loop_data

                    if data:
                        result.append(data)
                    try:
                        tag = value.get("_tag", None)
                        key_to_use = value.get("_key", "loop_data")
                        if tag is not None and tag != "":
                            try:
                                self.items_collected[url][tag].update({key_to_use: result})
                            except KeyError:
                                self.items_collected[url][tag] = ({key_to_use: result})
                        else:
                            self.items_collected[url][key_to_use] = result
                    except KeyError:
                        pass

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

           
            
            if isinstance(value, dict): 
                if {'value', 'type'}.issubset(value.keys()): 
                    extracted_data = None 
                    value_type = value['type'] 
                    xpath_value = value['value'] 
 
                    if value_type in [None, 'str', 'int']: 
                        extracted_data = response.xpath(xpath_value).get() 
 
                    elif value_type == 'timestamp': 
                        date = response.xpath(xpath_value).get() 
                        extracted_data = int(dateparser.parse(date).timestamp() * 1000) if date else None 
 
                    elif value_type == 'list': 
                        extracted_data = response.xpath(xpath_value).getall() 
                            
                    tag = structure.get("_tag", None)
                    if tag == 'root':
                        try:
                            self.items_collected[url].update({key: extracted_data})
                        except KeyError:
                            self.items_collected[url] = ({key: extracted_data})

                        try:
                            self.items_collected[url]['global'].update({key: extracted_data})
                        except KeyError:
                            self.items_collected[url]['global'] = ({key: extracted_data})
                            
                        try:
                            self.items_collected[url]['parent'].update({key: extracted_data})
                        except KeyError:
                            self.items_collected[url]['parent'] = ({key: extracted_data})
                            
                    elif tag == 'global':
                        collected = self.items_collected[url]
                        collected['global'] = {}
                        collected['parent'] = {}
                        
                        try:
                            collected['global'].update({key: extracted_data})
                        except KeyError:
                            collected['global'] = ({key: extracted_data})
                        
                        try:
                            collected['parent'].update({key: extracted_data})
                        except KeyError:
                            collected['parent'] = ({key: extracted_data})
                    
                    elif tag == 'parent':
                        collected_global = self.items_collected[url + 'global']
                        clean = {key: value for key, value in collected_global.items() if key not in ["global", "parent", "content_stat", "detail_feature"]}
                        
                        if url + tag not in self.items_collected:
                            self.items_collected[url + tag] = {"link": url, 'global': collected['global'],  "parent": {}}
                                                        
                        collected = self.items_collected[url + tag]
                        for key, value in clean.items():
                            collected[key] = value
                            
                        collected['parent'] = {}
                        
                        try:
                            collected['global'].update({key: extracted_data})
                        except KeyError:
                            collected['global'] = ({key: extracted_data})
                        
                        try:
                            collected['parent'].update({key: extracted_data})
                        except KeyError:
                            collected['parent'] = ({key: extracted_data})     
                           
                    try:
                        if tag == 'root':
                            collected_data: dict[str, Any] = self.items_collected[url]
                        else:
                            collected_data: dict[str, Any] = self.items_collected[url + tag]
                                
                        if self._is_data_complete(collected_data, structure, response.url):
                            if any(key != "link" for key in collected_data.keys()):
                                yield self.items_collected.pop(url)
                            # else:
                            #     self.items_collected.pop(url)
                    except KeyError:
                        pass
                    except Exception as e:
                        self.logger.exception(e)    
                        
                    # if tag is not None and tag != "":
                    #     try:
                    #         self.items_collected[url][tag].update({key: extracted_data})
                    #     except KeyError:
                    #         self.items_collected[url][tag] = ({key: extracted_data})
                    # else:
                    #     self.items_collected[url][key] = extracted_data
                        
                elif not nested:
                    yield from self.parse_structure(response, value, nested=True)
        
            # try:
            #     collected_data: dict[str, Any] = self.items_collected[url]
            #     if self._is_data_complete(collected_data, structure, response.url):
            #         if any(key != "link" for key in collected_data.keys()):
            #             yield self.items_collected.pop(url)
            #         else:
            #             self.items_collected.pop(url)
            # except KeyError:
            #     pass
            # except Exception as e:
            #     self.logger.exception(e)

    def _is_data_complete(self, collected_data, structure, url):
        for key, value in structure.items():
            if key.startswith("_") and key != "_loop":
                continue

            if key.startswith("@") or key == "_loop":
                tag_key = value.get("_tag", key)
                tag_data = collected_data.get(tag_key, {})
                if not self._is_data_complete(tag_data, value, url):
                    return False

            elif isinstance(value, dict) and "value" in value:
                optional = value.get("optional", False)

                if not optional and not collected_data.get(key):
                    self.logger.warning(f"Missing required field '{key}' for URL: {url}")
                    return False

            elif isinstance(value, dict):
                nested_data = collected_data.get(key)
                if not self._is_data_complete(nested_data, value, url):
                    return False

            elif key not in collected_data or collected_data[key] is None:
                self.logger.warning(f"Field '{key}' is missing for URL: {url}")
                return False

        return True
