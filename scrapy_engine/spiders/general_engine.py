import json
import scrapy


class GeneralEngineSpider(scrapy.Spider):
    name = "general_engine"

    proxies = [
        "http://localhost:8118",
    ]

    current_proxy = 0

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
        super().__init__(*args, **kwargs)
        with open(config_path, 'r') as file:
            self.config = json.load(file)
        self.items_collected = {}

    def start_requests(self):
        yield scrapy.Request(
            url=self.config['base_url'],
            callback=self.parse_structure,
            headers=self.headers,
            cb_kwargs={"structure": self.config["structure"]},
            meta={'proxy': self.get_proxy()}
        )

    def parse_structure(self, response: scrapy.http.Response, structure):
        url = response.url
        if url not in self.items_collected:
            self.items_collected[url] = {"url": url}

        for key, value in structure.items():
            if key == "_element":
                continue

            if key == "_pagination":
                pagination_xpath = value["_element"]
                if pagination_xpath:
                    next_page = response.xpath(pagination_xpath).get()
                    if next_page:
                        next_page_url = response.urljoin(next_page)
                        self.log(f"Found pagination link: {next_page_url}")
                        yield response.follow(
                            url=next_page_url,
                            callback=self.parse_structure,
                            headers=self.headers,
                            cb_kwargs={"structure": structure},
                            meta={'proxy': self.get_proxy()}
                        )

            elif key == "_list":
                list_xpath = value["_element"]
                if list_xpath:
                    links = response.xpath(list_xpath).getall()
                    for link in links:
                        link_url = response.urljoin(link)
                        self.log(f"Found link: {link_url}")
                        yield response.follow(
                            url=link_url,
                            callback=self.parse_structure,
                            headers=self.headers,
                            cb_kwargs={"structure": value},
                            meta={'proxy': self.get_proxy()}
                        )

            elif key == "_loop":
                loop_elements = response.xpath(value["_element"])
                loop_keys = [k for k in value.keys() if k not in ["_element", "_key"]]

                result = []
                for element in loop_elements:
                    data = {}
                    for loop_key in loop_keys:
                        data[loop_key if loop_key[len(loop_key) - 1] != "*" else loop_key[:len(loop_key) - 1]] = element.xpath(value[loop_key]).get()
                    result.append(data)

                self.items_collected[url]["_loop" if value["_key"] is None else value["_key"]] = result

            elif isinstance(value, dict):
                yield from self.parse_structure(response, value)

            elif isinstance(value, str):
                extracted_data = response.xpath(value).getall()
                stripped_data = [item.strip() for item in extracted_data if item.strip()]
                if len(stripped_data) == 1:
                    extracted_data = response.xpath(value).get()

                if extracted_data:
                    self.items_collected[url][key if key[len(key) - 1] != "*" else key[:len(key) - 1]] = extracted_data

        collected_data = self.items_collected[url]
        if self._is_data_complete(collected_data, structure, response.url):
            if any(key != "url" for key in collected_data.keys()):
                yield self.items_collected.pop(url)
            else:
                self.items_collected.pop(url)


    def _is_data_complete(self, collected_data, structure, url):
        for key, value in structure.items():
            if key.startswith("_") or key.startswith("@"):
                continue

            is_optional = key.endswith("*")
            key_base = key.rstrip("*") if is_optional else key

            if isinstance(value, dict):
                if key_base not in collected_data or not self._is_data_complete(collected_data.get(key_base, {}),
                                                                                value):
                    if not is_optional:
                        self.log(f"Required key '{key_base}' not in collected_data for url {url}")
                        return False
            elif key_base not in collected_data:
                if not is_optional:
                    self.log(f"Required key '{key_base}' not in collected_data for url {url}")
                    return False

        return True
