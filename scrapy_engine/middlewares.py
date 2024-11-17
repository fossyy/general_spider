import json
from scrapy.exceptions import IgnoreRequest

class ScrapyEngineSpiderMiddleware:
    def __init__(self):
        self.scraped_urls = []
        self.first_item = True

    def process_request(self, request, spider):
        if self.first_item:
            output_file = getattr(spider, 'output_file', None)
            if not output_file:
                raise ValueError('output_file must be specified')

            with open(output_file, 'r') as f:
                self.scraped_urls = [item["url"] for item in json.load(f)]

            self.first_item = False
        spider.logger.info(f"Checking URL: {request.url}")
        if request.url in self.scraped_urls:
            spider.logger.info(f"Skipping already scraped URL: {request.url}")
            raise IgnoreRequest(f"URL {request.url} already scraped.")
        else:
            return None