import json
import os
from scrapy.exceptions import IgnoreRequest

class FilterRequestsMiddleware:
    scraped_urls = []

    def __init__(self):
        if os.path.exists('output.json'):
            with open('output.json', 'r') as f:
                try:
                    data = json.load(f)
                    self.scraped_urls = [item['url'] for item in data]
                except json.JSONDecodeError:
                    print("Could not load output.json as JSON.")

    def process_request(self, request, spider):
        spider.logger.info(f"Checking URL: {request.url}")
        if request.url in self.scraped_urls:
            spider.logger.info(f"Skipping already scraped URL: {request.url}")
            raise IgnoreRequest(f"URL {request.url} already scraped.")
        else:
            return None
