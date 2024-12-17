from datetime import datetime
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Response

class ScrapyEngineSpiderMiddleware:
    def __init__(self):
        self.scraped_urls = []
        self.first_item = True
        self.base_url = ''
        self.status_codes = {}
        self.last_logged = datetime.now()

    def process_request(self, request, spider):
        if self.scraped_urls == [] and self.first_item:
            self.scraped_urls = getattr(spider, "scraped_urls", None)
            self.base_url = getattr(spider, "base_url", None)
            self.first_item = False

        if request.url in self.scraped_urls and request.url != self.base_url:
            spider.logger.info(f"Skipping already scraped URL: {request.url}")
            raise IgnoreRequest(f"URL {request.url} already scraped.")
        else:
            self.scraped_urls.append(request.url)
            return None

    def process_response(self, request, response: Response, spider):
        if response.status == 200:
            self.scraped_urls.append(response.url)

        self.status_codes[response.status] = self.status_codes.get(response.status, 0) + 1
        setattr(spider, "status_codes", self.status_codes)
        spider.logger.info(f"Crawled ({response.status}) {response.url}")

        return response