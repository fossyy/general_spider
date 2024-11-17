from scrapy.exceptions import IgnoreRequest

class ScrapyEngineSpiderMiddleware:
    def __init__(self):
        self.scraped_urls = []
        self.first_item = True

    def process_request(self, request, spider):
        if self.scraped_urls == [] and self.first_item:
            self.scraped_urls = getattr(spider, "scraped_urls", None)
            self.first_item = False

        if request.url in self.scraped_urls:
            spider.logger.info(f"Skipping already scraped URL: {request.url}")
            raise IgnoreRequest(f"URL {request.url} already scraped.")
        else:
            self.scraped_urls.append(request.url)
            return None