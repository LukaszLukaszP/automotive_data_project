import scrapy
import os
from scrapy_playwright.page import PageMethod

class OtomotoRawSpider(scrapy.Spider):
    name = "otomoto_raw"
    MAX_PAGES = 5  # how many pages to fetch

    def start_requests(self):
        self.logger.info(f"→ Starting to crawl {self.MAX_PAGES} pages with Playwright")
        """
        Generate initial requests for pages 1..MAX_PAGES.
        """
        #base = "https://www.otomoto.pl/osobowe?search%5Border%5D=created_at_first%3Adesc&page={}"
        base = "https://www.otomoto.pl/osobowe/?search[order]=created_at_first:desc&page={}"
        for page in range(1, self.MAX_PAGES + 1):
            yield scrapy.Request(
                url=base.format(page),
                callback=self.parse,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "p.ooa-oj1jk2")
                    ],
                    "page": page
                },
            )

    def parse(self, response):
        """
        Save the raw HTML of each listing page to raw_data/page_{n}.html.
        """
        page = response.meta['page']
        filename = f"page_{page}.html"
        # ensure ../raw_data exists
        os.makedirs(os.path.join('..', 'raw_data'), exist_ok=True)
        path = os.path.join('..', 'raw_data', filename)
        with open(path, 'wb') as f:
            f.write(response.body)
        self.log(f"Saved raw HTML to {path}")