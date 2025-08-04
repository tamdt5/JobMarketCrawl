import scrapy
import re
from scrapy.http import Request

class VieclamtotSpider(scrapy.Spider):
    name = 'vieclamtot'
    allowed_domains = ['vieclamtot.com', 'gateway.chotot.com']
    start_urls = [f'https://vieclamtot.com/viec-lam-ban-hang-sdjt2?page={page}' for page in range(1, 4)]
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'HTTPERROR_ALLOWED_CODES': [403],
        'LOG_LEVEL': 'DEBUG',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy_cloudflare_middleware.middlewares.CloudFlareMiddleware': 560,
        }
    }

    async def parse(self, response):
        """Xử lý trang danh sách."""
        self.logger.debug(f"Processing page: {response.url}")
        if response.status == 403:
            self.logger.warning(f"Received 403 for {response.url}. Response: {response.text}")
            return

        # Lấy danh sách URL bài đăng
        post_urls = response.css('div.job-item a::attr(href)').getall()
        self.logger.debug(f"Found {len(post_urls)} post URLs on {response.url}")
        
        if not post_urls:
            self.logger.warning(f"No post URLs found on {response.url}. Check selector or HTML structure.")

        for post_url in post_urls:
            post_url = response.urljoin(post_url)
            self.logger.debug(f"Yielding request for post: {post_url}")
            yield Request(
                post_url,
                callback=self.parse_post,
                errback=self.handle_error,
                headers={'Referer': 'https://vieclamtot.com/'}
            )

    async def parse_post(self, response):
        """Xử lý trang bài đăng."""
        self.logger.debug(f"Processing post: {response.url}")
        if response.status == 403:
            self.logger.warning(f"Received 403 for {response.url}. Response: {response.text}")
            return

        # Trích xuất dữ liệu từ trang bài đăng
        yield {
            'title': response.css('h1::text').get(),
            'salary': response.css('span.salary::text').get(),
            'location': response.css('div.location::text').get(),
            'posted_date': response.css('div.posted-date::text').get(),
            'requirements': response.css('div.requirements::text').getall(),
        }

        # Trích xuất post_id từ URL bài đăng
        post_id = self.extract_post_id(response.url)
        if post_id:
            self.logger.debug(f"Extracted post_id: {post_id}")
            api_url = f"https://gateway.chotot.com/v2/public/ad-listing/{post_id}"
            yield Request(
                api_url,
                callback=lambda response, post_url=response.url: self.parse_api(response, post_url),
                errback=self.handle_error,
                headers={'Referer': 'https://vieclamtot.com/'}
            )
        else:
            self.logger.warning(f"Could not extract post_id from {response.url}")

    def extract_post_id(self, url):
        """
        Trích xuất post_id từ URL bài đăng.
        URL dạng: https://vieclamtot.com/<khu-vuc>/<post_id>
        """
        match = re.search(r'/(\d+)$', url)
        return match.group(1) if match else None

    async def parse_api(self, response, post_url):
        """Xử lý phản hồi từ API."""
        self.logger.debug(f"Processing API response: {response.url}")
        if response.status == 403:
            self.logger.warning(f"Received 403 for {response.url}. Response: {response.text}")
            return

        try:
            data = response.json()
            self.logger.debug(f"API data: {data}")
            yield {
                'post_id': self.extract_post_id(post_url),
                'post_url': post_url,
                'api_data': data
            }
        except ValueError:
            self.logger.error(f"Invalid JSON response from {response.url}")

    async def handle_error(self, failure):
        """Xử lý lỗi yêu cầu."""
        self.logger.error(f"Request failed: {failure}")