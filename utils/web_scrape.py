import asyncio
from crawl4ai import AsyncWebCrawler

async def scrape_website(url: str):
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)
        return result.markdown
    return ""