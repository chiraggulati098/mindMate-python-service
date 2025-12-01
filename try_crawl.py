import asyncio

from utils.web_scrape import scrape_website

print(asyncio.run(scrape_website("https://www.geeksforgeeks.org/dsa/array-data-structure-guide/")))