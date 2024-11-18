import asyncio
import aiohttp
import logging
from bs4 import BeautifulSoup
from aiohttp.client_exceptions import ClientResponseError


class StockScraper:
    def __init__(self):
        self.urls = [
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=11",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=21",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=31",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=41",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=51",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=61",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=71",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=81",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=91",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=101",
            "https://finviz.com/screener.ashx?v=340&s=ta_topgainers&r=111",
        ]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.stocks = []

    async def fetch(self, session, url, retries=3, delay=3):
        attempt = 0
        while attempt < retries:
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.text()
            except ClientResponseError as e:
                if e.status == 429:
                    retry_after = e.headers.get("Retry-After", delay)
                    logging.warning(f"Rate limited. Retrying after {retry_after} seconds.")
                    await asyncio.sleep(int(retry_after))
                else:
                    raise
            except Exception as e:
                logging.error(f"Error occurred: {e}")
                await asyncio.sleep(delay)
            attempt += 1
        raise Exception(f"Failed to fetch {url} after {retries} retries.")

    async def fetch_all(self):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [self.fetch(session, url) for url in self.urls]
            return await asyncio.gather(*tasks)

    def extract_financial_details(self, soup, stock_data):
        snapshot_table_body = soup.find("table", class_="snapshot-table2 screener_snapshot-table-body")
        if not snapshot_table_body:
            return

        rows = snapshot_table_body.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 4:
                # Left-side value
                key1 = cols[0].text.strip()
                value1 = cols[1].text.strip()
                if key1 in ['Market Cap', 'P/E', 'EPS (ttm)', 'Avg Volume']:
                    stock_data[key1] = value1

                # Right-side value
                key2 = cols[2].text.strip()
                value2 = cols[3].text.strip()
                if key2 in ['Market Cap', 'P/E', 'EPS (ttm)', 'Avg Volume']:
                    stock_data[key2] = value2

    async def parse_stock_data(self, html):
        soup = BeautifulSoup(html, "html.parser")
        screener_content = soup.find("div", id="screener-content")
        if screener_content:
            tables = screener_content.find_all("table", class_="snapshot-table")
            for table in tables:
                stock_data = {}

                # Extract Ticker and Market
                ticker_row = table.find("td", string="Ticker")
                if ticker_row:
                    ticker = ticker_row.find_next_sibling("td").text.strip()
                    stock_data["Ticker"] = ticker
                    market = ticker_row.find_next_sibling("td").text.split("[")[-1].strip("]")
                    stock_data["Market"] = market

                # Extract Company Name
                company_row = table.find("td", string="Company")
                if company_row:
                    company = company_row.find_next_sibling("td").text.strip()
                    stock_data["Company"] = company

                # Extract Industry
                industry_row = table.find("td", string="Industry")
                if industry_row:
                    industry = industry_row.find_next_sibling("td").text.strip()
                    stock_data["Industry"] = industry

                self.extract_financial_details(soup, stock_data)
                self.stocks.append(stock_data)

    async def scrape(self):
        html_pages = await self.fetch_all()
        tasks = [self.parse_stock_data(html) for html in html_pages]
        await asyncio.gather(*tasks)

    async def add_additional_data(self):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            logging.info('Getting additional informations about stocks')
            for stock in self.stocks:
                ticker = stock.get("Ticker", "").split(" [")[0]
                url = f"https://finviz.com/quote.ashx?t={ticker}&ty=c&p=d&b=1"

                try:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        html = await response.text()
                        soup = BeautifulSoup(html, "html.parser")

                        # Extract ATR (14)
                        atr_label_td = soup.find("td", string="ATR (14)")
                        if atr_label_td:
                            atr_value_td = atr_label_td.find_next_sibling("td", class_="snapshot-td2 w-[8%]")
                            if atr_value_td:
                                stock["ATR (14)"] = atr_value_td.find("b").text.strip()

                        # Extract Sector
                        sector_tag = soup.find("a", class_="tab-link", href=lambda x: x and "sec_" in x)
                        if sector_tag:
                            stock["Sector"] = sector_tag.text.strip()
                except ClientResponseError as e:
                    if e.status == 429:
                        logging.warning(f"Rate limit hit for {url}. Retrying after a delay...")
                        await asyncio.sleep(3)
                        continue
                    else:
                        logging.error(f"HTTP error {e.status} for {url}: {e.message}")
                except Exception as e:
                    logging.error(f"An error occurred for {ticker}: {e}")
    
    async def get_stocks(self):
        await self.scrape()
        await self.add_additional_data()
        return self.stocks
