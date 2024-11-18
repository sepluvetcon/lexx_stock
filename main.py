import os
import asyncio
import logging
from dotenv import load_dotenv

from csv_manager import CSVManager
from tg_client import TelegramClient
from finviz_client import StockScraper


load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("logs.log", mode="w")
file_handler.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


async def main():
    scraper = StockScraper()
    stocks = await scraper.get_stocks()

    csv_manager = CSVManager(stocks)
    updated_stocks = csv_manager.get_stocks()
    
    telegram_client = TelegramClient(
        os.getenv('BOT_TOKEN'), os.getenv('CHANNEL_ID')
    )
    telegram_client.send_stock_info(updated_stocks)

if __name__ == "__main__":
    asyncio.run(main())