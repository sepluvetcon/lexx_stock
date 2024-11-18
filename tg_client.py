import time
import logging
import requests


class TelegramClient:
    def __init__(self, bot_token, channel_id):
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
    
    def send_message(self, message):

        payload = {
            "chat_id": self.channel_id,
            "text": message,
            "parse_mode": "HTML"
        }

        while True:
            try:
                response = requests.post(self.base_url, data=payload).json()
                if response.get("ok"):
                    logging.info("Message sent successfully.")
                    break
                elif response.get("error_code") == 429:
                    # Rate limited: retry after the specified time
                    retry_after = response["parameters"]["retry_after"]
                    logging.warning(f"Rate limited. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                else:
                    logging.error(f"Failed to send message: {response}")
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f"An error occurred while sending the message: {e}")
                break

    def send_stock_info(self, stocks):
        """Send the information of each stock to the Telegram channel."""
        for stock in stocks:
            stock_message = (
                f"<b>Ticker:</b> {stock.get('Ticker', 'N/A')}\n"
                f"<b>Market:</b> {stock.get('Market', 'N/A')}\n"
                f"<b>Company:</b> {stock.get('Company', 'N/A')}\n"
                f"<b>Industry:</b> {stock.get('Industry', 'N/A')}\n"
                f"<b>Market Cap:</b> {stock.get('Market Cap', 'N/A')}\n"
                f"<b>EPS (ttm):</b> {stock.get('EPS (ttm)', 'N/A')}\n"
                f"<b>P/E:</b> {stock.get('P/E', 'N/A')}\n"
                f"<b>Avg Volume:</b> {stock.get('Avg Volume', 'N/A')}\n"
                f"<b>ATR (14):</b> {stock.get('ATR (14)', 'N/A')}\n"
                f"<b>Sector:</b> {stock.get('Sector', 'N/A')}\n"
                f"<b>PreMarket High:</b> {stock.get('PreMarket High', 'N/A')}\n"
                f"<b>PreMarket Low:</b> {stock.get('PreMarket Low', 'N/A')}\n"
                f"<b>PreMarket Volume:</b> {stock.get('PreMarket Volume', 'N/A')}\n"
            )
            self.send_message(stock_message)
