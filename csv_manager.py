import csv
import logging
import pandas as pd


class CSVManager:
    def __init__(self, stocks, csv_directory="1m/29.11.2021"):
        self.stocks = stocks
        self.csv_directory = csv_directory
        self.csv_file_path = "stocks_data_with_premarket.csv"
        self.header = [
            'Ticker', 'Market', 'Company', 'Industry', 'Market Cap', 'EPS (ttm)', 'P/E', 'Avg Volume', 
            'ATR (14)', 'Sector', 'PreMarket High', 'PreMarket Low', 'PreMarket Volume'
        ]
    
    def add_premarket_data(self):
        """Adds pre-market data to the stock dictionary."""
        for stock in self.stocks:
            ticker_with_market = stock['Ticker']
            ticker = ticker_with_market.split(' [')[0]
            
            try:
                # Read the corresponding CSV file for the stock
                df = pd.read_csv(f"{self.csv_directory}/{ticker}.csv", sep=";")
                df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d.%m.%Y %H:%M:%S-%f')

                # Filter for PreMarket (04:00 - 09:30)
                pre_market_data = df[(df['Datetime'].dt.hour >= 4) & (df['Datetime'].dt.hour < 9) | 
                                      ((df['Datetime'].dt.hour == 9) & (df['Datetime'].dt.minute <= 30))]

                # Calculate PreMarket High, Low, Volume
                pre_market_high = pre_market_data['High'].max()
                pre_market_low = pre_market_data['Low'].min()
                pre_market_volume = pre_market_data['Volume'].sum()

                # Add PreMarket data to the stock dictionary
                stock["PreMarket High"] = pre_market_high
                stock["PreMarket Low"] = pre_market_low
                stock["PreMarket Volume"] = pre_market_volume

            except FileNotFoundError:
                logging.warning(f"CSV file for {ticker} not found!")
    
    def save_to_csv(self):
        with open(self.csv_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.header)
            writer.writeheader()
            for stock in self.stocks:
                writer.writerow(stock)
        logging.info(f"Stock data with PreMarket information has been written to {self.csv_file_path}")
    
    def get_stocks(self):
        self.add_premarket_data()
        self.save_to_csv()
        return self.stocks