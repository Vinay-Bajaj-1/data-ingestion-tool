import pandas as pd
from datetime import datetime, timedelta
import time

class SingleTickerIngestor:
    def __init__(self, angelone_client, clickhouse_client, preprocess_class, table_name):
        self.angelone_client = angelone_client
        self.clickhouse_client = clickhouse_client
        self.preprocess_class = preprocess_class
        self.table_name = table_name

    def ingest(self, ticker_token: str, ticker: str):
        print(f"Ingesting data for {ticker}")

        # Get the latest timestamp from ClickHouse
        last_date = self.clickhouse_client.get_last_date_data(ticker)
        if last_date is None or last_date.year < 1980:
            last_date = datetime(2025, 1, 1)

        all_dataframes = []
        empty_chunk_count = 0

        to_date = datetime.today().date()

        while empty_chunk_count < 3:
            from_date = max(last_date.date() + timedelta(days=1), to_date - timedelta(days=29))

            if from_date > to_date:
                print(f"from_date ({from_date}) is after to_date ({to_date}). Ending loop.")
                break

            print(f"Fetching from {from_date} to {to_date}")

            time.sleep(0.5)
            raw_data = self.angelone_client.get_historical_data(from_date, to_date, ticker_token)

            if raw_data and raw_data.get('data') is not None and len(raw_data['data']) > 0:
                processed = self.preprocess_class.preprocess_data(raw_data['data'], ticker)
                all_dataframes.append(processed)
                empty_chunk_count = 0
            else:
                print("Empty data chunk")
                empty_chunk_count += 1

            if from_date <= last_date.date():
                print("Reached known data boundary.")
                break

            to_date = from_date - timedelta(days=1)

        if all_dataframes:
            combined_df = pd.concat(all_dataframes)
            combined_df = combined_df.sort_values(by='timestamp')
            self.clickhouse_client.push_data_to_database(self.table_name, combined_df)
            
        else:
            print(f"No data to insert for {ticker}.")