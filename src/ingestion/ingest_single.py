import pandas as pd
from datetime import datetime, timedelta
import time
from src.utils.logger import AppLogger
import logging


logger = logging.getLogger(__name__)


class SingleTickerIngestor:
    def __init__(self, angelone_client, clickhouse_client, preprocess_class, table_name):
        self.angelone_client = angelone_client
        self.clickhouse_client = clickhouse_client
        self.preprocess_class = preprocess_class
        self.table_name = table_name

    def ingest(self, ticker_token: str, ticker: str):
        
        print(f"Ingesting data for {ticker}")

        # Get the latest timestamp of existing data from the ClickHouse database
        last_date = self.clickhouse_client.get_last_date_data(ticker)
        # If no data exists or the last date is extremely old, start fetching from a predefined past date
        if last_date is None or last_date.year < 1980:
            last_date = datetime(2016, 1, 1)

        all_dataframes = []  
        empty_chunk_count = 0

        # Set the initial 'to_date' for fetching to the current date
        to_date = datetime.today().date()

        # Loop to fetch historical data in chunks until a certain condition is met
        while empty_chunk_count < 3:
            # Determine the 'from_date' for the current chunk:
            # It's either one day after the last known data point, or 29 days before 'to_date',
            # whichever is later, to fetch data in approximately 30-day chunks.
            from_date = max(last_date.date() + timedelta(days=1), to_date - timedelta(days=29))

            if from_date > to_date:
                print(f"Complete data fetched for {ticker} till {to_date}. Ending loop.")
                break

            print(f"Fetching from {from_date} to {to_date} for {ticker}")

            # Introduce a small delay to respect API rate limits
            time.sleep(0.5)
           
            raw_data = self.angelone_client.get_historical_data(from_date, to_date, ticker_token)

            # Check if raw data was returned and contains actual data points
            if raw_data and raw_data.get('data') is not None and len(raw_data['data']) > 0:
                processed = self.preprocess_class.preprocess_data(raw_data['data'], ticker)
                all_dataframes.append(processed) 
                empty_chunk_count = 0  # Reset empty chunk counter as data was received
            else:
                print("Empty data chunk")
                logger.error(f'Empty data for {ticker} from {from_date} to {to_date}.')
                empty_chunk_count += 1  # Increment empty chunk counter

            # If the 'from_date' is less than or equal to the 'last_date' from DB,
            # it implies we've reached or passed already ingested data
            if from_date <= last_date.date():
                print("Reached known data boundary.")
                break

            # For the next iteration, set the 'to_date' to one day before the current 'from_date'
            to_date = from_date - timedelta(days=1)

        # After the fetching loop, if any dataframes were collected
        if all_dataframes:
            combined_df = pd.concat(all_dataframes)
            combined_df = combined_df.sort_values(by='timestamp')
            self.clickhouse_client.push_data_to_database(self.table_name, combined_df)
            logger.info(f'Data fetched cleaned and pushed for {ticker} in database')
            
        else:
            print(f"No data to insert for {ticker}.")
            logger.error(f"No data to insert for {ticker}.")