import os
from dotenv import load_dotenv
import pandas as pd
from src.downloader.fetch_local_data import ReadLocalData
from src.downloader.angelone_api_client import AngelOneApiClient
from src.preprocess.preprocess import PreprocessData
from src.ingestion.clickhouse import ClickhouseConnect
from src.ingestion.ingest_single import SingleTickerIngestor

class PipelineRunner:
    def __init__(self):
        load_dotenv()

        self.table_name = os.getenv("CLICKHOuse_TABLE")
        self.mode = os.getenv("DATA_SOURCE_MODE", "api").lower()

        self.clickhouse_client = ClickhouseConnect(
            host=os.getenv("CLICKHOUSE_HOST"),
            username=os.getenv("CLICKHOUSE_USERNAME"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database=os.getenv("CLICKHOUSE_DATABASE"),
            table_name=self.table_name
        )

        if self.mode == "api":
            self._setup_api_client()
        elif self.mode == "local":
            self._setup_local_data()
        else:
            raise ValueError(f"Invalid DATA_SOURCE_MODE: {self.mode}")

    def _setup_api_client(self):
        self.api_client = AngelOneApiClient(
            api_key=os.getenv("ANGELONE_API"),
            user_id=os.getenv("ANGEL_ONE_USER_ID"),
            mpin=os.getenv("ANGEL_ONE_PIN"),
            access_token=os.getenv("ANGEL_ONE_TOKEN"),
            url=os.getenv("ANGEL_ONE_API_SCRIP_LINK")
        )

    def _setup_local_data(self):
        self.local_data = ReadLocalData.read_local_data('local_data')

    def run(self):
        if self.mode == "api":
            self._run_api_mode()
        elif self.mode == "local":
            print('Calling Local')
            self._run_local_mode()

    def _run_api_mode(self):
        print("Running API ingestion...")
        scrip_df = self.api_client.get_latest_scrip()
        single_ingestor = SingleTickerIngestor(
            self.api_client, self.clickhouse_client, PreprocessData, self.table_name
        )

        for _, row in scrip_df.iterrows():
            ticker_token = row['token']
            ticker = row['symbol'].replace("-EQ", "")
            single_ingestor.ingest(ticker_token, ticker)
            

    def _run_local_mode(self):
        print("Restoring from local CSV backup...")
        for ticker, df in self.local_data.items():
            processed = PreprocessData.preprocess_data(df, ticker)
            processed.sort_values(by='timestamp', inplace=True)
            processed['partition'] = processed['timestamp'].dt.to_period('M')

            for _, chunk in processed.groupby('partition'):
                chunk.drop(columns='partition', inplace=True)
                self.clickhouse_client.push_data_to_database(self.table_name, chunk)

            print(f"Inserted backup data for {ticker}")
            
