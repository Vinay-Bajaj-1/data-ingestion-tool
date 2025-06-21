import os
from dotenv import load_dotenv
import pandas as pd
from src.downloader.fetch_local_data import ReadLocalData
from src.downloader.angelone_api_client import AngelOneApiClient
from src.preprocess.preprocess import PreprocessData
from src.ingestion.clickhouse import ClickhouseConnect
from src.ingestion.ingest_single import SingleTickerIngestor
from src.utils.logger import AppLogger

logger = AppLogger.get_logger(__name__)

class PipelineRunner:
    def __init__(self):
        load_dotenv()

        self.table_name = os.getenv("CLICKHOUSE_TABLE")
        self.mode = os.getenv("DATA_SOURCE_MODE", "api").lower()

        self.clickhouse_client = ClickhouseConnect(
            host=os.getenv("CLICKHOUSE_HOST"),
            username=os.getenv("CLICKHOUSE_USERNAME"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            database=os.getenv("CLICKHOUSE_DATABASE"),
            table_name=self.table_name
        )
        self.single_ingestor = SingleTickerIngestor(
            self.clickhouse_client, PreprocessData, self.table_name
        )

        self.local_data_dir = os.getenv('LOCAL_DATA_FOLDER')

        if self.mode == "api":
            self._setup_api_client()
        elif self.mode == 'local':
            pass
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

    def run(self):
        if self.mode == "api":
            self._run_api_mode()
        elif self.mode == "local":
            print('Calling Local')
            self._run_local_mode()

    def _run_api_mode(self):
        print("Running API ingestion...")
        scrip_df = self.api_client.get_latest_scrip()
        

        for _, row in scrip_df.iterrows():
            ticker_token = row['token']
            ticker = row['symbol'].replace("-EQ", "")
            self.single_ingestor.ingest(self.api_client, ticker_token, ticker)
            

    def _run_local_mode(self):
        for data in ReadLocalData.read_local_data_in_chunks('local_data'):
            for ticker, df in data.items():
                processed_data = PreprocessData.preprocess_data(df, ticker)
                print(f'Processing {ticker}, shape : {df.shape}')
                self.single_ingestor.ingest_in_chunks(processed_data, ticker)
                print(f'Data Inserted for {ticker}')
