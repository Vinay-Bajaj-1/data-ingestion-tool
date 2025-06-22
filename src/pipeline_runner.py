from config.settings import Config
from src.downloader.fetch_local_data import ReadLocalData
from src.downloader.angelone_api_client import AngelOneApiClient
from src.preprocess.preprocess import PreprocessData
from src.ingestion.clickhouse import ClickhouseConnect
from src.ingestion.ingest_single import SingleTickerIngestor


class PipelineRunner:
    def __init__(self, config):
        # Load configuration
        self.config = config()

        # Initialize ClickHouse client
        self.clickhouse_client = ClickhouseConnect(
            host=self.config.CLICKHOUSE_HOST,
            username=self.config.CLICKHOUSE_USERNAME,
            password=self.config.CLICKHOUSE_PASSWORD,
            database=self.config.CLICKHOUSE_DATABASE,
            table_name=self.config.CLICKHOUSE_TABLE
        )

        # Create ingestor for single ticker ingestion
        self.single_ingestor = SingleTickerIngestor(
            self.clickhouse_client, PreprocessData, self.config.CLICKHOUSE_TABLE
        )

        # Determine data source mode
        if self.config.DATA_SOURCE_MODE == "api":
            self._setup_api_client()
        elif self.config.DATA_SOURCE_MODE == "local":
            self.local_data_dir = self.config.LOCAL_DATA_FOLDER
        else:
            raise ValueError(f"Invalid DATA_SOURCE_MODE: {self.config.DATA_SOURCE_MODE}")

    def _setup_api_client(self):
        # Initialize Angel One API client
        self.api_client = AngelOneApiClient(
            api_key=self.config.ANGELONE_API,
            username=self.config.ANGEL_ONE_USER_ID,
            pin=self.config.ANGEL_ONE_PIN,
            token=self.config.ANGEL_ONE_TOKEN,
            scrip_url=self.config.ANGEL_ONE_API_SCRIP_LINK
        )

    def run(self):
        # Execute pipeline based on data source
        if self.config.DATA_SOURCE_MODE == "api":
            self._run_api_mode()
        elif self.config.DATA_SOURCE_MODE == "local":
            self._run_local_mode()

    def _run_api_mode(self):
        # Ingest data from Angel One API
        print("Running API ingestion...")
        scrip_df = self.api_client.get_latest_scrip()

        for _, row in scrip_df.iterrows():
            ticker_token = row['token']
            ticker = row['symbol'].replace("-EQ", "")
            self.single_ingestor.fetch_and_store_single_ticker(self.api_client, ticker_token, ticker)

    def _run_local_mode(self):
        # Ingest data from local CSV files
        for data in ReadLocalData.read_local_data_in_chunks(self.local_data_dir):
            for ticker, df in data.items():
                processed_data = PreprocessData.preprocess_data(df, ticker)
                print(f'Processing {ticker}, shape : {df.shape}')
                self.single_ingestor.insert_data_monthly_chunks(processed_data, ticker)
                print(f'Data Inserted for {ticker}')