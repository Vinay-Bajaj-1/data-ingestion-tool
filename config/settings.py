import os
from dotenv import load_dotenv

class Config:
    def __init__(self):
        load_dotenv()

        # Angel One API Credentials
        self.ANGELONE_API = os.getenv("ANGELONE_API") 
        self.ANGELONE_SECRET_KEY = os.getenv("ANGELONE_SECRET_KEY")
        self.ANGEL_ONE_PASSWORD = os.getenv("ANGEL_ONE_PASSWORD")
        self.ANGEL_ONE_TOKEN = os.getenv("ANGEL_ONE_TOKEN")
        self.ANGEL_ONE_USER_ID = os.getenv("ANGEL_ONE_USER_ID")
        self.ANGEL_ONE_PIN = os.getenv("ANGEL_ONE_PIN")
        self.ANGEL_ONE_API_SCRIP_LINK = os.getenv("ANGEL_ONE_API_SCRIP_LINK")

        # Local / API Mode
        self.DATA_SOURCE_MODE = os.getenv("DATA_SOURCE_MODE", "api").lower()

        # ClickHouse Config
        self.CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE")
        self.CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
        self.CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME")
        self.CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
        self.CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")

        # Local Data Path
        self.LOCAL_DATA_FOLDER = os.getenv("LOCAL_DATA_FOLDER")
