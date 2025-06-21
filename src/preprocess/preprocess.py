import pandas as pd
from src.utils.logger import AppLogger

logger = AppLogger.get_logger(__name__)

class PreprocessData:
    """
    Handles preprocessing of raw financial data.
    """

    def __init__(self):
        pass
    
    @staticmethod
    def preprocess_data(data,ticker):
        """
        Converts raw data into a DataFrame, renames columns, converts datetime, and sorts.

        Args:
            data (list): Raw data from the API.

        Returns:
            pd.DataFrame: The preprocessed DataFrame.
        """
        df = pd.DataFrame(data)
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
        df['ticker'] = ticker
        final_columns = ['ticker', 'timestamp', 'open', 'high', 'low', 'close', 'volume']

        df = df.ffill()
        df.dropna(inplace=True)
        return df[final_columns]