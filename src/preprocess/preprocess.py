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
    def preprocess_data(data, ticker):
        """
        Converts raw data into a DataFrame, renames columns, converts datetime, fills missing values,
        and removes negatives from numeric columns.

        Args:
            data (list): Raw data from the API.
            ticker (str): The ticker symbol for the data.

        Returns:
            pd.DataFrame: The cleaned and preprocessed DataFrame.
        """
        df = pd.DataFrame(data)
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
        df['ticker'] = ticker

        # Fill forward and drop rows with remaining NaNs
        df = df.ffill()
        df.dropna(inplace=True)

        # Remove negative values from numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        df[numeric_cols] = df[numeric_cols].clip(lower=0)

        final_columns = ['ticker', 'timestamp'] + numeric_cols
        return df[final_columns]
