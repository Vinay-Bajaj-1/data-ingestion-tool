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
    def preprocess_data(data):
        """
        Converts raw data into a DataFrame, renames columns, converts datetime, and sorts.

        Args:
            data (list): Raw data from the API.

        Returns:
            pd.DataFrame: The preprocessed DataFrame.
        """
        df = pd.DataFrame(data)
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        df.sort_values(by = 'datetime', inplace = True)
        
        logger.info("Data preprocessed successfully.")
        return df