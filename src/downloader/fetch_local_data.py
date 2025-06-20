import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ReadLocalData:
    @staticmethod

    def read_local_data(directory_name):
        """
        Ensures a specified directory exists outside the project folder and reads all CSV files from it.
        Returns a dictionary where keys are filenames (without extension) and values are pandas DataFrames.

        Args:
            directory_name (str): The name of the directory (expected one level above the current directory).

        Returns:
            dict: Dictionary of filename -> DataFrame. Returns an empty dictionary if no CSVs are present.
        """
        # Get absolute path one level above current directory
        base_path = os.getcwd() 
        directory_path = os.path.join(base_path, directory_name)

        # Create the directory if it doesn't exist
        if not os.path.exists(directory_path):
            try:
                os.makedirs(directory_path)
                logger.info(f"Directory created at: {directory_path}")
            except Exception as e:
                logger.error(f"Failed to create directory: {directory_path}, Error: {e}")
                return {}

        # Read CSV files in the directory
        data = {}
        for filename in os.listdir(directory_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(directory_path, filename)
                try:
                    df = pd.read_csv(file_path)
                    key = os.path.splitext(filename)[0]
                    data[key] = df
                except Exception as e:
                    logger.error(f"Error reading {filename}: {e}")

        return data