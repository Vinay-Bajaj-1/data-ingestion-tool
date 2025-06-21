import os
import pandas as pd
from src.utils.logger import AppLogger

logger = AppLogger.get_logger()



class ReadLocalData:
    @staticmethod
    def read_local_data_in_chunks(directory_name, chunk_size=5):
        """
        Ensures a specified directory exists and yields DataFrames from CSV files in chunks.
        This is a generator function to avoid loading all files into memory at once.

        Args:
            directory_name (str): The name of the directory (relative to current working directory).
            chunk_size (int): The number of CSV files to process in each chunk.

        Yields:
            tuple: (dict: {filename_without_ext: pandas.DataFrame}) for each chunk.
        """
        base_path = os.getcwd()
        directory_path = os.path.join(base_path, directory_name)

        if not os.path.exists(directory_path):
            try:
                os.makedirs(directory_path)
                logger.info(f"Directory created at: {directory_path}")
            except Exception as e:
                logger.error(f"Failed to create directory: {directory_path}, Error: {e}")
                return # Exit generator

        csv_files = [f for f in os.listdir(directory_path) if f.endswith(".csv")]
        logger.info(f'Found {len(csv_files)} CSV files in {directory_path}')

        for i in range(0, len(csv_files), chunk_size):
            current_chunk_files = csv_files[i:i + chunk_size]
            chunk_data = {}
            logger.info(f"Processing chunk {int(i/chunk_size) + 1} with {len(current_chunk_files)} files.")
            
            for filename in current_chunk_files:
                file_path = os.path.join(directory_path, filename)
                try:
                    df = pd.read_csv(file_path)
                    key = os.path.splitext(filename)[0]
                    chunk_data[key] = df
                    logger.info(f"Loaded '{key}' ({df.shape[0]} rows, {df.shape[1]} columns) into current chunk.")
                except Exception as e:
                    logger.error(f"Error reading {filename}: {e}")
            
            if chunk_data: # Yield only if there's data in the chunk
                yield chunk_data
            else:
                logger.warning(f"Chunk {int(i/chunk_size) + 1} yielded no data (e.g., due to read errors).")
