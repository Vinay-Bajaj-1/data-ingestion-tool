from clickhouse_connect import get_client
from datetime import datetime
import logging


logger = logging.getLogger(__name__)

class ClickhouseConnect:

    def __init__(self, host, username, password, database, table_name):
        
        self.client = get_client(host=host, username=username, password=password, database=database)
        self.sql_mapping = {
            'create_table': 'src/ingestion/query/create_table.sql',
            'latest_timestamp': 'src/ingestion/query/latest_timestamp.sql',
            'validate_table': 'src/ingestion/query/validate_table.sql',
            'table_exists': 'src/ingestion/query/table_exists.sql',
        }
        self.create_table_from_sql(table_name)

    def table_exists(self, table_name):
        """
        Checks if a table exists in the connected ClickHouse database.

        Args:
            table_name (str): The table name to check.
            sql_file_path (str): Path to the SQL file.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            with open(self.sql_mapping['table_exists'], 'r') as file:
                query_template = file.read()

            query = query_template.format(
                database=self.client.database,
                table_name=table_name
            )

            result = self.client.query(query)
            count = result.result_rows[0][0]
            return count > 0

        except Exception as e:
            print(f"Error checking if table exists: {e}")
            return False


    def create_table_from_sql(self, table_name):
        """
        Creates a ClickHouse table by reading a SQL template file and executing the query.

        Args:
            table_name (str): The name of the table to create.
            sql_file_path (str): Path to the .sql file containing the CREATE TABLE statement.
        """
        try:
            with open(self.sql_mapping['create_table'], 'r') as f:
                sql_template = f.read()
            
            create_query = sql_template.format(table_name=table_name)
            if not self.table_exists(table_name):
                self.client.command(create_query)
                logger.info('Table Created')
                print(f"Table '{table_name}' created successfully.")
        
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")


    def get_last_date_data(self, ticker):
        """
        Fetches the latest timestamp for a given ticker by reading and executing a SQL file.

        Args:
            ticker (str): The stock ticker symbol.
            sql_file_path (str): Path to the SQL file.

        Returns:
            datetime or None: Latest timestamp or None if not found.
        """
                    # Load SQL query from file
        with open(self.sql_mapping['latest_timestamp'], 'r') as file:
            query_template = file.read()

        query = query_template.format(ticker=ticker)

        result = self.client.query(query)
        latest_ts = result.result_rows[0][0] if result.result_rows else None

        # Handle default epoch case
        if latest_ts is not None and latest_ts == datetime(1970, 1, 1):
            return None

        return latest_ts


    def validate_table(self, table_name, dataframe):
        """
        Validates if the columns in the ClickHouse table match those in the DataFrame.

        Args:
            table_name (str): The ClickHouse table name.
            dataframe (pd.DataFrame): The DataFrame to validate.

        Returns:
            bool: True if column names match, False otherwise.
        """
        try:
            # Read SQL template from file
            with open(self.sql_mapping['validate_table'], 'r') as file:
                query_template = file.read()

            # Replace placeholders
            query = query_template.format(
                database=self.client.database,
                table_name=table_name
            )

            # Execute query
            result = self.client.query(query)
            table_columns = [row[0] for row in result.result_rows]

            # Normalize and compare column names
            df_columns = list(dataframe.columns)
            if table_columns == df_columns:
                return True
            else:
                print(f"Column mismatch:\nDB: {table_columns}\nDF: {df_columns}")
                return False

        except Exception as e:
            print(f"Validation error: {e}")
            return False



    def push_data_to_database(self, table_name,dataframe):
        """
        Pushes the provided DataFrame to the 'stock_ohlcv' table in ClickHouse.

        Args:
            dataframe (pd.DataFrame): The data to insert. Expected to have columns:
                ['ticker', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
        """
        try:
            if self.validate_table(table_name, dataframe):
                self.client.insert_df('stock_ohlcv', dataframe)
                logger.info("Data successfully inserted into ClickHouse.")
            else:
                logger.error("Data not inserted into ClickHouse. Kindly check dataframe columns and table columns")

        except Exception as e:
            logger.error(f"Failed to insert data into ClickHouse: {e}")