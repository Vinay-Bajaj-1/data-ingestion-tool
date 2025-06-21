import pandas as pd
import requests
from SmartApi import SmartConnect
import pyotp


from src.utils.logger import AppLogger

logger = AppLogger.get_logger()


class AngelOneApiClient:
    """
    A client class to interact with the Angel One SmartAPI.
    It handles authentication, historical data fetching, and scrip master data retrieval.
    """

    def __init__(self, api_key, username, pin, token, scrip_url):
        """
        Initializes the AngelOneApiClient and establishes a session with SmartAPI.

        Args:
            api_key (str): Your Angel One SmartAPI Key.
            username (str): Your Angel One Client ID.
            pin (str): Your Angel One 4-digit MPIN.
            secret_key (str): The base32-encoded secret key obtained from SmartAPI portal
                                for generating TOTPs.
            scrip_url (str): The URL to fetch the latest scrip master data from Angel One.
        """
        self.api_key = api_key
        self.token = token
        self.username = username
        self.pin = pin
        self.scrip_url = scrip_url 

        try:
            # Generate the current Time-Based One-Time Password (TOTP)
            self._totp = pyotp.TOTP(self.token).now()
            logger.info(f"TOTP generated successfully for user {self.username}.")
        except Exception as e:
            logger.error(f"Error generating TOTP: {e}. Please check your TOTP secret key.")
            raise

        # Initialize SmartConnect with the API key
        self.smartApi = SmartConnect(self.api_key)

        try:
            # Generate a session using client ID, MPIN, and TOTP
            # This step authenticates the user and retrieves JWT, refresh token, etc.
            response = self.smartApi.generateSession(self.username, self.pin, self._totp)
            
            if response and response.get('status'):
                self.auth_token = response['data']['jwtToken']
                self.refresh_token = response['data']['refreshToken']
                self.feed_token = self.smartApi.getfeedToken() # Get feed token for market data
                logger.info("SmartAPI session generated successfully.")
                logger.info(f"Auth Token: {self.auth_token[:10]}...") # Log partial token for security
            else:
                error_message = response.get('message', 'Unknown error during session generation.')
                error_code = response.get('errorcode', 'N/A')
                logger.error(f"Failed to generate SmartAPI session. Error Code: {error_code}, Message: {error_message}")
                raise Exception(f"SmartAPI Session Error: {error_message} (Code: {error_code})")

        except Exception as e:
            logger.error(f"An unexpected error occurred during SmartAPI session setup: {e}")
            raise

    def get_historical_data(self, from_date, to_date, symbol_token):
        """
        Fetches historical candle data for a given symbol token.

        Args:
            from_date (datetime.datetime): The start date and time for the historical data.
                                           Format will be adjusted to 'YYYY-MM-DD HH:MM'.
            to_date (datetime.datetime): The end date and time for the historical data.
                                         Format will be adjusted to 'YYYY-MM-DD HH:MM'.
            symbol_token (str): The unique token for the symbol (e.g., from scrip master).

        Returns:
            dict: A dictionary containing the historical candle data.
                  Returns None if there's an API error or no data.
        """
        historicParam = {
            "exchange": "NSE",  # The exchange being queried (fixed to NSE)
            "symboltoken": symbol_token,  # The unique token for the instrument
            "interval": "ONE_MINUTE",  # The interval for the data (e.g., 'ONE_MINUTE', 'FIFTEEN_MINUTE', 'DAY')
            # Format dates to 'YYYY-MM-DD HH:MM' strings as required by the API
            "fromdate": from_date.strftime('%Y-%m-%d 09:15'),
            "todate": to_date.strftime('%Y-%m-%d 15:29')
        }
        
        logger.info(f"Fetching historical data for token {symbol_token} from {historicParam['fromdate']} to {historicParam['todate']}.")
        
        try:
            # Use the initialized smartApi object to call getCandleData
            res = self.smartApi.getCandleData(historicParam)
            if res and res.get('status'):
                logger.info(f"Successfully fetched historical data for {symbol_token}.")
                return res
            else:
                error_message = res.get('message', 'Unknown error fetching historical data.')
                error_code = res.get('errorcode', 'N/A')
                logger.warning(f"Failed to fetch historical data for {symbol_token}. Error: {error_message} (Code: {error_code})")
                return None
        except Exception as e:
            logger.error(f"An error occurred while fetching historical data: {e}")
            return None

    def get_latest_scrip(self):
        """
        Fetches the latest scrip master data from the configured URL, filters it
        for tradable NSE Equity instruments, and saves it to a CSV file.

        Returns:
            pandas.DataFrame: A DataFrame containing the filtered tradable scrip instruments.
                              Returns an empty DataFrame if fetching or filtering fails.
        """
        logger.info(f"Attempting to fetch scrip master data from: {self.scrip_url}")
        
        
        # Make an HTTP GET request to the scrip URL
        response = requests.get(self.scrip_url)
        data = response.json() 
        scrip = pd.DataFrame(data)
        logger.info(f"Successfully fetched {len(scrip)} entries from scrip master.")

        # Filter for NSE Equity data only based on specified criteria
        filtered_scrip = scrip[
            (scrip["exch_seg"] == "NSE") &  # Exchange segment must be NSE
            (scrip["lotsize"].astype(str) == "1") &  # Lot size should be exactly 1 for equity
            (scrip["symbol"].str.endswith("-EQ")) &  # Symbol should end with '-EQ' to identify equity
            (~scrip["symbol"].str.contains("NSETEST", na=False)) &  # Exclude test symbols
            # Instrument type should be null, empty, OR if it's not null/empty,
            # the 'name' should not end with "ETF" to filter out ETFs
            (scrip["instrumenttype"].isna() | 
                (scrip["instrumenttype"] == "") | 
                (~scrip["name"].str.endswith("ETF", na=False)))
        ].sort_values(by='symbol') # Sort by symbol for consistency

        # Save the filtered scrip data to a CSV file
        output_filename = "tradable_instrument.csv"
        filtered_scrip.to_csv(output_filename, index=False)
        logger.info(f"Filtered {len(filtered_scrip)} tradable instruments and saved to {output_filename}.")
        
        return filtered_scrip
