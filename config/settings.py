import os
import json
from dotenv import load_dotenv 

class Config:

    def __init__(self):
        load_dotenv()

        # -- Angel One Api Credentials -- 
        self.ANGELONE_API = os.getenv("ANGELONE_API") 
        self.ANGELONE_SECRET_KEY = os.getenv("ANGELONE_SECRET_KEY")
        self.ANGEL_ONE_PASSWORD = os.getenv("ANGEL_ONE_PASSWORD")
        self.ANGEL_ONE_TOKEN = os.getenv("ANGEL_ONE_TOKEN")
        self.ANGEL_ONE_USER_ID = os.getenv("ANGEL_ONE_USER_ID")
        self.ANGEL_ONE_PIN = os.getenv("ANGEL_ONE_PIN")
        self.ANGEL_ONE_API_SCRIP_LINK = os.getenv('ANGEL_ONE_API_SCRIP_LINK')
        pass