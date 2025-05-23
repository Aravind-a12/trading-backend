import requests
import time
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client["binance"]
open_interest_collection = db["open_intrest"]

def fetch_open_interest():
    url = "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=1"
    while True:
        response = requests.get(url).json()
        if response:
            data = response[0]
            oi_data = {
                "timestamp": datetime.utcfromtimestamp(data["timestamp"] / 1000),
                "open_interest": float(data["sumOpenInterest"])
            }
            open_interest_collection.insert_one(oi_data)
            print("Inserted Open Interest:", oi_data)
        time.sleep(300)  

fetch_open_interest()
