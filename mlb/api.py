import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("API_KEY")
BASE_URL = os.environ.get("BASE_URL")


def fetch_odds(sport: str, markets: str) -> dict:
    url = f"{BASE_URL}/{sport}/odds"
    resp = requests.get(url, params={
        "apiKey":     API_KEY,
        "regions":    "us",
        "markets":    markets,
        "oddsFormat": "american",
    })
    resp.raise_for_status()
    return resp.json()
