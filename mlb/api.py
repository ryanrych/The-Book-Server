import os
import json
from datetime import datetime

import requests
from dotenv import load_dotenv


load_dotenv()
key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")


def get_games_json(date: datetime):

    url = base_url + "/historical/sports/baseball_mlb/odds"
    params = {
        "apiKey": key,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "date": date.isoformat() + "Z"
    }

    response = requests.get(url, params=params)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise Exception(f"Request failed: {response.status_code} - {response.text}") from e

    data = response.json()
    games = data["data"]

    for game in games:

        url = base_url + f"/historical/sports/baseball_mlb/events/{game["id"]}/odds"
        params = {
            "apiKey": key,
            "regions": "us",
            "markets": "team_totals",
            "oddsFormat": "american",
            "date": date.isoformat() + "Z"
        }

        dk_index = None
        for i, bookmaker in enumerate(game["bookmakers"]):
            if bookmaker["key"] == "draftkings":
                dk_index = i
                break

        if dk_index is None:
            return {
                "Error": f"{game["away_team"]} @ {game["home_team"]} {game["commence_time"]} not found on Draft Kings"
            }

        inner_response = requests.get(url, params=params)

        try:
            inner_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"Request failed: {inner_response.status_code} - {inner_response.text}") from e

        inner_game = inner_response.json()["data"]

        inner_dk_index = None
        for i, bookmaker in enumerate(inner_game["bookmakers"]):
            if bookmaker["key"] == "draftkings":
                inner_dk_index = i
                break

        if inner_dk_index is None:

            game["bookmakers"][dk_index]["markets"].append(
                {
                    "key": "team_totals",
                    "last_update": "",
                    "outcomes": []
                }
            )

        else:

            team_totals_market = inner_response.json()["data"]["bookmakers"][inner_dk_index]["markets"][0]

            game["bookmakers"][dk_index]["markets"].append(
                team_totals_market
            )

    return games


import json
dt = datetime(2025, 12, 26, 0, 0, 0)
e = get_games_json(dt)
print(json.dumps(e, indent=2))
