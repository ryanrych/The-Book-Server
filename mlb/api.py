import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


load_dotenv()
key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")


def to_api_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_games_json(date: datetime):
    """
    Returns a list of full event objects, each pulled at that game's exact commence_time.
    """
    # Step 1: day-level discovery only
    url = base_url + "/historical/sports/baseball_mlb/odds"
    params = {
        "apiKey": key,
        "regions": "us",
        "markets": "h2h",  # discovery only; keep this minimal
        "oddsFormat": "american",
        "date": to_api_timestamp(date),
    }

    response = requests.get(url, params=params, timeout=30)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise Exception(f"Request failed: {response.status_code} - {response.text}") from e

    discovery_games = response.json()["data"]

    final_games = []

    # Step 2: exact per-game snapshot at commence_time
    for game in discovery_games:
        game_commence_time = parse_iso8601(game["commence_time"])

        event_url = base_url + f"/historical/sports/baseball_mlb/events/{game['id']}/odds"
        event_params = {
            "apiKey": key,
            "regions": "us",
            "markets": "h2h,spreads,totals,team_totals",
            "oddsFormat": "american",
            "date": to_api_timestamp(game_commence_time),
        }

        event_response = requests.get(event_url, params=event_params, timeout=30)

        try:
            event_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(
                f"Skipping {game['away_team']} @ {game['home_team']} "
                f"{game['commence_time']} - {event_response.status_code} - {event_response.text}"
            )
            continue

        event_data = event_response.json()["data"]

        # Normalize missing team_totals so db.py gets a stable shape
        bookmakers = event_data.get("bookmakers", [])

        if bookmakers:
            dk_index = None
            for i, bookmaker in enumerate(bookmakers):
                if bookmaker.get("key") == "draftkings":
                    dk_index = i
                    break

            if dk_index is None:
                dk_index = 0

            markets = bookmakers[dk_index].get("markets", [])
            has_team_totals = any(m.get("key") == "team_totals" for m in markets)

            if not has_team_totals:
                markets.append(
                    {
                        "key": "team_totals",
                        "last_update": "",
                        "outcomes": []
                    }
                )

        final_games.append(event_data)

    return final_games
