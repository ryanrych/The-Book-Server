from datetime import datetime, timezone
from api import fetch_odds
from db import get_connection, upsert_snapshot, upsert_games, upsert_bookmakers, insert_odds

SPORT = "baseball_mlb"
MARKETS = "h2h,spreads,totals"


def parse(payload: list, snapshot_time: datetime) -> tuple:
    games = []
    bookmakers = []
    odds = []

    for game in payload:
        games.append((
            game["id"],
            game["sport_key"],
            game["sport_title"],
            game["commence_time"],
            game["home_team"],
            game["away_team"],
        ))

        for bm in game.get("bookmakers", []):
            bookmakers.append((bm["key"], bm["title"]))

            for market in bm.get("markets", []):
                for outcome in market.get("outcomes", []):
                    odds.append((
                        snapshot_time,
                        game["id"],
                        bm["key"],
                        market["key"],
                        bm["last_update"],
                        outcome["name"],
                        outcome["price"],
                        outcome.get("point"),
                    ))

    return games, bookmakers, odds


def run():
    snapshot_time = datetime.now(timezone.utc)
    print(f"[MLB] Starting snapshot {snapshot_time.isoformat()}")

    try:
        payload = fetch_odds(SPORT, MARKETS)
    except Exception as e:
        print(f"[MLB] API error: {e}")
        return

    games, bookmakers, odds = parse(payload, snapshot_time)

    try:
        conn = get_connection()
        cur = conn.cursor()

        upsert_snapshot(cur, snapshot_time)
        upsert_games(cur, games)
        upsert_bookmakers(cur, bookmakers)
        insert_odds(cur, odds)

        conn.commit()
        cur.close()
        conn.close()

        print(f"[MLB] Done — {len(games)} games, {len(odds)} odds rows")

    except Exception as e:
        print(f"[MLB] DB error: {e}")


if __name__ == "__main__":
    run()