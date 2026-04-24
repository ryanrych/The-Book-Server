import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("HOST"),
        port=os.environ.get("PORT"),
        dbname=os.environ.get("DATABASE"),
        user=os.environ.get("DB_USERNAME"),
        password=os.environ.get("PASSWORD"),
        sslmode="require"
    )


def upsert_snapshot(cur, snapshot_time, previous_timestamp=None, next_timestamp=None):
    cur.execute("""
        INSERT INTO snapshots (timestamp, previous_timestamp, next_timestamp)
        VALUES (%s, %s, %s)
        ON CONFLICT (timestamp) DO NOTHING
    """, (snapshot_time, previous_timestamp, next_timestamp))


def upsert_games(cur, games: list):
    if not games:
        return
    execute_values(cur, """
        INSERT INTO games (id, sport_key, sport_title, commence_time, home_team, away_team)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            commence_time = EXCLUDED.commence_time,
            home_team     = EXCLUDED.home_team,
            away_team     = EXCLUDED.away_team
    """, games)


def upsert_bookmakers(cur, bookmakers: list):
    if not bookmakers:
        return
    execute_values(cur, """
        INSERT INTO bookmakers (key, title)
        VALUES %s
        ON CONFLICT (key) DO UPDATE SET title = EXCLUDED.title
    """, bookmakers)


def insert_odds(cur, odds: list):
    if not odds:
        return
    execute_values(cur, """
        INSERT INTO odds (
            snapshot_time, game_id, bookmaker_key, market_key,
            last_update, outcome_name, price, point
        )
        VALUES %s
        ON CONFLICT (snapshot_time, game_id, bookmaker_key, market_key, outcome_name)
        DO NOTHING
    """, odds)
