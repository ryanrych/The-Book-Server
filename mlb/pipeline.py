import json
import os
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import statsapi
from dotenv import load_dotenv
from tqdm import tqdm

from api import get_games_json
from db import upsert_odds_payload


load_dotenv()
host = os.getenv("HOST")
port = os.getenv("PORT")
user = os.getenv("DB_USERNAME")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")

DB_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"


def iter_valid_days(start_date: datetime, end_date: datetime):
    current = start_date

    while current <= end_date:
        # Skip Dec, Jan, Feb
        if current.month not in (12, 1, 2):
            yield current

        current += timedelta(days=1)


def print_mlb_teams():
    teams = statsapi.get("teams", {"sportId": 1})

    all_teams = []

    for team in teams.get("teams", []):
        all_teams.append({
            "id": team["id"],
            "name": team["name"],
            "abbreviation": team.get("abbreviation"),
            "teamName": team.get("teamName"),
            "locationName": team.get("locationName"),
        })

    # Sort alphabetically for easier comparison
    all_teams.sort(key=lambda x: x["name"])

    for t in all_teams:
        print(f"{t['id']:>3} | {t['name']} | {t['abbreviation']}")


DB_DSN = os.getenv("DB_DSN")  # ex: postgres://user:pass@host:5432/dbname
TABLE_NAME = "mlb.games"

# Expand/adjust these if your DB team names differ from MLB Stats API names.
TEAM_NAME_MAP = {
    "Oakland Athletics": "Athletics",
}

FINAL_STATES = {
    "Final",
    "Game Over",
    "Completed Early",
}


def normalize_team(name: str) -> str:
    if not name:
        return name
    return TEAM_NAME_MAP.get(name.strip(), name.strip())


def fetch_pending_games(conn):
    """
    Pull one record per odds_api_id for games missing scores.
    Assumes each game has exactly 2 rows sharing the same odds_api_id.
    """
    sql = f"""
        SELECT
            odds_api_id,
            MIN(commence_time) AS commence_time,
            MAX(CASE WHEN home = true THEN team END) AS home_team,
            MAX(CASE WHEN home = false THEN team END) AS away_team
        FROM {TABLE_NAME}
        WHERE (home_score IS NULL OR away_score IS NULL)
          AND odds_api_id IS NOT NULL
        GROUP BY odds_api_id
        ORDER BY MIN(commence_time)
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return cur.fetchall()


def fetch_schedule_for_date(game_date):
    """
    Use raw get('schedule', ...) because it is the most flexible documented interface.
    """
    data = statsapi.get(
        "schedule",
        {
            "sportId": 1,  # MLB
            "date": game_date.strftime("%Y-%m-%d"),
        },
    )

    games = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            teams = game.get("teams", {})
            home = teams.get("home", {})
            away = teams.get("away", {})

            games.append(
                {
                    "gamePk": game.get("gamePk"),
                    "gameDate": game.get("gameDate"),
                    "abstractGameState": game.get("status", {}).get("abstractGameState"),
                    "detailedState": game.get("status", {}).get("detailedState"),
                    "home_team": home.get("team", {}).get("name"),
                    "away_team": away.get("team", {}).get("name"),
                    "home_score": home.get("score"),
                    "away_score": away.get("score"),
                }
            )
    return games


def choose_best_match(db_home_team, db_away_team, commence_time, candidates):
    """
    Match by exact normalized team names first.
    If multiple exist, choose the nearest gameDate to commence_time.
    """
    db_home_team = normalize_team(db_home_team)
    db_away_team = normalize_team(db_away_team)

    exact = [
        g for g in candidates
        if normalize_team(g["home_team"]) == db_home_team
        and normalize_team(g["away_team"]) == db_away_team
    ]

    if not exact:
        return None

    # Commence time is timestamptz in Postgres, so it should already be timezone-aware.
    def sort_key(g):
        gd = g.get("gameDate")
        if gd:
            try:
                game_dt = datetime.fromisoformat(gd.replace("Z", "+00:00"))
                return abs((game_dt - commence_time).total_seconds())
            except Exception:
                pass
        return float("inf")

    exact.sort(key=sort_key)
    return exact[0]


def update_scores(conn, odds_api_id, home_score, away_score):
    sql = f"""
        UPDATE {TABLE_NAME}
        SET
            home_score = %s,
            away_score = %s,
            updated_at = NOW()
        WHERE odds_api_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (home_score, away_score, odds_api_id))


def main():
    if not DB_DSN:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database,
            sslmode="require",
            sslrootcert="./ca-certificate.crt",
        )
        conn.autocommit = False
    else:

        conn = psycopg2.connect(DB_DSN)
        conn.autocommit = False

    try:
        pending_games = fetch_pending_games(conn)
        print(f"Found {len(pending_games)} games missing scores.")

        # Cache schedules by date so we only call MLB once per day.
        schedule_cache = {}

        updated = 0
        skipped_not_final = 0
        skipped_no_match = 0

        for row in pending_games:
            odds_api_id = row["odds_api_id"]
            commence_time = row["commence_time"]
            home_team = row["home_team"]
            away_team = row["away_team"]

            # Search same day plus adjacent day in case timezone/date boundary differs.
            search_dates = {
                commence_time.date() - timedelta(days=1),
                commence_time.date(),
                commence_time.date() + timedelta(days=1),
            }

            candidates = []
            for d in sorted(search_dates):
                if d not in schedule_cache:
                    schedule_cache[d] = fetch_schedule_for_date(d)
                candidates.extend(schedule_cache[d])

            match = choose_best_match(home_team, away_team, commence_time, candidates)

            if not match:
                skipped_no_match += 1
                print(f"[NO MATCH] {odds_api_id}: {away_team} @ {home_team} ({commence_time})")
                continue

            if match["detailedState"] not in FINAL_STATES:
                skipped_not_final += 1
                print(
                    f"[NOT FINAL] {odds_api_id}: "
                    f'{match["away_team"]} @ {match["home_team"]} - {match["detailedState"]}'
                )
                continue

            home_score = match["home_score"]
            away_score = match["away_score"]

            if home_score is None or away_score is None:
                skipped_not_final += 1
                print(f"[NO SCORES YET] {odds_api_id}: matched but scores missing")
                continue

            update_scores(conn, odds_api_id, home_score, away_score)
            updated += 1

            print(
                f"[UPDATED] {odds_api_id}: "
                f'{match["away_team"]} {away_score} @ {match["home_team"]} {home_score}'
            )

        conn.commit()
        print(
            f"Done. Updated={updated}, "
            f"skipped_not_final={skipped_not_final}, "
            f"skipped_no_match={skipped_no_match}"
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

exit()


start = datetime(2023, 5, 4, tzinfo=timezone.utc)
end = datetime.now(timezone.utc)
end = datetime(2026, 4, 9, tzinfo=timezone.utc)

days = list(iter_valid_days(start, end))
total = 0
with psycopg.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    dbname=database,
    sslmode="require",
    sslrootcert="./ca-certificate.crt",
) as conn:

    for i, day in enumerate(tqdm(days, desc="Processing days"), start=1):

        payload = get_games_json(day)

        total += upsert_odds_payload(conn, payload, commit=False)

        if i % 25 == 0:
            conn.commit()
            print(f"Committed batch {i}")

    conn.commit()

    conn.commit()
