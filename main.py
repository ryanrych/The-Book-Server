from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg


UPSERT_SQL = """
INSERT INTO mlb.moneyline_odds (
    odds_api_id,
    commence_time,
    team,
    opponent,
    home,
    ml_odds,
    spread,
    spread_odds,
    home_score,
    away_score,
    pulled_at,
    updated_at
)
VALUES (
    %(odds_api_id)s,
    %(commence_time)s,
    %(team)s,
    %(opponent)s,
    %(home)s,
    %(ml_odds)s,
    %(spread)s,
    %(spread_odds)s,
    %(home_score)s,
    %(away_score)s,
    %(pulled_at)s,
    %(pulled_at)s
)
ON CONFLICT (odds_api_id, team)
DO UPDATE SET
    commence_time = EXCLUDED.commence_time,
    opponent = EXCLUDED.opponent,
    home = EXCLUDED.home,
    ml_odds = EXCLUDED.ml_odds,
    spread = EXCLUDED.spread,
    spread_odds = EXCLUDED.spread_odds,
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    pulled_at = EXCLUDED.pulled_at,
    updated_at = EXCLUDED.updated_at;
"""


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_events(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if not isinstance(data, list):
            raise ValueError("Expected payload['data'] to be a list")
        return data

    if isinstance(payload, list):
        return payload

    raise ValueError("Payload must be either a dict with 'data' or a list of events")


def pick_bookmaker(event: dict[str, Any]) -> dict[str, Any] | None:
    bookmakers = event.get("bookmakers") or []
    if not bookmakers:
        return None

    for bookmaker in bookmakers:
        if bookmaker.get("key") == "draftkings":
            return bookmaker

    return bookmakers[0]


def get_market(bookmaker: dict[str, Any] | None, market_key: str) -> dict[str, Any] | None:
    if not bookmaker:
        return None

    for market in bookmaker.get("markets", []):
        if market.get("key") == market_key:
            return market

    return None


def extract_scores(event: dict[str, Any]) -> tuple[int | None, int | None]:
    """
    Your sample payload does not include scores, so these will usually be None.
    This supports a couple of possible future shapes.
    """
    home_score = to_int_or_none(event.get("home_score"))
    away_score = to_int_or_none(event.get("away_score"))

    if home_score is not None or away_score is not None:
        return home_score, away_score

    scores = event.get("scores")
    if isinstance(scores, list):
        score_map: dict[str, int | None] = {}
        for item in scores:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if name:
                score_map[name] = to_int_or_none(item.get("score"))

        return (
            score_map.get(event.get("home_team")),
            score_map.get(event.get("away_team")),
        )

    return None, None


def build_rows_from_payload(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = normalize_events(payload)
    pulled_at = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []

    for event in events:
        odds_api_id = event["id"]
        commence_time = parse_iso8601(event.get("commence_time"))
        home_team = event["home_team"]
        away_team = event["away_team"]

        home_score, away_score = extract_scores(event)

        bookmaker = pick_bookmaker(event)
        h2h_market = get_market(bookmaker, "h2h")
        spreads_market = get_market(bookmaker, "spreads")

        team_rows: dict[str, dict[str, Any]] = {
            home_team: {
                "odds_api_id": odds_api_id,
                "commence_time": commence_time,
                "team": home_team,
                "opponent": away_team,
                "home": True,
                "ml_odds": None,
                "spread": None,
                "spread_odds": None,
                "home_score": home_score,
                "away_score": away_score,
                "pulled_at": pulled_at,
            },
            away_team: {
                "odds_api_id": odds_api_id,
                "commence_time": commence_time,
                "team": away_team,
                "opponent": home_team,
                "home": False,
                "ml_odds": None,
                "spread": None,
                "spread_odds": None,
                "home_score": home_score,
                "away_score": away_score,
                "pulled_at": pulled_at,
            },
        }

        if h2h_market:
            for outcome in h2h_market.get("outcomes", []):
                team_name = outcome.get("name")
                if team_name in team_rows:
                    team_rows[team_name]["ml_odds"] = to_int_or_none(outcome.get("price"))

        if spreads_market:
            for outcome in spreads_market.get("outcomes", []):
                team_name = outcome.get("name")
                if team_name in team_rows:
                    team_rows[team_name]["spread"] = to_float_or_none(outcome.get("point"))
                    team_rows[team_name]["spread_odds"] = to_int_or_none(outcome.get("price"))

        rows.extend(team_rows.values())

    return rows


def upsert_odds_json(
    conn: psycopg.Connection,
    payload: dict[str, Any] | list[dict[str, Any]],
    commit: bool = True,
) -> int:
    rows = build_rows_from_payload(payload)

    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)

    if commit:
        conn.commit()

    return len(rows)
