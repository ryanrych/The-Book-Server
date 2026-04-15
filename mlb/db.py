from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg


UPSERT_SQL = """
INSERT INTO mlb.games (
    odds_api_id,
    commence_time,
    team,
    opponent,
    home,
    ml_odds,
    spread,
    spread_odds,
    game_total_line,
    game_total_over_odds,
    game_total_under_odds,
    team_total_line,
    team_total_over_odds,
    team_total_under_odds,
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
    %(game_total_line)s,
    %(game_total_over_odds)s,
    %(game_total_under_odds)s,
    %(team_total_line)s,
    %(team_total_over_odds)s,
    %(team_total_under_odds)s,
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
    game_total_line = EXCLUDED.game_total_line,
    game_total_over_odds = EXCLUDED.game_total_over_odds,
    game_total_under_odds = EXCLUDED.game_total_under_odds,
    team_total_line = EXCLUDED.team_total_line,
    team_total_over_odds = EXCLUDED.team_total_over_odds,
    team_total_under_odds = EXCLUDED.team_total_under_odds,
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


def to_decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_events(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if not isinstance(data, list):
            raise ValueError("Expected payload['data'] to be a list")
        return data

    if isinstance(payload, list):
        return payload

    raise ValueError("Payload must be dict or list")


def pick_bookmaker(event: dict[str, Any]) -> dict[str, Any] | None:
    """
    Prefer DraftKings if present, otherwise use bookmakers[0].
    """
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


def dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Keep the first occurrence of each event id.
    """
    seen: dict[str, dict[str, Any]] = {}

    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue

        if event_id not in seen:
            seen[event_id] = event

    return list(seen.values())


def extract_scores(event: dict[str, Any]) -> tuple[int | None, int | None]:
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
    """
    Build exactly 2 rows per event:
      - one for home team
      - one for away team
    """
    events = dedupe_events(normalize_events(payload))
    pulled_at = datetime.now(timezone.utc)

    rows: list[dict[str, Any]] = []

    for event in events:
        odds_api_id = event.get("id")
        commence_time = parse_iso8601(event.get("commence_time"))
        home_team = event.get("home_team")
        away_team = event.get("away_team")

        if not odds_api_id or not commence_time or not home_team or not away_team:
            continue

        home_score, away_score = extract_scores(event)

        bookmaker = pick_bookmaker(event)
        h2h_market = get_market(bookmaker, "h2h")
        spreads_market = get_market(bookmaker, "spreads")
        totals_market = get_market(bookmaker, "totals")
        team_totals_market = get_market(bookmaker, "team_totals")

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
                "game_total_line": None,
                "game_total_over_odds": None,
                "game_total_under_odds": None,
                "team_total_line": None,
                "team_total_over_odds": None,
                "team_total_under_odds": None,
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
                "game_total_line": None,
                "game_total_over_odds": None,
                "game_total_under_odds": None,
                "team_total_line": None,
                "team_total_over_odds": None,
                "team_total_under_odds": None,
                "home_score": home_score,
                "away_score": away_score,
                "pulled_at": pulled_at,
            },
        }

        if h2h_market:
            for outcome in h2h_market.get("outcomes", []):
                name = outcome.get("name")
                if name in team_rows:
                    team_rows[name]["ml_odds"] = to_int_or_none(outcome.get("price"))

        if spreads_market:
            for outcome in spreads_market.get("outcomes", []):
                name = outcome.get("name")
                if name in team_rows:
                    team_rows[name]["spread"] = to_decimal_or_none(outcome.get("point"))
                    team_rows[name]["spread_odds"] = to_int_or_none(outcome.get("price"))

        if totals_market:
            game_total_line = None
            game_total_over_odds = None
            game_total_under_odds = None

            for outcome in totals_market.get("outcomes", []):
                side = outcome.get("name")
                point = to_decimal_or_none(outcome.get("point"))
                price = to_int_or_none(outcome.get("price"))

                if point is not None:
                    game_total_line = point

                if side == "Over":
                    game_total_over_odds = price
                elif side == "Under":
                    game_total_under_odds = price

            for team_name in team_rows:
                team_rows[team_name]["game_total_line"] = game_total_line
                team_rows[team_name]["game_total_over_odds"] = game_total_over_odds
                team_rows[team_name]["game_total_under_odds"] = game_total_under_odds

        if team_totals_market:
            for outcome in team_totals_market.get("outcomes", []):
                team_name = outcome.get("description")
                side = outcome.get("name")

                if team_name not in team_rows:
                    continue

                point = to_decimal_or_none(outcome.get("point"))
                price = to_int_or_none(outcome.get("price"))

                if point is not None:
                    team_rows[team_name]["team_total_line"] = point

                if side == "Over":
                    team_rows[team_name]["team_total_over_odds"] = price
                elif side == "Under":
                    team_rows[team_name]["team_total_under_odds"] = price

        for row in team_rows.values():
            if (
                row["ml_odds"] is None
                and row["spread"] is None
                and row["spread_odds"] is None
                and row["game_total_line"] is None
                and row["game_total_over_odds"] is None
                and row["game_total_under_odds"] is None
                and row["team_total_line"] is None
                and row["team_total_over_odds"] is None
                and row["team_total_under_odds"] is None
            ):
                continue

            rows.append(row)

    return rows


def upsert_odds_payload(
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


def backfill_payloads(
    conn: psycopg.Connection,
    payloads: list[dict[str, Any] | list[dict[str, Any]]],
    commit_every: int = 50,
) -> int:
    total = 0

    for i, payload in enumerate(payloads, start=1):
        total += upsert_odds_payload(conn, payload, commit=False)

        if i % commit_every == 0:
            conn.commit()

    conn.commit()
    return total
