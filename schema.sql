-- Snapshots: one row per API poll
CREATE TABLE snapshots (
    timestamp           TIMESTAMPTZ PRIMARY KEY,
    previous_timestamp  TIMESTAMPTZ,
    next_timestamp      TIMESTAMPTZ
);

-- Games: one row per game, upserted on each poll
CREATE TABLE games (
    id              TEXT PRIMARY KEY,
    sport_key       TEXT NOT NULL,
    sport_title     TEXT NOT NULL,
    commence_time   TIMESTAMPTZ NOT NULL,
    home_team       TEXT NOT NULL,
    away_team       TEXT NOT NULL
);

-- Bookmakers: reference/lookup table
CREATE TABLE bookmakers (
    key     TEXT PRIMARY KEY,
    title   TEXT NOT NULL
);

-- Odds: every line from every bookmaker for every market
CREATE TABLE odds (
    id              BIGSERIAL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ NOT NULL REFERENCES snapshots(timestamp),
    game_id         TEXT NOT NULL REFERENCES games(id),
    bookmaker_key   TEXT NOT NULL REFERENCES bookmakers(key),
    market_key      TEXT NOT NULL,
    last_update     TIMESTAMPTZ NOT NULL,
    outcome_name    TEXT NOT NULL,
    price           INTEGER NOT NULL,
    point           NUMERIC,
    UNIQUE (snapshot_time, game_id, bookmaker_key, market_key, outcome_name)
);

-- Indexes for the queries you'll actually run
CREATE INDEX ON odds (game_id, market_key);
CREATE INDEX ON odds (snapshot_time);
CREATE INDEX ON odds (bookmaker_key);
