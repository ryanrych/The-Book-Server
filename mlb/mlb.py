import os
from pathlib import Path
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_echarts import st_echarts, JsCode
from dotenv import load_dotenv
from datetime import date


st.set_page_config(page_title="MLB Strategy Tester", layout="wide")

load_dotenv()

# ---------- Team classification ----------

ANIMAL_TEAMS = {
    "Arizona Diamondbacks",
    "Baltimore Orioles",
    "Chicago Cubs",
    "Detroit Tigers",
    "Miami Marlins",
    "St. Louis Cardinals",
    "Tampa Bay Rays",
    "Toronto Blue Jays",
}

TEAM_CITY_COLLEGE_RANK = {
    "Boston Red Sox":           1,
    "New York Yankees":         3,
    "New York Mets":            3,
    "Chicago Cubs":             6,
    "Chicago White Sox":        6,
    "Los Angeles Dodgers":      15,
    "Los Angeles Angels":       15,
    "San Francisco Giants":     17,
    "Oakland Athletics":        17,
    "Athletics":                17,
    "Philadelphia Phillies":    6,
    "Washington Nationals":     22,
    "Seattle Mariners":         40,
    "Atlanta Braves":           24,
    "Houston Astros":           17,
    "St. Louis Cardinals":      24,
    "Minnesota Twins":          40,
    "Pittsburgh Pirates":       40,
    "Detroit Tigers":           21,
    "San Diego Padres":         28,
    "Cleveland Guardians":      40,
    "Cleveland Indians":        40,
    "Milwaukee Brewers":        50,
    "Cincinnati Reds":          70,
    "Baltimore Orioles":        9,
    "Toronto Blue Jays":        18,
    "Miami Marlins":            50,
    "Tampa Bay Rays":           80,
    "Texas Rangers":            38,
    "Kansas City Royals":       100,
    "Colorado Rockies":         95,
    "Arizona Diamondbacks":     105,
}


def has_animal_mascot(team: str) -> bool:
    return team in ANIMAL_TEAMS


def has_better_college(team: str, opponent: str) -> bool:
    return TEAM_CITY_COLLEGE_RANK.get(team, 999) < TEAM_CITY_COLLEGE_RANK.get(opponent, 999)


# ---------- Helpers ----------

def american_profit(stake: float, odds: float) -> float:
    if odds > 0:
        return stake * (odds / 100.0)
    if odds < 0:
        return stake * (100.0 / abs(odds))
    return 0.0


def american_to_decimal(odds: float) -> float:
    if odds > 0:
        return 1 + (odds / 100)
    return 1 + (100 / abs(odds))


# ---------- DB ----------

def get_connection():
    return psycopg2.connect(
        host=os.getenv("HOST"),
        port=os.getenv("PORT", "5432"),
        dbname=os.getenv("DATABASE"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="verify-full",
        sslrootcert=os.getenv("SSL_CERT_PATH"),
    )


@st.cache_data(ttl=600)
def load_games() -> pd.DataFrame:
    conn = get_connection()
    try:
        query = """
            SELECT
                g.id AS game_id,
                g.commence_time,
                g.home_team,
                g.away_team,
                gr.home_score,
                gr.away_score,
                MAX(CASE WHEN o.market_key = 'h2h' AND o.outcome_name = g.home_team THEN o.price END) AS home_ml,
                MAX(CASE WHEN o.market_key = 'h2h' AND o.outcome_name = g.away_team THEN o.price END) AS away_ml,
                MAX(CASE WHEN o.market_key = 'totals' AND o.outcome_name = 'Over' THEN o.price END)   AS over_price,
                MAX(CASE WHEN o.market_key = 'totals' AND o.outcome_name = 'Under' THEN o.price END)  AS under_price,
                MAX(CASE WHEN o.market_key = 'totals' AND o.outcome_name = 'Over' THEN o.point END)   AS total_line
            FROM games g
            JOIN game_results gr ON gr.game_id = g.id
            JOIN odds o ON o.game_id = g.id
            WHERE g.sport_key = 'baseball_mlb'
              AND gr.status = 'final'
              AND o.bookmaker_key = 'fanduel'
            GROUP BY g.id, g.commence_time, g.home_team, g.away_team,
                     gr.home_score, gr.away_score
            ORDER BY g.commence_time
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    df["commence_time"] = pd.to_datetime(df["commence_time"], utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["home_ml", "away_ml", "home_score", "away_score"])
    return df.reset_index(drop=True)


# ---------- Strategies ----------

def select_about_to_be_swept(games: pd.DataFrame, max_gap_days: int = 4) -> pd.DataFrame:
    bets = []
    long_form = []
    for _, row in games.iterrows():
        long_form.append({
            "game_id": row["game_id"], "commence_time": row["commence_time"],
            "team": row["home_team"], "opponent": row["away_team"],
            "ml_odds": row["home_ml"],
            "team_score": row["home_score"], "opp_score": row["away_score"],
        })
        long_form.append({
            "game_id": row["game_id"], "commence_time": row["commence_time"],
            "team": row["away_team"], "opponent": row["home_team"],
            "ml_odds": row["away_ml"],
            "team_score": row["away_score"], "opp_score": row["home_score"],
        })

    long_df = pd.DataFrame(long_form).sort_values(["team", "commence_time"]).reset_index(drop=True)

    for team, team_games in long_df.groupby("team", sort=False):
        team_games = team_games.reset_index(drop=True)
        for i in range(len(team_games)):
            current = team_games.iloc[i]
            prior_results = []
            next_time = current["commence_time"]
            j = i - 1
            while j >= 0:
                prev = team_games.iloc[j]
                gap = (next_time - prev["commence_time"]).total_seconds() / 86400
                if prev["opponent"] != current["opponent"] or gap > max_gap_days:
                    break
                prior_results.append("W" if prev["team_score"] > prev["opp_score"] else "L")
                next_time = prev["commence_time"]
                j -= 1

            is_final = True
            if i < len(team_games) - 1:
                nxt = team_games.iloc[i + 1]
                gap_next = (nxt["commence_time"] - current["commence_time"]).total_seconds() / 86400
                if nxt["opponent"] == current["opponent"] and gap_next <= max_gap_days:
                    is_final = False

            if is_final and len(prior_results) >= 2 and all(r == "L" for r in prior_results):
                bets.append({
                    "commence_time": current["commence_time"],
                    "bet_team": team, "opponent": current["opponent"],
                    "ml_odds": current["ml_odds"],
                    "won": current["team_score"] > current["opp_score"],
                })
    return pd.DataFrame(bets)


def select_home_underdog(games: pd.DataFrame) -> pd.DataFrame:
    bets = []
    for _, row in games.iterrows():
        if row["home_ml"] > 0:
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["home_team"], "opponent": row["away_team"],
                "ml_odds": row["home_ml"],
                "won": row["home_score"] > row["away_score"],
            })
    return pd.DataFrame(bets)


def select_animal_vs_human(games: pd.DataFrame) -> pd.DataFrame:
    bets = []
    for _, row in games.iterrows():
        home_animal = has_animal_mascot(row["home_team"])
        away_animal = has_animal_mascot(row["away_team"])
        if home_animal and not away_animal:
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["home_team"], "opponent": row["away_team"],
                "ml_odds": row["home_ml"],
                "won": row["home_score"] > row["away_score"],
            })
        elif away_animal and not home_animal:
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["away_team"], "opponent": row["home_team"],
                "ml_odds": row["away_ml"],
                "won": row["away_score"] > row["home_score"],
            })
    return pd.DataFrame(bets)


def select_better_college(games: pd.DataFrame) -> pd.DataFrame:
    bets = []
    for _, row in games.iterrows():
        if has_better_college(row["home_team"], row["away_team"]):
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["home_team"], "opponent": row["away_team"],
                "ml_odds": row["home_ml"],
                "won": row["home_score"] > row["away_score"],
            })
        elif has_better_college(row["away_team"], row["home_team"]):
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["away_team"], "opponent": row["home_team"],
                "ml_odds": row["away_ml"],
                "won": row["away_score"] > row["home_score"],
            })
    return pd.DataFrame(bets)


def select_favorites(games: pd.DataFrame) -> pd.DataFrame:
    bets = []
    for _, row in games.iterrows():
        if row["home_ml"] < row["away_ml"]:
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["home_team"], "opponent": row["away_team"],
                "ml_odds": row["home_ml"],
                "won": row["home_score"] > row["away_score"],
            })
        else:
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["away_team"], "opponent": row["home_team"],
                "ml_odds": row["away_ml"],
                "won": row["away_score"] > row["home_score"],
            })
    return pd.DataFrame(bets)


def select_underdogs(games: pd.DataFrame) -> pd.DataFrame:
    bets = []
    for _, row in games.iterrows():
        if row["home_ml"] > row["away_ml"]:
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["home_team"], "opponent": row["away_team"],
                "ml_odds": row["home_ml"],
                "won": row["home_score"] > row["away_score"],
            })
        else:
            bets.append({
                "commence_time": row["commence_time"],
                "bet_team": row["away_team"], "opponent": row["home_team"],
                "ml_odds": row["away_ml"],
                "won": row["away_score"] > row["home_score"],
            })
    return pd.DataFrame(bets)


def select_overs(games: pd.DataFrame) -> pd.DataFrame:
    bets = []
    for _, row in games.iterrows():
        if pd.isna(row["total_line"]) or pd.isna(row["over_price"]):
            continue
        total_runs = row["home_score"] + row["away_score"]
        if total_runs == row["total_line"]:
            continue
        bets.append({
            "commence_time": row["commence_time"],
            "bet_team": f"Over {row['total_line']}",
            "opponent": f"{row['away_team']} @ {row['home_team']}",
            "ml_odds": row["over_price"],
            "won": total_runs > row["total_line"],
        })
    return pd.DataFrame(bets)


def select_unders(games: pd.DataFrame) -> pd.DataFrame:
    bets = []
    for _, row in games.iterrows():
        if pd.isna(row["total_line"]) or pd.isna(row["under_price"]):
            continue
        total_runs = row["home_score"] + row["away_score"]
        if total_runs == row["total_line"]:
            continue
        bets.append({
            "commence_time": row["commence_time"],
            "bet_team": f"Under {row['total_line']}",
            "opponent": f"{row['away_team']} @ {row['home_team']}",
            "ml_odds": row["under_price"],
            "won": total_runs < row["total_line"],
        })
    return pd.DataFrame(bets)


# ---------- Simulation ----------

def simulate(bets: pd.DataFrame, starting_bankroll: float, bet_pct: float) -> pd.DataFrame:
    if bets.empty:
        return pd.DataFrame()

    bets = bets.sort_values("commence_time").reset_index(drop=True)
    bankroll = float(starting_bankroll)
    current_day = None
    daily_stake = None
    rows = []

    for i, bet in bets.iterrows():
        bet_day = pd.to_datetime(bet["commence_time"]).date()
        if bet_day != current_day:
            current_day = bet_day
            daily_stake = round(bankroll * bet_pct, 2)

        stake = daily_stake
        bankroll_before = bankroll

        if bet["won"]:
            pnl = american_profit(stake, float(bet["ml_odds"]))
            result = "W"
        else:
            pnl = -stake
            result = "L"

        bankroll = round(bankroll + pnl, 2)
        rows.append({
            "bet_number": i + 1,
            "commence_time": bet["commence_time"],
            "bet_team": bet["bet_team"],
            "opponent": bet["opponent"],
            "ml_odds": int(bet["ml_odds"]),
            "result": result,
            "bankroll_before": round(bankroll_before, 2),
            "stake": round(stake, 2),
            "pnl": round(pnl, 2),
            "bankroll_after": round(bankroll, 2),
        })

    return pd.DataFrame(rows)


# ---------- Charting ----------

def build_chart(name: str, sim: pd.DataFrame) -> dict:
    x_axis = sim["bet_number"].tolist()
    data = []
    for _, row in sim.iterrows():
        odds = int(row["ml_odds"])
        data.append({
            "value": float(row["bankroll_after"]),
            "stake": float(row["stake"]),
            "opponent": str(row["opponent"]),
            "bet_team": str(row["bet_team"]),
            "odds": f"+{odds}" if odds > 0 else str(odds),
            "result": str(row["result"]),
            "date": pd.to_datetime(row["commence_time"]).strftime("%Y-%m-%d"),
        })

    tooltip = JsCode("""
        function (params) {
            const p = Array.isArray(params) ? params[0] : params;
            const d = p.data;
            return [
                "Bet #" + p.axisValue,
                "Date: " + d.date,
                "Bankroll: $" + Number(d.value).toFixed(2),
                "Bet: " + d.bet_team,
                "Matchup: " + d.opponent,
                "Odds: " + d.odds,
                "Stake: $" + Number(d.stake).toFixed(2),
                "Result: " + d.result,
            ].join("<br/>");
        }
    """)

    return {
        "title": {"text": name, "left": "center"},
        "tooltip": {"trigger": "axis", "formatter": tooltip},
        "grid": {"left": "5%", "right": "5%", "top": 60, "bottom": 50, "containLabel": True},
        "xAxis": {"type": "category", "name": "Bet #", "data": x_axis},
        "yAxis": {"type": "value", "name": "Bankroll ($)", "scale": True},
        "series": [{
            "name": "Bankroll",
            "type": "line",
            "symbol": "circle",
            "symbolSize": 5,
            "data": data,
            "lineStyle": {"width": 2},
        }],
    }


# ---------- App ----------

st.title("MLB Strategy Tester")
st.caption("FanDuel odds. Stake set daily at chosen % of bankroll.")

with st.sidebar:
    st.header("Settings")
    starting_bankroll = st.number_input("Starting bankroll", min_value=1.0, value=100.0, step=10.0)
    bet_pct = st.number_input("Daily stake (% of bankroll)", min_value=0.01, max_value=1.0,
                              value=0.05, step=0.01, format="%.2f")

try:
    games_df = load_games()
except Exception as e:
    st.error("Could not load data from PostgreSQL.")
    st.code(str(e))
    st.stop()

if games_df.empty:
    st.warning("No games loaded.")
    st.stop()

st.caption(f"Loaded {len(games_df):,} games with results.")

# Bounds for the date sliders
min_date = games_df["commence_time"].min().date()
max_date = games_df["commence_time"].max().date()

strategies = {
    "About to be swept":      select_about_to_be_swept,
    "Home underdog":          select_home_underdog,
    "Animal mascot vs human": select_animal_vs_human,
    "Better college city":    select_better_college,
    "Favorites":              select_favorites,
    "Underdogs":              select_underdogs,
    "Overs":                  select_overs,
    "Unders":                 select_unders,
}

selected = st.multiselect("Strategies to show", list(strategies.keys()), default=list(strategies.keys()))

for name in selected:
    st.markdown(f"### {name}")

    # Date range slider per strategy
    start_date, end_date = st.slider(
        "Date range",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="YYYY-MM-DD",
        key=f"date_range_{name}",
    )

    # Filter games to the selected date range BEFORE running the strategy
    # This ensures bankroll simulation only runs over the chosen window
    mask = (games_df["commence_time"].dt.date >= start_date) & \
           (games_df["commence_time"].dt.date <= end_date)
    filtered_games = games_df[mask].reset_index(drop=True)

    bets = strategies[name](filtered_games)
    sim = simulate(bets, starting_bankroll, bet_pct)

    if sim.empty:
        st.info("No bets generated by this strategy in the selected range.")
        continue

    wins   = int((sim["result"] == "W").sum())
    losses = int((sim["result"] == "L").sum())
    final  = float(sim["bankroll_after"].iloc[-1])
    profit = round(final - starting_bankroll, 2)
    avg_odds = sim["ml_odds"].mean()
    avg_decimal = sim["ml_odds"].apply(american_to_decimal).mean()
    avg_american = f"+{avg_odds:.0f}" if avg_odds >= 0 else f"{avg_odds:.0f}"
    pct = ((final - starting_bankroll) / starting_bankroll) * 100

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Final bankroll", f"${final:,.2f}", f"{pct:+.1f}%")
    c2.metric("Profit", f"${profit:,.2f}")
    c3.metric("Record", f"{wins}-{losses}")
    c4.metric("Avg odds", f"{avg_american} ({avg_decimal:.2f})")

    st_echarts(options=build_chart(name, sim), height="380px", key=f"chart_{name}")

    with st.expander(f"Show {name} bet log"):
        st.dataframe(sim, use_container_width=True, hide_index=True)
