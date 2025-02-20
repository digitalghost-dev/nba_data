import duckdb
import pendulum
import polars as pl
import toml
from nba_api.stats.endpoints import scoreboardv2


def get_scoreboard() -> pl.DataFrame:
    today = pendulum.now("America/Los_Angeles").to_date_string()

    scoreboard = scoreboardv2.ScoreboardV2(
        game_date=today, league_id="00", day_offset=0
    )

    data_dict = scoreboard.get_dict()

    line_score_data = data_dict["resultSets"][1]

    return (
        pl.DataFrame(line_score_data["rowSet"], schema=line_score_data["headers"], orient="row")
        .drop("TEAM_CITY_NAME", "TEAM_NAME")
        .rename(
            {
                "GAME_DATE_EST": "game_date_est",
                "GAME_SEQUENCE": "game_sequence",
                "GAME_ID": "game_id",
                "TEAM_ID": "team_id",
                "TEAM_ABBREVIATION": "team_abbreviation",
                "TEAM_WINS_LOSSES": "team_wins_losses",
                "PTS_QTR1": "pts_qtr1",
                "PTS_QTR2": "pts_qtr2",
                "PTS_QTR3": "pts_qtr3",
                "PTS_QTR4": "pts_qtr4",
                "PTS_OT1": "pts_ot1",
                "PTS_OT2": "pts_ot2",
                "PTS_OT3": "pts_ot3",
                "PTS_OT4": "pts_ot4",
                "PTS_OT5": "pts_ot5",
                "PTS_OT6": "pts_ot6",
                "PTS_OT7": "pts_ot7",
                "PTS_OT8": "pts_ot8",
                "PTS_OT9": "pts_ot9",
                "PTS_OT10": "pts_ot10",
                "PTS": "points",
                "FG_PCT": "field_goal_pct",
                "FT_PCT": "free_throw_pct",
                "FG3_PCT": "three_point_pct",
                "AST": "assists",
                "REB": "rebounds",
                "TOV": "turnovers",
            }
        )
    )


def upload_dataframe():
    scoreboard_dataframe = get_scoreboard()

    with open("../../secrets.toml") as f:
        secrets = toml.load(f)

    motherduck_token = secrets["tokens"]["motherduck"]

    try:
        conn = duckdb.connect(f"md:nba_data_staging?motherduck_token={motherduck_token}")

        conn.register("scoreboard", scoreboard_dataframe)

        conn.sql("CREATE OR REPLACE TABLE scoreboard AS SELECT * FROM scoreboard")
        conn.sql("ALTER TABLE nba_data_staging.scoreboard ADD PRIMARY KEY (team_id)")

        conn.close()
    except duckdb.IntegrityError:
        print(
            "Integrity error: Possible duplicate primary key or constraint violation."
        )
    except duckdb.OperationalError:
        print("Operational error: Check database connection and MotherDuck token.")
    except duckdb.ProgrammingError:
        print("SQL syntax error or incorrect table reference.")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    upload_dataframe()
