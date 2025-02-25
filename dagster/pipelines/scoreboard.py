import duckdb
import pendulum
import polars as pl
from typing import Any
from nba_api.stats.endpoints import scoreboardv2

from dagster.pipelines.token import get_motherduck_token


def extract_scoreboard() -> dict[str, Any]:
    """
    Extracts the scoreboard data from the NBA API for today's games.

    Returns:
        dict[str, Any]: A dictionary with scoreboard.
    """
    today = pendulum.now("America/Los_Angeles").to_date_string()

    scoreboard = scoreboardv2.ScoreboardV2(
        game_date=today, league_id="00", day_offset=0
    )

    data_dict = scoreboard.get_dict()

    scoreboard_dict = data_dict["resultSets"][1]

    return scoreboard_dict


def transform_scoreboard(scoreboard_dict: dict[str, Any]) -> pl.DataFrame:
    """
    Transforms the raw scoreboard data into a Polars DataFrame.

    Args:
        scoreboard_dict (dict[str, Any]): Raw JSON-like dictionary containing NBA scoreboard data.

    Returns:
        pl.DataFrame: A structured Polars DataFrame with dropped and renamed columns.
    """

    scoreboard_dataframe = (
        pl.DataFrame(
            scoreboard_dict["rowSet"], schema=scoreboard_dict["headers"], orient="row"
        )
        .drop(
            "TEAM_CITY_NAME",
            "TEAM_NAME",
            "PTS_OT3",
            "PTS_OT4",
            "PTS_OT5",
            "PTS_OT6",
            "PTS_OT7",
            "PTS_OT8",
            "PTS_OT9",
            "PTS_OT10",
        )
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

    return scoreboard_dataframe


def upload_dataframe(scoreboard_dataframe: pl.DataFrame, motherduck_token: str) -> None:
    """
    Uploads the given Polars DataFrame to a DuckDB database hosted on MotherDuck.

    Args:
        scoreboard_dataframe (pl.DataFrame): The transformed scoreboard dataframe to be uploaded.
        motherduck_token (str): The authentication token for connecting to MotherDuck.

    Raises:
        duckdb.IntegrityError: If there's a primary key or constraint violation.
        duckdb.OperationalError: If the database connection or token is invalid.
        duckdb.ProgrammingError: If there is a SQL syntax error or incorrect table reference.
        Exception: For any other unexpected errors.
    """

    try:
        conn = duckdb.connect(
            f"md:nba_data_staging?motherduck_token={motherduck_token}"
        )

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


scoreboard_df = transform_scoreboard(scoreboard_dict=extract_scoreboard())
motherduck_token = get_motherduck_token()

upload_dataframe(scoreboard_df, motherduck_token)
