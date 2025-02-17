import duckdb
import polars as pl
import toml
from nba_api.stats.endpoints import playerindex


def get_players() -> pl.DataFrame:
    players = playerindex.PlayerIndex(league_id="00", season=2024)

    data_dict = players.get_dict()

    players_data = data_dict["resultSets"][0]

    players_dataframe = pl.DataFrame(
        players_data["rowSet"], schema=players_data["headers"], orient="row"
    )

    players_dataframe = players_dataframe.rename(
        {
            "PERSON_ID": "person_id",
            "PLAYER_LAST_NAME": "player_last_name",
            "PLAYER_FIRST_NAME": "player_first_name",
            "PLAYER_SLUG": "player_slug",
            "TEAM_ID": "team_id",
            "TEAM_SLUG": "team_slug",
            "IS_DEFUNCT": "is_defunct",
            "TEAM_CITY": "team_city",
            "TEAM_NAME": "team_name",
            "TEAM_ABBREVIATION": "team_abbreviation",
            "JERSEY_NUMBER": "jersey_number",
            "POSITION": "position",
            "HEIGHT": "height",
            "WEIGHT": "weight",
            "COLLEGE": "college",
            "COUNTRY": "country",
            "DRAFT_YEAR": "draft_year",
            "DRAFT_ROUND": "draft_round",
            "DRAFT_NUMBER": "draft_number",
            "ROSTER_STATUS": "roster_status",
            "FROM_YEAR": "from_year",
            "TO_YEAR": "to_year",
            "PTS": "points",
            "REB": "rebounds",
            "AST": "assists",
            "STATS_TIMEFRAME": "stats_timeframe",
        }
    )

    return players_dataframe


def upload_dataframe():
    players_dataframe = get_players()

    with open("../../secrets.toml") as f:
        secrets = toml.load(f)

    motherduck_token = secrets["tokens"]["motherduck"]

    try:
        conn = duckdb.connect(f"md:nba_data_staging?motherduck_token={motherduck_token}")

        conn.register("players", players_dataframe)

        conn.sql("CREATE OR REPLACE TABLE players AS SELECT * FROM players")
        conn.sql("ALTER TABLE nba_data_staging.players ADD PRIMARY KEY(person_id)")

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
