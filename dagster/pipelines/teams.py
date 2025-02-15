import time

import duckdb
import polars as pl
import toml
from nba_api.stats.endpoints import teamdetails
from nba_api.stats.static.teams import get_teams


def get_team_id():
    nba_teams = get_teams()
    return [team["id"] for team in nba_teams]


def get_team_details() -> pl.DataFrame:
    team_id_list = get_team_id()
    all_team_data = []

    for team_id in team_id_list:
        time.sleep(2)  # Add a delay to prevent rate limiting

        team = teamdetails.TeamDetails(str(team_id))
        data_dict = team.get_dict()

        team_data = next(
            (
                item
                for item in data_dict["resultSets"]
                if item["name"] == "TeamBackground"
            ),
            None,
        )

        if not team_data:
            raise ValueError(
                f"TeamBackground result set not found for team ID {team_id}"
            )

        team_df = pl.DataFrame(
            team_data["rowSet"], schema=team_data["headers"], orient="row"
        )
        all_team_data.append(team_df)

    # Combine all individual team DataFrames into one
    all_teams_dataframe = pl.concat(all_team_data)

    all_teams_dataframe = all_teams_dataframe.rename(
        {
            "TEAM_ID": "team_id",
            "ABBREVIATION": "abbreviation",
            "NICKNAME": "nickname",
            "YEARFOUNDED": "year_founded",
            "CITY": "city",
            "ARENA": "arena",
            "ARENACAPACITY": "arena_capacity",
            "OWNER": "owner",
            "GENERALMANAGER": "general_manager",
            "HEADCOACH": "head_coach",
            "DLEAGUEAFFILIATION": "league_affiliation",
         }
    )

    return all_teams_dataframe

def get_team_logo() -> list[str]:
    logo_url_first = "https://cdn.nba.com/logos/nba/"
    logo_url_last = "/primary/L/logo.svg"
    team_id_list = get_team_id()

    team_logo_list = [
        f"{logo_url_first}{team_id}{logo_url_last}" for team_id in team_id_list
    ]

    return team_logo_list


def build_and_upload_dataframe():
    base_dataframe = get_team_details()
    logo_list = get_team_logo()

    final_dataframe = base_dataframe.with_columns(
        pl.Series(name="team_logo", values=logo_list)
    )

    with open("./secrets.toml", "r") as f:
        secrets = toml.load(f)

    motherduck_token = secrets["tokens"]["motherduck"]

    try:
        conn = duckdb.connect(f"md:nba_data?motherduck_token={motherduck_token}")

        conn.register("teams", final_dataframe)

        conn.sql("CREATE OR REPLACE TABLE teams AS SELECT * FROM teams;")
        conn.sql("ALTER TABLE nba_data.teams ADD PRIMARY KEY (TEAM_ID);")

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
    build_and_upload_dataframe()
