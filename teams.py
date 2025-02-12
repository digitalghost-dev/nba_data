import time
import duckdb
import polars as pl
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
            (item for item in data_dict["resultSets"] if item["name"] == "TeamBackground"),
            None
        )

        if not team_data:
            raise ValueError(f"TeamBackground result set not found for team ID {team_id}")

        team_df = pl.DataFrame(team_data["rowSet"], schema=team_data["headers"], orient="row")
        all_team_data.append(team_df)

    # Combine all individual team DataFrames into one
    all_teams_dataframe = pl.concat(all_team_data)

    return all_teams_dataframe

def get_team_logo() -> list[str]:
    logo_url_first = "https://cdn.nba.com/logos/nba/"
    logo_url_last = "/primary/L/logo.svg"
    team_id_list = get_team_id()

    team_logo_list = [f"{logo_url_first}{team_id}{logo_url_last}" for team_id in team_id_list]

    return team_logo_list

def build_and_upload_dataframe():
    base_dataframe = get_team_details()
    logo_list = get_team_logo()

    final_dataframe = base_dataframe.with_columns(pl.Series(name="team_logo", values=logo_list))

    conn = duckdb.connect("md:nba_data")

    conn.register("teams", final_dataframe)

    conn.sql("CREATE OR REPLACE TABLE teams AS SELECT * FROM teams")

    result = conn.sql("SELECT * FROM nba_teams").df()
    print(result)

build_and_upload_dataframe()
