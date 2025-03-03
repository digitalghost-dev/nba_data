import time
from typing import Any

import duckdb
import polars as pl
from nba_api.stats.endpoints import teamdetails
from nba_api.stats.static.teams import get_teams

from dagster.pipelines.token import get_motherduck_token


def get_team_id() -> list[int]:
    """
    Retrieves each team's id from the static team's endpoint.

    Returns:
        list[int]: A list of team ids.
    """
    nba_teams = get_teams()
    return [team["id"] for team in nba_teams]


def get_team_logo(team_id_list: list[int]) -> list[str]:
    """
    Builds a URL for each team's logo from cdn.nba.com

    Args:
        team_id_list (list[int]): A list of team ids.

    Returns:
        list[str]: A list of logo urls.
    """

    logo_url_first = "https://cdn.nba.com/logos/nba/"
    logo_url_last = "/primary/L/logo.svg"

    team_logo_list = [
        f"{logo_url_first}{team_id}{logo_url_last}" for team_id in team_id_list
    ]

    return team_logo_list


def get_headers() -> list[Any]:
    """
    Extracts the headers for the TeamBackground dictionary.
    The headers are later used for the Polars dataframe.

    Returns:
        list[Any]: A list of headers.
    """

    headers = teamdetails.TeamDetails("1610612738")
    data_dict = headers.get_dict()

    header_details = next(
        (
            item["headers"]
            for item in data_dict["resultSets"]
            if item["name"] == "TeamBackground"
        ),
        None,
    )

    return header_details


def extract_teams(team_id_list: list[int]) -> list[list[Any]]:
    """
    Iterates through each team id and returns a combined dataframe with all teams.

    Args:
        team_id_list (list[int]): A list of team ids.

    Returns:
        List[List[Any]]: A list of combined teams.
    """

    all_teams_list = []

    for team_id in team_id_list:
        time.sleep(2)

        team = teamdetails.TeamDetails(str(team_id))
        data_dict = team.get_dict()

        team_details = next(
            (
                item["rowSet"]
                for item in data_dict["resultSets"]
                if item["name"] == "TeamBackground"
            ),
            None,
        )

        if team_details is None:
            raise ValueError("TeamBackground resultSets not found.")

        all_teams_list.append(team_details)

    return all_teams_list


def transform_teams(
    all_teams_list: list[list[Any]], header_details: list[str], team_logo_list: list[str]
) -> pl.DataFrame:
    """
    Transforms the raw standings data into a Polars DataFrame.

    Args:
        all_teams_list (list[list[Any]]): List of a list of teams and details.
        header_details (list[str]): A list of headers.
        team_logo_list (list[str]): A list of logo urls.

    Returns:
        pl.DataFrame: A structured Polars DataFrame with dropped and renamed columns.
    """

    flattened_teams = [team[0] for team in all_teams_list]

    teams_dataframe = pl.DataFrame(
        flattened_teams, schema=header_details, orient="row"
    ).rename(
        {
            "TEAM_ID": "team_id",
            "ABBREVIATION": "abbreviation",
            "NICKNAME": "team_name",
            "YEARFOUNDED": "year_founded",
            "CITY": "team_city",
            "ARENA": "arena",
            "ARENACAPACITY": "arena_capacity",
            "OWNER": "owner",
            "GENERALMANAGER": "general_manager",
            "HEADCOACH": "head_coach",
            "DLEAGUEAFFILIATION": "league_affiliation",
        }
    ).with_columns(pl.Series(name="team_logo", values=team_logo_list))

    return teams_dataframe


def upload_dataframe(teams_dataframe: pl.DataFrame, motherduck_token: str) -> None:
    """
    Uploads the given Polars DataFrame to a DuckDB database hosted on MotherDuck.

    Args:
        teams_dataframe (pl.DataFrame): The transformed teams dataframe to be uploaded.
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

        conn.register("teams", teams_dataframe)

        conn.sql("CREATE OR REPLACE TABLE teams AS SELECT * FROM teams;")
        conn.sql("ALTER TABLE nba_data_staging.teams ADD PRIMARY KEY (team_id)")

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


def main():
    team_id_list = get_team_id()
    all_teams_list = extract_teams(team_id_list)
    header_details = get_headers()
    team_logo_list = get_team_logo(team_id_list)

    teams_df = transform_teams(all_teams_list, header_details, team_logo_list)
    motherduck_token = get_motherduck_token()

    upload_dataframe(teams_df, motherduck_token)


main()
