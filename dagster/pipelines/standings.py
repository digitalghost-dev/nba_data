import duckdb
import polars as pl
from typing import Any
from nba_api.stats.endpoints import leaguestandingsv3

from dagster.pipelines.token import get_motherduck_token


def extract_standings() -> dict[str, Any]:
    """
    Extracts the standings data from the NBA API for current standings.

    Returns:
         dict[str, Any]: A dictionary with standings data.
    """
    standings = leaguestandingsv3.LeagueStandingsV3(
        league_id="00", season=2024, season_type="Regular Season"
    )

    data_dict = standings.get_dict()

    standings_dict= data_dict["resultSets"][0]

    return standings_dict


def transform_standings(standings_dict: dict[str, Any]) -> pl.DataFrame:
    """
    Transforms the raw standings data into a Polars DataFrame.

    Args:
        standings_dict (dict[str, Any]): Raw JSON-like dictionary containing NBA standings data.

    Returns:
        pl.DataFrame: A structured Polars DataFrame with dropped and renamed columns.
    """

    standings_dataframe = (
        pl.DataFrame(standings_dict["rowSet"], schema=standings_dict["headers"], orient="row")
        .drop("TeamCity", "TeamName")
        .rename(
            {
                "LeagueID": "league_id",
                "SeasonID": "season_id",
                "TeamID": "team_id",
                "TeamSlug": "team_slug",
                "Conference": "conference",
                "ConferenceRecord": "conference_record",
                "PlayoffRank": "playoff_rank",
                "ClinchIndicator": "clinch_indicator",
                "Division": "division",
                "DivisionRecord": "division_record",
                "DivisionRank": "division_rank",
                "WINS": "wins",
                "LOSSES": "losses",
                "WinPCT": "win_pct",
                "LeagueRank": "league_rank",
                "Record": "record",
                "HOME": "home",
                "ROAD": "road",
                "L10": "last_10",
                "Last10Home": "last_10_home",
                "Last10Road": "last_10_road",
                "OT": "overtime",
                "ThreePTSOrLess": "three_pts_or_less",
                "TenPTSOrMore": "ten_pts_or_more",
                "LongHomeStreak": "long_home_streak",
                "strLongHomeStreak": "str_long_home_streak",
                "LongRoadStreak": "long_road_streak",
                "strLongRoadStreak": "str_long_road_streak",
                "LongWinStreak": "long_win_streak",
                "LongLossStreak": "long_loss_streak",
                "CurrentHomeStreak": "current_home_streak",
                "strCurrentHomeStreak": "str_current_home_streak",
                "CurrentRoadStreak": "current_road_streak",
                "strCurrentRoadStreak": "str_current_road_streak",
                "CurrentStreak": "current_streak",
                "strCurrentStreak": "str_current_streak",
                "ConferenceGamesBack": "conference_games_back",
                "DivisionGamesBack": "division_games_back",
                "ClinchedConferenceTitle": "clinched_conference_title",
                "ClinchedDivisionTitle": "clinched_division_title",
                "ClinchedPlayoffBirth": "clinched_playoff_birth",
                "ClinchedPlayIn": "clinched_play_in",
                "EliminatedConference": "eliminated_conference",
                "EliminatedDivision": "eliminated_division",
                "AheadAtHalf": "ahead_at_half",
                "BehindAtHalf": "behind_at_half",
                "TiedAtHalf": "tied_at_half",
                "AheadAtThird": "ahead_at_third",
                "BehindAtThird": "behind_at_third",
                "TiedAtThird": "tied_at_third",
                "Score100PTS": "score_100_pts",
                "OppScore100PTS": "opp_score_100_pts",
                "OppOver500": "opp_over_500",
                "LeadInFGPCT": "lead_in_fg_pct",
                "LeadInReb": "lead_in_reb",
                "FewerTurnovers": "fewer_turnovers",
                "PointsPG": "points_pg",
                "OppPointsPG": "opp_points_pg",
                "DiffPointsPG": "diff_points_pg",
                "vsEast": "vs_east",
                "vsAtlantic": "vs_atlantic",
                "vsCentral": "vs_central",
                "vsSoutheast": "vs_southeast",
                "vsWest": "vs_west",
                "vsNorthwest": "vs_northwest",
                "vsPacific": "vs_pacific",
                "vsSouthwest": "vs_southwest",
                "Jan": "january",
                "Feb": "february",
                "Mar": "march",
                "Apr": "april",
                "May": "may",
                "Jun": "june",
                "Jul": "july",
                "Aug": "august",
                "Sep": "september",
                "Oct": "october",
                "Nov": "november",
                "Dec": "december",
                "Score_80_Plus": "score_80_plus",
                "Opp_Score_80_Plus": "opp_score_80_plus",
                "Score_Below_80": "score_below_80",
                "Opp_Score_Below_80": "opp_score_below_80",
                "TotalPoints": "total_points",
                "OppTotalPoints": "opp_total_points",
                "DiffTotalPoints": "diff_total_points",
                "LeagueGamesBack": "league_games_back",
                "PlayoffSeeding": "playoff_seeding",
                "ClinchedPostSeason": "clinched_post_season",
                "NEUTRAL": "neutral",
            }
        )
    )

    return standings_dataframe


def upload_dataframe(standings_dataframe: pl.DataFrame, motherduck_token: str) -> None:
    """
    Uploads the given Polars DataFrame to a DuckDB database hosted on MotherDuck.

    Args:
        standings_dataframe (pl.DataFrame): The transformed standings dataframe to be uploaded.
        motherduck_token (str): The authentication token for connecting to MotherDuck.

    Raises:
        duckdb.IntegrityError: If there's a primary key or constraint violation.
        duckdb.OperationalError: If the database connection or token is invalid.
        duckdb.ProgrammingError: If there is a SQL syntax error or incorrect table reference.
        Exception: For any other unexpected errors.
    """

    try:
        conn = duckdb.connect(f"md:nba_data_staging?motherduck_token={motherduck_token}")

        conn.register("standings", standings_dataframe)

        conn.sql("CREATE OR REPLACE TABLE standings AS SELECT * FROM standings;")
        conn.sql("ALTER TABLE nba_data_staging.standings ADD PRIMARY KEY (team_id);")

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


standings_df = transform_standings(standings_dict=extract_standings())
motherduck_token = get_motherduck_token()

upload_dataframe(standings_df, motherduck_token)
