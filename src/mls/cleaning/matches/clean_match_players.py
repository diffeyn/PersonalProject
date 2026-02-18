import pandas as pd
import re

team_map = {
    "Atlanta United": "ATL",
    "Austin FC": "ATX",
    "CF Montréal": "MTL",
    "Charlotte FC": "CLT",
    "Chicago Fire FC": "CHI",
    "FC Cincinnati": "CIN",
    "Colorado Rapids": "COL",
    "Columbus Crew": "CLB",
    "D.C. United": "DC",
    "FC Dallas": "DAL",
    "Houston Dynamo FC": "HOU",
    "Sporting Kansas City": "SKC",
    "LA Galaxy": "LA",
    "Los Angeles Football Club": "LAFC",
    "Inter Miami CF": "MIA",
    "Minnesota United": "MIN",
    "Minnesota United FC": "MIN",
    "Nashville SC": "NSH",
    "New England Revolution": "NE",
    "New York City Football Club": "NYC",
    "New York City FC": "NYC",
    "New York Red Bulls": "RBNY",
    "Orlando City": "ORL",
    "Philadelphia Union": "PHI",
    "Portland Timbers": "POR",
    "Real Salt Lake": "RSL",
    "San Diego FC": "SD",
    "San Jose Earthquakes": "SJ",
    "Seattle Sounders FC": "SEA",
    "St. Louis CITY SC": "STL",
    "Toronto FC": "TOR",
    "Vancouver Whitecaps FC": "VAN"
}

def clean_match_players(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "match_id" in df.columns:
        df = df[["match_id"] + [c for c in df.columns if c != "match_id"]]
    else:
        raise ValueError(f"No match_id column found. Columns are: {df.columns.tolist()}")

    # ---- date parsing (robust) ----
    # Handles: "Saturday March 2", "March 2", "March 2 2025", "March 2, 2025"
    # Removes leading weekday word if present, then lets pandas do the work.
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.replace(r"^[A-Za-z]+,\s*|^[A-Za-z]+\s+", "", regex=True)
        # If year missing, assume 2025 (change if you need dynamic behavior)
        df["date"] = df["date"].where(df["date"].str.contains(r"\b\d{4}\b"), df["date"] + " 2025")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        raise ValueError("No date column found.")

    # ---- team mapping ----
    def norm_team(s):
        if pd.isna(s):
            return ""
        return (
            str(s)
            .lower()
            .strip()
            .replace(".", "")
            .replace("’", "'")
        )

    # Use your team_map as the base truth
    team_map_norm = {norm_team(k): v for k, v in team_map.items()}

    # Add aliases you actually see in scraped data
    aliases = {
        "chicago fire": "CHI",
        "houston dynamo": "HOU",
        "dc united": "DC",
        "los angeles fc": "LAFC",
        "orlando city sc": "ORL",
        "st louis city sc": "STL",
        "inter miami": "MIA",
        "minnesota united fc": "MIN",
        "new york red bulls": "RBNY",
        "new york city": "NYC",
        "new england": "NE",
        "sporting kansas city": "SKC",
        "kansas city": "SKC",
        "salt lake": "RSL",
        "real salt lake": "RSL",
        "la galaxy": "LA",
    }
    team_map_norm.update({norm_team(k): v for k, v in aliases.items()})

    # Already-valid abbreviations should pass through unchanged
    abbrs = set(team_map_norm.values())

    if "club" in df.columns:
        df["_club_norm"] = df["club"].apply(norm_team)

        df["club"] = df["_club_norm"].apply(
            lambda x: x.upper() if x.upper() in abbrs else team_map_norm.get(x)
        )

        # Drop helper norm column
        df = df.drop(columns=["_club_norm"])
    else:
        raise ValueError("No club column found.")

    # ---- sort ----
    df = df.sort_values(by=["date", "match_id"], ascending=[False, False])
    
    print('columns after cleaning:', df.columns.tolist())


    # ---- stat column renames ----
    df = df.rename(columns={
        "Player": "player_name",
        "Mins": "minutes",
        "G": "goals",
        "xG": "expected_goals",
        "Conv%": "shot_conv_perc",
        "SOT": "on_target",
        "Pass%": "pass_perc",
        "A": "assists",
        "P": "passes",
        "Cross": "cross",
        "CK": "corner_kick",
        "KP": "key_pass",
        "AD": "aerial",
        "AD%": "aerial_perc",
        "FC": "fouls",
        "FS": "fouls_against",
        "OFF": "offside",
        "YC": "yellow_card",
        "RC": "red_card",
        "GS": "gk_goals_saved",
        "GA": "gk_goals_against",
        "XGA": "gk_expected_goals_against",
        "Pass`": "gk_pass",
        "THRW": "gk_throws",
        "LB": "gk_long_balls",
        "LNCH": "gk_launches",
        "GKSV": "GK",
        "CC": "corners_conceded",
    })
    
    print('columns after rename:', df.columns.tolist())

    return df


