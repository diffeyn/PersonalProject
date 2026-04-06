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
    df = df.sort_values(by=["match_id"], ascending=[False]).reset_index(drop=True)
    

    ### if anything after space in column name, delete it (e.g. "Player Name" -> "Player")
    df.columns = [col.split()[0] for col in df.columns]
    

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
        
    return df


import pandas as pd
 
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
    "Vancouver Whitecaps FC": "VAN",
}
 
 
def clean_match_gk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
 
    if "match_id" not in df.columns:
        raise ValueError(f"No match_id column found. Columns: {df.columns.tolist()}")
 
    # match_id first
    df = df[["match_id"] + [c for c in df.columns if c != "match_id"]]
 
    # ---- team mapping ----
    def norm_team(s):
        if pd.isna(s):
            return ""
        return str(s).lower().strip().replace(".", "").replace("\u2019", "'")
 
    team_map_norm = {norm_team(k): v for k, v in team_map.items()}
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
    abbrs = set(team_map_norm.values())
 
    if "club" in df.columns:
        df["_club_norm"] = df["club"].apply(norm_team)
        df["club"] = df["_club_norm"].apply(
            lambda x: x.upper() if x.upper() in abbrs else team_map_norm.get(x)
        )
        df = df.drop(columns=["_club_norm"])
    else:
        raise ValueError("No club column found.")
 
    # ---- strip trailing tokens from column names ----
    df.columns = [col.split()[0] for col in df.columns]
 
    # ---- GK-specific renames ----
    df = df.rename(columns={
        "Player":  "player_name",
        "Mins":    "minutes",
        "GS":      "gk_goals_saved",
        "GA":      "gk_goals_against",
        "XGA":     "gk_expected_goals_against",
        "Pass":    "gk_pass_count",       # raw pass count
        "Pass%":   "gk_pass_perc",        # pass accuracy %
        "THRW":    "gk_throws",
        "LB":      "gk_long_balls",
        "LNCH":    "gk_launches",
        "FC":      "fouls",
        "FS":      "fouls_against",
        "YC":      "yellow_card",
        "RC":      "red_card",
        "CC":      "corners_conceded",
    })
 
    # ---- drop columns that are always zero / not useful for GKs ----
    cols_to_drop = [c for c in ["goals", "assists", "passes", "shot_conv_perc",
                                 "expected_goals", "on_target", "cross",
                                 "corner_kick", "key_pass", "aerial",
                                 "aerial_perc", "offside", "GK"] if c in df.columns]
    df = df.drop(columns=cols_to_drop)
 
    # ---- fill nulls ----
    int_cols   = ["minutes", "gk_goals_saved", "gk_goals_against", "gk_throws",
                  "gk_long_balls", "gk_launches", "fouls", "fouls_against",
                  "yellow_card", "red_card", "corners_conceded"]
    float_cols = ["gk_expected_goals_against", "gk_pass_count", "gk_pass_perc"]
 
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
 
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
 
    # ---- sort ----
    df = df.sort_values(by=["match_id"], ascending=False).reset_index(drop=True)
 
    return df



