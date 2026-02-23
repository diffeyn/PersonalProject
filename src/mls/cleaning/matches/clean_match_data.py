import pandas as pd


team_fullnames_to_short = {
    "Columbus": "CLB",
    "Orlando": "ORL",
    "Dallas": "DAL",
    "New York City": "NYC",
    "LAFC": "LAFC",
    "Portland": "POR",
    "New England": "NE",
    "Montréal": "MTL",
    "San Diego": "SD",
    "Nashville": "NSH",
    "Colorado": "COL",
    "Kansas City": "SKC",
    "Minnesota": "MIN",
    "LA Galaxy": "LA",
    "Vancouver": "VAN",
    "San Jose": "SJ",
    "Atlanta": "ATL",
    "D.C. United": "DC",
    "Cincinnati": "CIN",
    "Seattle": "SEA",
    "Houston": "HOU",
    "Charlotte": "CLT",
    "Salt Lake": "RSL",
    "Philadelphia": "PHI",
    "New York": "RBNY",        
    "Toronto": "TOR",
    "Austin": "ATX",
    "Miami": "MIA",
    "Chicago": "CHI",
    "St. Louis": "STL"
}



def clean_match_data(df):
    df = df.copy()
    
    # 1) make string + remove venue (anything after •)
    s = df["date"].astype(str).str.split("•", n=1).str[0].str.strip()

    # 2) remove leading weekday if present (Sunday, Mon, etc.)
    s = s.str.replace(
        r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+",
        "",
        regex=True
    )

    # 3) optional: collapse multiple spaces
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()

    # 4) parse dates (handles both "Feb 16 2026" and "10 5 2025")
    # Try strict known formats first, then fall back to inference.
    dt = pd.to_datetime(s, format="%b %d %Y", errors="coerce")  # Feb 16 2026
    dt2 = pd.to_datetime(s, format="%B %d %Y", errors="coerce") # February 16 2026
    dt3 = pd.to_datetime(s, format="%m %d %Y", errors="coerce") # 10 5 2025

    df["date"] = dt.fillna(dt2).fillna(dt3)
    
    df['home_team'] = df['home_team'].replace(team_fullnames_to_short)
    df['away_team'] = df['away_team'].replace(team_fullnames_to_short)
    
    return df