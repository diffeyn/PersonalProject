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
    matches = df.copy()
    
    if "date" in matches.columns:
        matches["date"] = matches["date"].astype(str).str.replace(r"^[A-Za-z]+,\s*|^[A-Za-z]+\s+", "", regex=True)
        # If year missing, assume 2025 (change if you need dynamic behavior)
        matches["date"] = matches["date"].where(matches["date"].str.contains(r"\b\d{4}\b"), matches["date"] + " 2025")
        matches["date"] = pd.to_datetime(matches["date"], errors="coerce")
    else:
        raise ValueError("No date column found.")
    
    matches['home_team'] = matches['home_team'].replace(team_fullnames_to_short)
    matches['away_team'] = matches['away_team'].replace(team_fullnames_to_short)
    
    return matches