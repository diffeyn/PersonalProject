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
    
    df['home_team'] = df['home_team'].replace(team_fullnames_to_short)
    df['away_team'] = df['away_team'].replace(team_fullnames_to_short)
    
    df = df.rename(columns={'home_team_score': 'home_score', 'away_team_score': 'away_score'})    
    
    return df