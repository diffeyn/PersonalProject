import pandas as pd

### rename clubs according to dictionaries 

clubs = [
    "Atlanta United",
    "Austin FC",
    "CF Montréal",
    "Charlotte FC",
    "Chicago Fire FC",
    "FC Cincinnati",
    "Colorado Rapids",
    "Columbus Crew",
    "D.C. United",
    "FC Dallas",
    "Houston Dynamo FC",
    "Sporting Kansas City",
    "LA Galaxy",
    "Los Angeles Football Club",
    "Inter Miami CF",
    "Minnesota United FC",
    "Minnesota United",
    "Nashville SC",
    "New England Revolution",
    "New York City Football Club",
    "New York City FC",
    "New York Red Bulls",
    "Orlando City",
    "Philadelphia Union",
    "Portland Timbers",
    "Real Salt Lake",
    "San Diego FC",
    "San Jose Earthquakes",
    "Seattle Sounders FC",
    "St. Louis CITY SC",
    "Toronto FC",
    "Vancouver Whitecaps FC"
]

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



def clean_players(df):
    """
    Clean and standardize player statistics data from MLS matches.
    This function renames columns from their abbreviated forms to more descriptive names
    and ensures that 'match_id' is the first column in the resulting DataFrame.
    Parameters
    ----------
    df : pandas.DataFrame
        A DataFrame containing player statistics with columns in their original/abbreviated form.
        Expected to contain columns like 'Player', 'Mins', 'G', 'xG', etc.
    Returns
    -------
    pandas.DataFrame
        A cleaned DataFrame with standardized column names and 'match_id' as the first column.
        Column mappings include:
        - Player -> player_name
        - Mins -> minutes
        - G -> goals
        - xG -> expected_goals
        - Conv% -> shot_conv_perc
        - SOT -> on_target
        - Pass% -> pass_perc
        - A -> assists
        - P -> passes
        - Cross -> cross
        - CK -> corner_kick
        - KP -> key_pass
        - AD -> aerial
        - AD% -> aerial_perc
        - FC -> fouls
        - FS -> fouls_against
        - OFF -> offside
        - YC -> yellow_card
        - RC -> red_card
    Notes
    -----
    If 'match_id' column is not found in the input DataFrame, a warning message
    will be printed to the console listing all available columns.
    """
    df = df.rename(columns={
        'match_id': 'match_id',
        'Player': 'player_name',
        'Mins': 'minutes',
        'G' : 'goals',
        'xG': 'expected_goals',
        'Conv%' : 'shot_conv_perc',
        'SOT' : 'on_target',
        'Pass%' : 'pass_perc',
        'A' : 'assists',
        'P' : 'passes',
        'Cross' : 'cross',
        'CK' : 'corner_kick',
        'KP' : 'key_pass',
        'AD' : 'aerial',
        'AD%' : 'aerial_perc',
        'FC' : 'fouls',
        'FS' : 'fouls_against',
        'OFF' : 'offside',
        'YC' : 'yellow_card',
        'RC' : 'red_card'
    })
    
    if 'match_id' in df.columns:
        df = df[['match_id'] + [c for c in df.columns if c != 'match_id']]
    else:
        print("No match_id column found. Columns are:", df.columns.tolist())
        
    month_map = {
        "january": "01", "february": "02", "march": "03", "april": "04",
        "may": "05", "june": "06", "july": "07", "august": "08",
        "september": "09", "october": "10", "november": "11", "december": "12"
    }

    df['date'] = df['date'].str.replace(r"^[A-Za-z]+ ", "", regex=True)

    df['date'] = df['date'].apply(
            lambda x: re.sub(
                r"([A-Za-z]+)",
                lambda m: month_map[m.group(1).lower()],
                x
            )
        )

    df['date'] = df['date'].apply(lambda x: x + " 2025" if re.search(r"\d{4}$", x) is None else x)

    df['date'] = pd.to_datetime(df['date'], format="%m %d %Y")
    
    df = df.sort_values(by=['date', 'match_id'], ascending=[False, False])
    
    def norm_team(s):
        if pd.isna(s):
            return ""
        return (
            str(s).lower()
            .strip()
            .replace(".", "")
            .replace("’", "'")
        )

    # 1) Build a normalized map from your official team_map
    team_map_norm = {norm_team(k): v for k, v in team_map.items()}

    # 2) Add aliases that appear in your current data
    aliases = {
        "chicago fire": "CHI",                 # vs Chicago Fire FC
        "houston dynamo": "HOU",               # vs Houston Dynamo FC
        "dc united": "DC",                     # vs D.C. United
        "los angeles fc": "LAFC",              # vs Los Angeles Football Club
        "orlando city sc": "ORL",              # vs Orlando City
        "st louis city sc": "STL",             # vs St. Louis CITY SC
        "inter miami": "MIA",                  # vs Inter Miami CF
        "minnesota united fc": "MIN",          # already there but keep explicit
    }
    abbrs = set(team_map_norm.values())

    df["club_norm"] = df["club"].apply(norm_team)

    df["club_abbr"] = df["club_norm"].apply(
        lambda x: x.upper() if x.upper() in abbrs else team_map_norm.get(x)
    )
    
    return df
