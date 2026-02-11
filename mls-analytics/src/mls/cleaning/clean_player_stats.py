import pandas as pd
import re

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

def clean_team_name(s):
    if s is None:
        return s

    s = re.sub(r'\bCF\b', '', s)
    s = re.sub(r'\bFC\b', '', s)
    s = re.sub(r'\bUnited\b', '', s)
    s = re.sub(r'\bSC\b', '', s)
    s = re.sub(r'\bFootball Club\b', '', s)
    s = re.sub(r'\.', '', s)
    s = s.strip().lower()

    # collapse extra spaces created by removals
    s = re.sub(r'\s+', ' ', s)

    return s

team_map_clean = {
    clean_team_name(k): v
    for k, v in team_map.items()
}

def safe_eval(x):
    try:
        if '+' in str(x):
            return sum(int(i) for i in str(x).split('+'))
        elif '-' in str(x):
            return int(str(x).split('-')[0]) - sum(int(i) for i in str(x).split('-')[1:])
        else:
            return int(x)
    except:
        return x

def clean_player_stats(df):
    """
    Clean and standardize player statistics data from a DataFrame.
    This function performs comprehensive cleaning and transformation of player statistics data,
    including parsing contract information, converting units, standardizing monetary values,
    and reorganizing columns.
    Args:
        df (pd.DataFrame): Raw player statistics DataFrame containing columns such as:
            - Name: Player name (may contain trailing capital letters)
            - date: Date information (will be converted to datetime)
            - Team & Contract: Combined string with position, jersey number, and contract dates
            - Height: Height with 'cm' unit
            - Weight: Weight with 'kg' unit
            - Wage: Wage with '€' symbol and K/M suffixes
            - Value: Player value with '€' symbol and K/M suffixes
    Returns:
        pd.DataFrame: Cleaned DataFrame with:
            - Removed unnamed columns
            - Cleaned player names (trailing capitals removed)
            - Parsed contract information (position, jersey_num, contract_start, contract_end)
            - Converted height and weight to integer values in cm and kg
            - Converted wage and value to integer EUR values
            - Applied safe_eval to all columns except 'date'
            - Reorganized columns with main columns first, followed by remaining columns
            - Numeric conversion applied where possible
            - Column names lowercased and spaces replaced with underscores
    Note:
        - Requires 'safe_eval' function to be defined in scope
        - Assumes specific format for 'Team & Contract': position(jersey_num)start_year ~ end_year
        - K suffix represents thousands (000), M suffix represents millions (000000)
    """
    df = df.copy()

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    df.loc[:, 'Name'] = df['Name'].str.replace(r'[A-Z]+$', '', regex=True)
    df.loc[:, 'date'] = pd.to_datetime(df['date']).dt.date

    pat = r'(\w+)\((\d+)\)(\d{4}) ~ (\d{4})'
    df[['position', 'jersey_num', 'contract_start',
        'contract_end']] = df.loc[:, 'Team & Contract'].astype(str).str.extract(pat)

    df.loc[:, 'height_cm'] = df['Height'].astype(str).str.split('cm ').str[0].astype('Int64')
    df.loc[:, 'weight_kg'] = df['Weight'].astype(str).str.split('kg ').str[0].astype('Int64')

    df.loc[:, 'wage_eur'] = df['Wage'].astype(str).str.replace('€', '').str.replace(
        ',', '').str.replace('K', '000').str.replace('M',
                                                     '000000').astype('Int64')

    df.loc[:, 'value_eur'] = df['Value'].astype(str).str.replace('€', '').str.replace(
        ',', '').str.replace('.', '').str.replace('K', '000').str.replace(
            'M', '000000').astype('Int64')


    df.drop(columns=['Height', 'Weight', 'Team & Contract', 'Value', 'Wage'],
            inplace=True)
    
    ## safe eval everything but date
    for col in df.columns:
        if col != 'date':
            df[col] = df[col].apply(safe_eval)

    
    main_cols = [
        'ID', 'date', 'Name', 'Age', 'height_cm', 'weight_kg', 'team',
        'contract_start', 'contract_end', 'position', 'foot', 'jersey_num',
        'wage_eur', 'value_eur'
    ]

    rest_cols = [col for col in df.columns if col not in main_cols]
    df = df[main_cols + rest_cols]

    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c])
        except (ValueError, TypeError):
            pass 

    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    df = df.rename(columns={
    'goals_saved': 'gk_goals_saved',
    'goals_against': 'gk_goals_against',
    'expected_goals_against': 'gk_expected_goals_against',
    'Pass': 'gk_pass'
    })
    
    df = df.drop(columns=['date'])
    
    df['team_abbr'] = df['team'].map(team_map_clean)
   
    return df

