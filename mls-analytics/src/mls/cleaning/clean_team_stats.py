import pandas as pd


def clean_teams_stats(df):
    """
    Clean and transform team statistics data from wide to long format.
    This function processes team statistics by:
    1. Mapping time period codes (bar_0, bar_1, etc.) to their minute ranges (0-5, 6-10, etc.)
    2. Combining category and stat_name into a single stat column
    3. Extracting possession percentages for time-based statistics
    4. Removing unnecessary columns and standardizing the output format
    Args:
        df (pd.DataFrame): DataFrame containing team statistics with columns:
            - match_id: Unique identifier for the match
            - category: Category of the statistic
            - stat_name: Name of the specific statistic
            - tip_id: Time period identifier (e.g., 'bar_0', '0-5')
            - home_possession: Home team possession data
            - away_possession: Away team possession data
            - home_value: Home team statistic value
            - away_value: Away team statistic value
            - home_advantage: Home advantage indicator (dropped)
            - away_advantage: Away advantage indicator (dropped)
    Returns:
        pd.DataFrame: Cleaned DataFrame with columns:
            - match_id: Match identifier
            - stat: Combined statistic name (e.g., 'possession_0_5')
            - home_value: Home team value for the statistic
            - away_value: Away team value for the statistic
    """   
    bar_dict = {
        '0-5': 'bar_1_0',
        '6-10': 'bar_1_1',
        '11-15': 'bar_2_0',
        '16-20': 'bar_2_1',
        '21-25': 'bar_2_2',
        '26-30': 'bar_2_3',
        '31-35': 'bar_2_4',
        '36-40': 'bar_2_5',
        '41-45': 'bar_2_6',
        '46-50': 'bar_3_0',
        '51-55': 'bar_3_1',
        '56-60': 'bar_3_2',
        '61-65': 'bar_3_3',
        '66-70': 'bar_3_4',
        '71-75': 'bar_3_5',
        '76-80': 'bar_3_6',
        '81-85': 'bar_3_7',
        '86-90': 'bar_3_8',
    }

    bar_dict_switched = {v: k for k, v in bar_dict.items()}
    df = df.copy()
    df = df.drop(columns=['home_advantage', 'away_advantage'])
    df['stat'] = df['category'].astype(str) + '_' + df['stat_name'].astype(str)
    df = df.drop(columns=['category', 'stat_name'])
    df['tip_id'] = df['tip_id'].astype(str).str.strip() 
    df['tip_id'] = df['tip_id'].replace(bar_dict_switched)
    mask = df["tip_id"].astype(str).str.match(r"^\d{1,2}-\d{1,2}$", na=False)
    h_pct = df.loc[mask, "home_possession"].astype(str).str.extract(r"(\d+(?:\.\d+)?)")[0].astype(float)
    a_pct = df.loc[mask, "away_possession"].astype(str).str.extract(r"(\d+(?:\.\d+)?)")[0].astype(float)
    df.loc[mask, "home_value"] = h_pct.values 
    df.loc[mask, "away_value"] = a_pct.values
    df.loc[mask, "stat"] = "possession_" + df.loc[mask, "tip_id"].str.replace("-", "_", regex=False)
    df = df.drop(columns=['tip_id', 'home_possession', 'away_possession'])
    df = df[['match_id', 'stat', 'home_value', 'away_value']]
    return df

