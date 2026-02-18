import pandas as pd
import re


def reframe_stats(df):
    match_id = df['match_id'].iloc[0]
    out = {}

    for _, row in df.iterrows():
        stat = row.get("stat")
        if pd.isna(stat):
            continue
        stat = str(stat).strip().lower()
        if stat in ("", "nan", "none") or "nan" in stat:
            continue

        out[f"{stat}_home"] = row.get("home_value")
        out[f"{stat}_away"] = row.get("away_value")

    wide = pd.DataFrame([out]) if out else pd.DataFrame([{}])

    wide.columns = [
        re.sub(r"\s+", "_",
        c.replace('%', 'pct').replace('-', '_')).lower()
        for c in wide.columns
    ]
    wide["match_id"] = match_id
    return wide



def clean_match_team(df):
    df = df.copy()
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
    df = df.drop(columns=['home_advantage', 'away_advantage'])
    df["category"] = df["category"].fillna("").astype(str).str.strip()
    df["stat_name"] = df["stat_name"].fillna("").astype(str).str.strip()

    df["stat"] = (df["category"] + "_" + df["stat_name"]).str.strip("_")

    # remove junk stats
    df = df[df["stat"].ne("") & df["stat"].ne("nan") & ~df["stat"].str.contains(r"\bnan\b", na=False)]
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
    df = reframe_stats(df)
    df = df.drop(columns=['general_blocked_shots_home', 'general_blocked_shots_away',
                      'general_goals_home', 'general_goals_away',
                        'general_off_target_home', 'general_off_target_away',
                        'general_on_target_home', 'general_on_target_away'])
    
    df.rename(columns={'general_shots_on_target_home': 'general_shots_on_goal_home',
                       'general_shots_on_target_away': 'general_shots_on_goal_away'}, inplace=True)
    
    print('columns after cleaning match team stats:', df.columns)
    return df

