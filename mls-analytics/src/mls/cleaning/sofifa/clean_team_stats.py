import pandas as pd


def clean_team_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
        
    ## remove columns including "Unnamed"
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    rename_map = {
        "Name": "team_name",
        "ID": "team_id",
        "Formation": "team_formation",
        "Overall": "overall_score",
        "Attack": "attack",
        "Midfield": "midfield",
        "Defence": "defense",
        "Players": "num_players",
        "date": "date"
    }

    for possible in ["Club worth", "Club.worth"]:
        if possible in df.columns:
            rename_map[possible] = "worth_euro"

    df = df.rename(columns=rename_map)

    if "team_formation" in df.columns:
        split_cols = df["team_formation"].str.split(" ", expand=True)
        if split_cols.shape[1] >= 2:
            df["lineup"] = split_cols[0]
            df["style"] = split_cols[1]
        if split_cols.shape[1] >= 3:
            df["trash2"] = split_cols[2]
        else:
            df["trash2"] = None

    df["team_name"] = df["team_name"].str.replace("Major League Soccer", "", regex=False)

    if "worth_euro" in df.columns:
        df["worth_euro"] = (
            df["worth_euro"]
            .astype(str)
            .str.replace("€", "", regex=False)
            .str.replace("M", "", regex=False)
        )

    numeric_cols = ["overall_score", "attack", "midfield", "defense", "worth_euro", "num_players"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "trash2" in df.columns:
        df = df.drop(columns=["trash2"])
    
    
    df['date'] = pd.to_datetime(df['date'])
    
            
    df = df.rename(columns={'lineup': 'formation_base', 'style': 'formation_style', 'overall_score': 'overall', 'defense': 'defence', 'worth_euro': 'club_worth', 'team_name': 'name', 'num_players': 'players', 'Pressure': 'pressure'}) 
    
    
    df = df[['team_id', 'date', 'name', 'overall', 'attack', 'midfield', 'defence', 'pressure', 'formation_base', 'formation_style', 'club_worth', 'players']]
        
    return df