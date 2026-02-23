import pandas as pd

def players_general(df, engine):
    df = df.copy()
    df.rename(columns={"ID": "player_id"}, inplace=True)
    df = df[['player_id', 'name', 'height_cm', 'foot']]
    
    ### Check existing player_ids in DB to avoid duplicates
    existing_ids = pd.read_sql("SELECT player_id FROM players_general", engine)
    existing_ids = set(existing_ids['player_id'])
    
    ## Filter out players already in the database
    df = df[~df['player_id'].isin(existing_ids)]
    
    if df.empty:
        print("No new players to add to players_general.")
        return None
    
    return df

