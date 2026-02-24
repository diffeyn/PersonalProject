import pandas as pd

def get_player_stats(df):
    df = df.copy()
    df.rename(columns={"id": "player_id"}, inplace=True)
    df = df.drop(columns=['name', 'height_cm', 'weight_kg', 'team_name', 'contract_start', 'contract_end', 'position', 'foot', 'wage_eur', 'value_eur'])
    return df