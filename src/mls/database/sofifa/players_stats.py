import pandas as pd

def get_player_stats(df):
    df = df.drop(columns=['name', 'height_cm', 'weight_kg', 'team_name', 'contract_start', 'contract_end', 'position', 'foot'])
    return df