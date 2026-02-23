import pandas as pd

def get_players_finance(df):
    df = df.copy()
    df.rename(columns={"ID": "player_id"}, inplace=True)
    df = df[['player_id', 'date', 'wage_eur', 'value_eur']]
    return df
