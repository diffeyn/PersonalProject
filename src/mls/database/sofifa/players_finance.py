import pandas as pd

def get_players_finance(df):
    df = df[['player_id', 'date', 'wage_eur', 'value_eur']]
    return df
