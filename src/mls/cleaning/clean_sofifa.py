import pandas as pd
from mls.cleaning.sofifa import clean_player_stats, clean_team_stats



def clean_sofifa():
    
    player_stats = pd.read_csv('data/raw/sofifa/sofifa_player_stats.csv')    
    team_stats = pd.read_csv('data/raw/sofifa/sofifa_team_stats.csv')
    cleaned_player_stats = clean_player_stats.clean_player_stats(player_stats)
    cleaned_team_stats = clean_team_stats.clean_team_stats(team_stats)
    
    return cleaned_player_stats, cleaned_team_stats