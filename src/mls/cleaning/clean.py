from mls.cleaning import clean_matches, clean_sofifa
from mls.utils.scraping import io
import pandas as pd


def clean_data():    ## clean match data
    cleaned_team_stats, cleaned_player_stats, cleaned_feed = clean_matches.clean_matches()
    
    cleaned_team_stats.to_csv("./tests/data/processed/matches/cleaned_team_stats.csv", index=False)
    cleaned_player_stats.to_csv("./tests/data/processed/matches/cleaned_player_stats.csv", index=False)
    cleaned_feed.to_csv("./tests/data/processed/matches/cleaned_match_feed.csv", index=False)  
    
    ## clean SoFIFA data
    cleaned_players, cleaned_team = clean_sofifa.clean_sofifa()
    cleaned_team.to_csv("./tests/data/processed/sofifa/cleaned_team_stats.csv", index=False)
    cleaned_players.to_csv("./tests/data/processed/sofifa/cleaned_player_stats.csv", index=False)    
    