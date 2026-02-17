from mls.cleaning.matches import clean_match_feed, clean_match_players, clean_match_team
from pandas import read_csv



def clean_matches():
    
    match_team_stats = read_csv('data/raw/matches/match_team_stats.csv')
    match_player_stats = read_csv('data/raw/matches/match_player_stats.csv')
    match_feed = read_csv('data/raw/matches/match_feed.csv')

    cleaned_team_stats = clean_match_team.clean_match_team(match_team_stats)
    cleaned_player_stats = clean_match_players.clean_match_players(match_player_stats)
    cleaned_feed = clean_match_feed.clean_match_feed(match_feed)
    
    return cleaned_team_stats, cleaned_player_stats, cleaned_feed