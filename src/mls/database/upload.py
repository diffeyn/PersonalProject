from sqlalchemy import engine
from mls.utils.database.sql_funct import upload_to_db
from mls.database.matches.connect_players import attach_player_ids, AttachConfig
from mls.database.sofifa.players_general import players_general
from mls.database.sofifa.players_finance import get_players_finance
from mls.database.sofifa.players_stats import get_player_stats
from mls.database.engine import make_engine
from mls.database.sofifa import team_roster as tr
from pathlib import Path
import pandas as pd

def upload_to_sql():
    indir = Path('data/interim')
    
    engine = make_engine()
    
    ### Read in latest cleaned match data
    mls_match_team_stats = pd.read_csv(indir / 'cleaned_matches/cleaned_match_team_stats.csv')
    mls_match_player_stats = pd.read_csv(indir / 'cleaned_matches/cleaned_match_player_stats.csv')
    match_events = pd.read_csv(indir / 'cleaned_matches/cleaned_match_feed.csv')
    matches = pd.read_csv(indir / 'cleaned_matches/cleaned_match_data.csv')
    
    cfg = AttachConfig(
        match_id_col="match_id",
        club_col="club",
        name_col="player_name",
        out_col="player_id",
        threshold=88,
    )

    mls_match_player_stats = attach_player_ids(mls_match_player_stats, engine, cfg)
    
    mls_match_player_stats = mls_match_player_stats.drop(columns=['team_id'])
            
    ### Upload match data to SQL
    upload_to_db(mls_match_team_stats, "match_team_stats", engine)
    upload_to_db(mls_match_player_stats, "match_player_stats", engine)
    upload_to_db(match_events, "match_events", engine)
    upload_to_db(matches, "matches", engine)
    
    
    ### Read in latest cleaned SoFIFA data
    sofifa_players = pd.read_csv(indir / 'sofifa/cleaned_player_stats.csv')
    sofifa_teams = pd.read_csv(indir / 'sofifa/cleaned_team_stats.csv')
    
    ### Create and upload players_general table with one row per player and static info if not already in DB
    players_general = players_general(sofifa_players, engine)
    
    ### check if new players were added to players_general and only upload if there are new players
    if players_general is not None:
        upload_to_db(players_general, "players_general", engine)
    
    ### Create and upload players_finance table with financial info for each player
    players_finance = get_players_finance(sofifa_players)
    upload_to_db(players_finance, "players_finance", engine)
    
    ### Create and upload player_stats table with performance attributes for each player
    player_stats = get_player_stats(sofifa_players)
    upload_to_db(player_stats, "players_stats", engine)
    
    ### refresh roster snapshots and stints
    tr.refresh_team_roster(engine, sofifa_players)
    
    
    ### Team stats
    upload_to_db(sofifa_teams, "team_stats", engine)