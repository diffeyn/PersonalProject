from mls.cleaning import clean_matches, clean_sofifa
from pathlib import Path



def clean_data():    ## clean match data
    
    outdir = Path("data/interim")
    outdir.mkdir(parents=True, exist_ok=True)
    
    cleaned_match_team_stats, cleaned_match_player_stats, cleaned_feed = clean_matches.clean_matches()
    
    cleaned_match_team_stats.to_csv(f"{outdir}/matches/cleaned_match_team_stats.csv", index=False)
    cleaned_match_player_stats.to_csv(f"{outdir}/matches/cleaned_match_player_stats.csv", index=False)
    cleaned_feed.to_csv(f"{outdir}/matches/cleaned_match_feed.csv", index=False)  
    
    ## clean SoFIFA data
    cleaned_players, cleaned_team = clean_sofifa.clean_sofifa()
    cleaned_team.to_csv(f"{outdir}/sofifa/cleaned_team_stats.csv", index=False)
    cleaned_players.to_csv(f"{outdir}/sofifa/cleaned_player_stats.csv", index=False)    
    