from mls.cleaning import clean_matches, clean_sofifa
from pathlib import Path
from mls.utils.scraping.io import write_csv




def clean_data():    ## clean match data
    
    outdir = Path("data/interim")
    outdir.mkdir(parents=True, exist_ok=True)
    
    cleaned_match_team_stats, cleaned_match_player_stats, cleaned_feed = clean_matches.clean_matches()
    
    write_csv(cleaned_match_team_stats, outdir / "cleaned_matches/cleaned_match_team_stats.csv")
    write_csv(cleaned_match_player_stats, outdir / "cleaned_matches/cleaned_match_player_stats.csv")
    write_csv(cleaned_feed, outdir / "cleaned_matches/cleaned_match_feed.csv")  
    
    ## clean SoFIFA data
    cleaned_players, cleaned_team = clean_sofifa.clean_sofifa()
    write_csv(cleaned_team, outdir / "cleaned_sofifa/cleaned_team_stats.csv")
    write_csv(cleaned_players, outdir / "cleaned_sofifa/cleaned_player_stats.csv")