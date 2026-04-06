import traceback
from mls.scraping.selenium.scrape_match import scrape_matches
from pathlib import Path
from mls.utils.scraping.io import write_csv
from mls.scraping.bs4.scrape_sofifa import scrape_sofifa


def scrape_all():    
    outdir = Path("data/raw")
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Scrape match stats and feed
 
    try:
        match_team_stats, match_outfield_stats, match_gk_stats, match_feed, match_data = scrape_matches()
        
        outputs = {
            "team_stats": match_team_stats,
            "outfield_stats": match_outfield_stats,
            "gk_stats": match_gk_stats,
            "feed": match_feed,
            "match_data": match_data,
        }
        
        failed = [k for k, v in outputs.items() if v is None or (hasattr(v, 'empty') and v.empty)]
        
        if failed:
            print(f"Scraping failed for: {failed}")
            return

        write_csv(match_team_stats, outdir / "matches/match_team_stats.csv")
        write_csv(match_outfield_stats, outdir / "matches/match_outfield_stats.csv")
        write_csv(match_gk_stats, outdir / "matches/match_gk_stats.csv")
        write_csv(match_feed, outdir / "matches/match_feed.csv")
        write_csv(match_data, outdir / "matches/match_data.csv")
    except Exception as e:
        print(f"Error during scraping match stats: {e}")
        traceback.print_exc()
        return
        
        
    # Scrape updated player and team data from SoFIFA
    
    try:
        teams, players = scrape_sofifa()
        write_csv(players, outdir / "sofifa/sofifa_player_stats.csv")
        write_csv(teams, outdir / "sofifa/sofifa_team_stats.csv")
    except Exception as e:
        print(f"Error during scraping player and team data: {e}")
        traceback.print_exc()
        return
    
    