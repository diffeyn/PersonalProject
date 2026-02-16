import traceback
from mls.scraping.scrape_match import scrape_matches
from selenium.webdriver.support.ui import WebDriverWait
from pathlib import Path
from mls.utils.io import write_csv
from mls.scraping.scrape_sofifa import scrape_sofifa


def main():    
    outdir = Path("./tests/data/interim/matches")
    outdir.mkdir(parents=True, exist_ok=True)
 
    try:
        print("Starting to scrape matches...")
        match_team_stats, match_player_stats, match_feed, match_data = scrape_matches()
        write_csv(match_team_stats, outdir / "match_team_stats.csv")
        write_csv(match_player_stats, outdir / "match_player_stats.csv")
        write_csv(match_feed, outdir / "match_feed.csv")
        write_csv(match_data, outdir / "match_data.csv")
    except Exception as e:
        print(f"Error during scraping match stats: {e}")
        traceback.print_exc()
        return
        
    try:
        print("Scraping updated player and team data...")
        teams, players = scrape_sofifa()
        write_csv(teams, outdir / "team_stats.csv")
        write_csv(players, outdir / "player_stats.csv")
    except Exception as e:
        print(f"Error during scraping player and team data: {e}")
        traceback.print_exc()
        return
    
if __name__ == "__main__":
    main()