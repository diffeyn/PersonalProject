import traceback
from mls.scraping.scrape_match import scrape_matches
from selenium.webdriver.support.ui import WebDriverWait
from pathlib import Path
from mls.utils import utils, helpers, selenium_helpers
from mls.utils.io import write_csv
from mls.utils.paths import DataPaths, repo_root, ensure_dir

match_links = ['https://www.mlssoccer.com/competitions/mls-regular-season/2025/matches/cltvsdc-07-16-2025/', 'https://www.mlssoccer.com/competitions/mls-regular-season/2025/matches/cinvsmtl-10-18-2025/']

def main():
    driver = selenium_helpers.set_up_driver()
    print("Driver set up successfully.")
    wait = WebDriverWait(driver, 10)
    
    outdir = Path("./tests/data/interim/matches")
    outdir.mkdir(parents=True, exist_ok=True)
 
     
    try:
        print("Starting to scrape matches...")
        match_team_stats, match_player_stats, match_feed = scrape_matches(driver, match_links)
        print(f'Match output stats type: {type(match_team_stats)}, {type(match_player_stats)}, {type(match_feed)}')
    except Exception as e:
        print(f"Error during scraping match stats: {e}")
        traceback.print_exc()
        driver.quit()
        return         
        
    write_csv(match_team_stats, outdir / "team_stats.csv")
    write_csv(match_player_stats, outdir / "player_stats.csv")    
    write_csv(match_feed, outdir / "match_feed.csv")
if __name__ == "__main__":
    main()