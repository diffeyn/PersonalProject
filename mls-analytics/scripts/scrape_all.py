import traceback
from mls.scraping.scrape_match import scrape_matches
from selenium.webdriver.support.ui import WebDriverWait
from pathlib import Path
from mls.utils import utils, helpers
from mls.utils.io import write_csv
from mls.utils.paths import DataPaths, repo_root, ensure_dir

match_links = ['https://www.mlssoccer.com/competitions/mls-regular-season/2025/matches/cltvsdc-07-16-2025/', 'https://www.mlssoccer.com/competitions/mls-regular-season/2025/matches/cinvsmtl-10-18-2025/']

def main():
    driver = helpers.set_up_driver()
    print("Driver set up successfully.")
    wait = WebDriverWait(driver, 10)
    
    outdir = Path("../data/interim/matches")
    outdir.mkdir(parents=True, exist_ok=True)
 
     
    try:
        outputs = scrape_matches(driver, match_links)
        print(type(outputs))
    except Exception as e:
        print(f"Error during scraping: {e}")
        traceback.print_exc()
        driver.quit()
        return         
        
    write_csv(outputs, outdir / "team_stats.csv")  
    
    
if __name__ == "__main__":
    main()