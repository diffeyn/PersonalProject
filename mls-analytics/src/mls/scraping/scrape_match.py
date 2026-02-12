from __future__ import annotations
import traceback
import pandas as pd
from mls.scraping.match_team_stats import extract_team_stats
from mls.utils import utils


def scrape_matches(driver, match_links):
    
    for link in match_links:
        combined_team_stats = pd.DataFrame()
        
        match_id = utils.make_match_id(link)
                
        try:
            team_data = extract_team_stats(driver, link, match_id)
            combined_team_stats = pd.concat([combined_team_stats, team_data], ignore_index=True)
        except Exception as e:
            print(f"Error extracting team stats for {match_id}: {e}")
            traceback.print_exc()
            raise            
        print('---')

    driver.quit()

    return combined_team_stats