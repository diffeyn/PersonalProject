from __future__ import annotations
import traceback
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from mls.scraping.match_team_stats import extract_team_stats
from mls.scraping.match_player_stats import extract_player_stats
from mls.scraping.match_feed import extract_feed
from mls.utils import utils, selenium_helpers


def scrape_matches(driver, match_links):
    
    driver.get("https://www.mlssoccer.com/")
    
    driver.implicitly_wait(2)
    
    selenium_helpers.dismiss_cookies(driver)

    wait = WebDriverWait(driver, 10)
    
    combined_team_stats = pd.DataFrame()
    combined_player_stats = pd.DataFrame()
    combined_feed = pd.DataFrame()

    
    for link in match_links:
       
        match_id = utils.make_match_id(link)
        
        driver.get(link)
        
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        print(f"Navigated to {link} for match ID: {match_id}")

                
        try:
            match_team_data, date, home_team, away_team = extract_team_stats(driver, match_id)
            combined_team_stats = pd.concat([combined_team_stats, match_team_data], ignore_index=True)
        except Exception as e:
            print(f"Error extracting team stats for {match_id}: {e}")
            traceback.print_exc()
            raise            
        print('Finished scraping team stats for match:' + match_id)
        
        
        try:
            match_player_data = extract_player_stats(driver, match_id, date)
            combined_player_stats = pd.concat([combined_player_stats, match_player_data], ignore_index=True)
        except Exception as e:
            print(f"Error extracting player stats for {match_id}: {e}")
            traceback.print_exc()
            raise
        print('Finished scraping player stats for match:' + match_id)
        
        
        try:
            match_feed_data = extract_feed(driver, match_id, date)
            combined_feed = pd.concat([combined_feed, match_feed_data], ignore_index=True)
        except Exception as e:
            print(f"Error extracting match feed for {match_id}: {e}")
            traceback.print_exc()
            raise
        print('Finished scraping match feed for match:' + match_id)
        

    driver.quit()

    return combined_team_stats, combined_player_stats, combined_feed