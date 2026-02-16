from __future__ import annotations
import traceback
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from mls.scraping.selenium.match_team_stats import extract_team_stats
from mls.scraping.selenium.match_player_stats import extract_player_stats
from mls.scraping.selenium.match_feed import extract_feed
from mls.utils import utils, selenium_helpers


def scrape_matches():
    
    driver = selenium_helpers.set_up_driver()
    print("Driver set up successfully.")
    wait = WebDriverWait(driver, 10)
    
    driver.get("https://www.mlssoccer.com/schedule/scores#competition=MLS-COM-000001&club=all")
    
    driver.implicitly_wait(2)

        
    print("Clicked on previous button 19 times.")
    
    selenium_helpers.dismiss_cookies(driver)

    wait = WebDriverWait(driver, 3)
    
        ### button with class
    button_calendar = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'mls-o-buttons__two-way')))
    
    print("found calendar button.")
    
    #### press button with value prev with class mls-o-buttons__two-way
    
    button_prev = button_calendar.find_element(By.XPATH, ".//button[@value='prev']")
    
    ### click the button 19 times to go back to the start of the season
    for _ in range(19):
        button_prev.click()
    
    # match_links = selenium_helpers.extract_match_links(driver)
    
    match_links = selenium_helpers.extract_match_links(driver)
    print(f"Extracted {len(match_links)} match links.")
    
    combined_team_stats = pd.DataFrame()
    combined_player_stats = pd.DataFrame()
    combined_feed = pd.DataFrame()

    
    for link in match_links:
       
        match_id = utils.make_match_id(link)
        
        driver.get(link)
        
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        print(f"Navigated to {link} for match ID: {match_id}")
                
        try:
            match_team_data, date, home_team, away_team, home_team_score, away_team_score = extract_team_stats(driver, match_id)
            combined_team_stats = pd.concat([combined_team_stats, match_team_data], ignore_index=True)
        except Exception as e:
            print(f"Error extracting team stats for {match_id}: {e}")
            traceback.print_exc()
            raise            
        
        match_data = pd.DataFrame([{
            "match_id": match_id,
            "date": date,
            "home_team": home_team,
            "away_team": away_team,
            "home_team_score": home_team_score,
            "away_team_score": away_team_score}])
                                  
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
            match_feed_data['home_team'] = home_team
            match_feed_data['away_team'] = away_team
            combined_feed = pd.concat([combined_feed, match_feed_data], ignore_index=True)
        except Exception as e:
            print(f"Error extracting match feed for {match_id}: {e}")
            traceback.print_exc()
            raise
        print('Finished scraping match feed for match:' + match_id)
        

    driver.quit()

    return combined_team_stats, combined_player_stats, combined_feed, match_data