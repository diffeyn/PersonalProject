import time
import random
import pandas as pd
import traceback
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from __future__ import annotations
from mls.utils.scraping import selenium_helpers
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from mls.scraping.selenium.match_team_stats import extract_team_stats
from mls.scraping.selenium.match_player_stats import extract_players
from mls.scraping.selenium.match_feed import extract_feed
from mls.utils.scraping import hashing


def scrape_matches():
    
    driver = selenium_helpers.set_up_driver()
    wait = WebDriverWait(driver, 10)
    
    ## navigate to most recent schedule page (weekly))
    driver.get("https://www.mlssoccer.com/schedule/scores#competition=MLS-COM-000001&club=all")
    
    driver.implicitly_wait(2)
    
    ## dismiss cookies if prompted
    selenium_helpers.dismiss_cookies(driver)

    wait = WebDriverWait(driver, 3)
    
    ### calendar button for navigating to previous weeks of matches
    button_calendar = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'mls-o-buttons__two-way')))
        
    #### press button to navigate to previous week of matches as scrapehappens monday morning for the previous week's matches    
    button_prev = button_calendar.find_element(By.XPATH, ".//button[@value='prev']")    
    
    ## scrape match links from schedule page
    match_links = selenium_helpers.extract_match_links(driver)
    
    if match_links is None or len(match_links) == 0:
        print("No match links found. Exiting.")
        driver.quit()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    
    ### initialize empty dataframes to hold combined data across matches
    combined_team_stats = pd.DataFrame()
    combined_player_stats = pd.DataFrame()
    combined_feed = pd.DataFrame()
    combined_match_data = pd.DataFrame()
    
    ### loop through match links and extract data for each match
    count = 1
    

    MAX_ROUNDS = 3
    COOLDOWN_SECONDS = 30  # "retry later" delay between rounds


    remaining_links = list(match_links)
    failed = []  # keep records of failures

    for round_num in range(1, MAX_ROUNDS + 1):
        print(f"\n=== Match scrape round {round_num}/{MAX_ROUNDS} | {len(remaining_links)} matches ===")

        next_remaining = []

        for link in remaining_links:
            match_id = hashing.make_match_id(link)

            try:
                driver.get(link)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                # --- team stats + match meta ---
                match_team_data, date, home_team, away_team, home_team_score, away_team_score = extract_team_stats(driver, match_id)
                combined_team_stats = pd.concat([combined_team_stats, match_team_data], ignore_index=True)

                match_data = pd.DataFrame([{
                    "match_id": match_id,
                    "date": date,
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_team_score": home_team_score,
                    "away_team_score": away_team_score
                }])
                combined_match_data = pd.concat([combined_match_data, match_data], ignore_index=True)

                # --- player stats ---
                match_player_data = extract_players(driver, match_id, date)
                combined_player_stats = pd.concat([combined_player_stats, match_player_data], ignore_index=True)

                # --- feed ---
                match_feed_data = extract_feed(driver, match_id, date)
                match_feed_data["home_team"] = home_team
                match_feed_data["away_team"] = away_team
                combined_feed = pd.concat([combined_feed, match_feed_data], ignore_index=True)

            except Exception as e:
                print(f"[FAILED] match_id={match_id} | url={link} | {e}")
                traceback.print_exc()
                failed.append({"round": round_num, "match_id": match_id, "url": link, "error": str(e)})
                next_remaining.append(link)
                continue

        if not next_remaining:
            print("All matches scraped successfully.")
            break

        remaining_links = next_remaining

        if round_num < MAX_ROUNDS:
            sleep_time = COOLDOWN_SECONDS + random.random() * 10
            print(f"Cooling down {sleep_time:.1f}s before retrying failed matches...")
            time.sleep(sleep_time)

    # close driver after processing
    driver.quit()

    # optional: save failed list somewhere
    # pd.DataFrame(failed).to_csv("data/raw/match_scrape_failures.csv", index=False)

    return combined_team_stats, combined_player_stats, combined_feed, combined_match_data