import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from mls.utils.scraping import selenium_helpers
from mls.scraping.bs4 import bs_scraper as bs


def extract_players(driver, match_id, date):
    wait = WebDriverWait(driver, 10)
    
    ### scroll to top of page to ensure buttons are visible
    title_head = driver.find_element(By.CSS_SELECTOR,
                                     "section.mls-l-module--match-hub-header-container"
                                     )
    
    selenium_helpers.js_scroll_into_view(driver, title_head)
    
    ### click player stats button to load player stats page for the match with error handling to return empty dataframe if button not found or clickable
    try:
        player_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '.mls-o-buttons__segment[value="players"]')
        ))
        player_btn.click()
    except:
        print("Could not click players button")
        return pd.DataFrame()

    driver.implicitly_wait(2)
    
    ### scroll to bottom of page to load all player stats (lazy loading)
    selenium_helpers.js_scroll_by(driver, 1500)
    
    ### get page source and parse player stats tables with BeautifulSoup
    html = driver.page_source
    player_stats= bs.parse_player_stats_from_html(html, date, match_id)
    
    return player_stats

