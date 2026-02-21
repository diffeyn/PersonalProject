import pandas as pd
from selenium.common import ElementClickInterceptedException, StaleElementReferenceException, TimeoutException
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
        # Wait for the toggle region to exist (reduces "wrong hidden button" hits)
        toggle = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'section[data-react="mls-match-hub-stats-toggle"]')
        ))

        # Find the button INSIDE the toggle (more specific than global CSS)
        player_btn = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR,
            'section[data-react="mls-match-hub-stats-toggle"] button.mls-o-buttons__segment[value="players"]')
        ))

        # Scroll the actual button into view (center it so sticky headers don't cover it)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", player_btn)

        # Now wait until it's clickable (after scroll)
        player_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR,
            'section[data-react="mls-match-hub-stats-toggle"] button.mls-o-buttons__segment[value="players"]')
        ))

        try:
            player_btn.click()
        except (ElementClickInterceptedException, StaleElementReferenceException):
            # React overlays / re-render? JS click doesn't care.
            player_btn = driver.find_element(
                By.CSS_SELECTOR,
                'section[data-react="mls-match-hub-stats-toggle"] button.mls-o-buttons__segment[value="players"]'
            )
            driver.execute_script("arguments[0].click();", player_btn)

    except TimeoutException:
        print("Players toggle not found/clickable.")
        return None 
        
        
    driver.implicitly_wait(2)
    
    ### scroll to bottom of page to load all player stats (lazy loading)
    selenium_helpers.js_scroll_by(driver, 1500)
    
    ### get page source and parse player stats tables with BeautifulSoup
    html = driver.page_source
    player_stats= bs.parse_player_stats_from_html(html, date, match_id)
    
    print(player_stats.columns.tolist())
    
    return player_stats

