import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from mls.utils import selenium_helpers

def extract_player_stats(driver, match_id, date):
    wait = WebDriverWait(driver, 10)
    
    title_head = driver.find_element(By.CSS_SELECTOR,
                                     "section.mls-l-module--match-hub-header-container"
                                     )
    
    selenium_helpers.js_scroll_into_view(driver, title_head)
    
    try:
        player_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '.mls-o-buttons__segment[value="players"]')
        ))
        player_btn.click()
    except:
        print("Could not click players button")
        return pd.DataFrame()

    driver.implicitly_wait(2)
    selenium_helpers.js_scroll_by(driver, 1500)

    try:
        teams = selenium_helpers.get_player_team_blocks(driver)
    except Exception as e:
        print("ERROR grouping team blocks:", e)
        return pd.DataFrame()

    player_rows = []
    gk_rows = []


    for idx, t in enumerate(teams):
        side = "home" if idx == 0 else "away"

        # parse main
        for row in selenium_helpers.scrape_table(t["main"]):
            row.update({"side": side, "club": t["team"], "date": date})
            player_rows.append(row)

        # parse gk
        for row in selenium_helpers.scrape_table(t["gk"]):
            row.update({"side": side, "club": t["team"], "date": date})
            gk_rows.append(row)
            
    player_stats = pd.DataFrame(player_rows + gk_rows)
    
    player_stats["match_id"] = match_id
    return player_stats

