import traceback
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from mls.utils import selenium_helpers
from bs4 import BeautifulSoup


def extract_feed(driver, match_id, date):
    wait = WebDriverWait(driver, 10)
    rows = []

    try:
        # Make sure header exists (page loaded)
        title_head = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "section.mls-l-module--match-hub-header-container"))
        )
        selenium_helpers.js_scroll_into_view(driver, title_head)

        # Click Feed tab (safe click)
        feed_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space(.)='Feed'] | //a[normalize-space(.)='Feed']"))
        )
        driver.execute_script("arguments[0].click();", feed_button)

        # Wait until feed container exists before scrolling/loading
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.mls-o-match-feed")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.mls-o-match-feed__container")))

        # Now load everything (lazy load)
        selenium_helpers.load_full_feed_by_height(driver, step_px=1500, delay=0.6, max_rounds=80, stable_rounds_required=3)

        # Re-grab HTML after loading is complete
        html = driver.page_source

        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        cont = soup.select_one("div.mls-o-match-feed")
        if not cont:
            print(f"[WARN] feed container not found in HTML for match {match_id}")
            return pd.DataFrame(columns=["match_id","date","minute","title","comment","out_player","in_player"])

        events = cont.select("div.mls-o-match-feed__container")
        print(f"[FEED] parsing {len(events)} events for match {match_id}")

        for ev in events:
            minute = selenium_helpers._text(ev.select_one(".mls-o-match-feed__regular-time")) or \
                     selenium_helpers._text(ev.select_one(".mls-o-match-feed__minute"))
            title = selenium_helpers._text(ev.select_one(".mls-o-match-feed__title"))
            comment = selenium_helpers._text(ev.select_one(".mls-o-match-feed__comment"))
            out_player = selenium_helpers._text(ev.select_one(".mls-o-match-feed__sub-out .mls-o-match-feed__player"))
            in_player  = selenium_helpers._text(ev.select_one(".mls-o-match-feed__sub-in  .mls-o-match-feed__player"))

            rows.append({
                "match_id": match_id,
                "date": date,
                "minute": minute,
                "title": title,
                "comment": comment,
                "out_player": out_player,
                "in_player": in_player,
            })

    except Exception as e:
        print(f"[ERR] extract_feed failed for match {match_id}: {type(e).__name__}: {e}")
        traceback.print_exc()

    feed = pd.DataFrame(rows)
    if feed.empty:
        print(f"[WARN] Feed DataFrame is empty for match ID {match_id}")

    return feed




