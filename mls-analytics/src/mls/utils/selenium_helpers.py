from __future__ import annotations
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from typing import List, Dict, Optional
import time



def set_up_driver():
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    
    log_file = open("chromedriver.log", "w", encoding="utf-8")
    service = Service(log_output=log_file) 

    driver = webdriver.Chrome(service=service, options=options)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    
    return driver


### Function to dismiss cookie popup
def dismiss_cookies(driver, timeout=8):
    """
    Attempts to dismiss cookie consent banners on a web page, specifically targeting OneTrust banners.
    This function tries multiple strategies to find and click cookie acceptance buttons:
    1. Direct selector matching for common OneTrust button IDs/classes
    2. Searching within the OneTrust banner container
    3. Switching into iframes that might contain consent dialogs
    4. Using OneTrust's JavaScript API or removing the banner as a last resort
    Args:
        driver: Selenium WebDriver instance used to interact with the page
        timeout (int, optional): Maximum time in seconds to wait for elements. Defaults to 8.
    Returns:
        bool: True if a cookie banner was successfully dismissed, False otherwise.
            Also prints debug information about attempted selectors if unsuccessful.
    Note:
        - The function attempts non-intrusive methods first (clicking visible buttons)
        - Falls back to JavaScript execution for stubborn elements
        - Handles iframe switching and ensures driver context is restored
        - Designed to be resilient to various OneTrust implementation patterns
    """
    wait = WebDriverWait(driver, timeout)
    tried = []

    def _visible(el):
        try:
            return el.is_displayed() and el.is_enabled()
        except Exception:
            return False

    # 0) Give the banner a second to mount
    driver.execute_script("window._probe = Date.now();")
    try:
        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    except TimeoutException:
        pass

    # 1) Direct hit: common OneTrust IDs/classes
    candidates = [
        (By.ID, "onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR,
         "#onetrust-banner-sdk button#onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR, "button#onetrust-reject-all-handler"
         ),  # sometimes only Reject is visible first
        (By.CSS_SELECTOR, "[data-testid='onetrust-accept-btn-handler']"),
        (By.XPATH,
         "//button[contains(@id,'accept') and contains(translate(., 'ACEPT','acept'),'accept')]"
         ),
        (By.XPATH,
         "//button[contains(@aria-label,'Accept') or contains(normalize-space(.),'Accept')]"
         ),
    ]
    for how, what in candidates:
        tried.append(f"{how}={what}")
        try:
            btn = driver.find_element(how, what)
            if _visible(btn):
                btn.click()
                return True
        except NoSuchElementException:
            continue
        except WebDriverException:
            # Try JS click if the element exists but normal click fails
            try:
                driver.execute_script("arguments[0].click();", btn)
                return True
            except Exception:
                continue

    # 2) If not found, check for a banner container (present but hidden/animating)
    try:
        banner = driver.find_element(By.ID, "onetrust-banner-sdk")
        if _visible(banner):
            try:
                btn = banner.find_element(By.CSS_SELECTOR,
                                          "button[id*='accept']")
                driver.execute_script("arguments[0].click();", btn)
                return True
            except Exception:
                pass
    except NoSuchElementException:
        pass

    # 3)Scan iframes and try inside.
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for i, frame in enumerate(iframes):
        # quick filter to avoid costly switches
        src = (frame.get_attribute("src") or "").lower()
        name = (frame.get_attribute("name") or "").lower()
        if any(k in src + name
               for k in ("consent", "onetrust", "privacy", "cookie")):
            tried.append(f"iframe[{i}] src={src or name}")
            try:
                driver.switch_to.frame(frame)
                # try common selectors again inside this frame
                for how, what in candidates:
                    try:
                        btn = WebDriverWait(driver, 2).until(
                            EC.presence_of_element_located((how, what)))
                        if _visible(btn):
                            driver.execute_script(
                                "arguments[0].scrollIntoView({block:'center'});",
                                btn)
                            try:
                                btn.click()
                            except Exception:
                                driver.execute_script("arguments[0].click();",
                                                      btn)
                            driver.switch_to.default_content()
                            return True
                    except Exception:
                        continue
                driver.switch_to.default_content()
            except Exception:
                # ensure we’re back
                try:
                    driver.switch_to.default_content()
                except:
                    pass

    # 4) Last resort: call OneTrust API if it exists, or remove the banner to unblock clicks
    try:
        ok = driver.execute_script("""
            if (window.OneTrust && OneTrust.AcceptAll) { OneTrust.AcceptAll(); return true; }
            const b = document.getElementById('onetrust-banner-sdk');
            if (b) { b.remove(); return 'removed'; }
            return false;
        """)
        if ok:
            return True
    except Exception:
        pass

    print("[cookies] Could not find/close cookie banner. Tried:",
          *tried,
          sep="\n - ")
    return False



### FUNCTIONS FOR SELENIUM SCRAPING OF MATCH DATA ###

def js_scroll_by(driver, by):
    driver.execute_script("window.scrollBy(0, arguments[0]);", by)


def js_scroll_into_view(driver, el):
    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center', inline:'nearest'});", el)
    
    
import time

def load_full_feed_by_height(driver, step_px=1500, delay=0.6,
                             max_rounds=80, stable_rounds_required=3):
    prev_h = -1
    stable = 0

    for i in range(max_rounds):
        h = driver.execute_script("return document.body.scrollHeight;")
        print(f"[FEED] round {i+1}: height={h} stable={stable}")

        if h == prev_h:
            stable += 1
        else:
            stable = 0

        if stable >= stable_rounds_required:
            print("[FEED] scrollHeight stabilized")
            return h

        prev_h = h
        driver.execute_script("window.scrollBy(0, arguments[0]);", step_px)
        time.sleep(delay)

    return driver.execute_script("return document.body.scrollHeight;")


def scrape_cards(group, driver):
    return driver.execute_script("""
        const root = arguments[0];
        return [...root.querySelectorAll('.mls-o-stat-chart')].map(c => ({
        stat:   c.querySelector('.mls-o-stat-chart__header')?.textContent.trim() || '',
        first:  c.querySelector('.mls-o-stat-chart__first-value')?.textContent.trim() || '',
        second: c.querySelector('.mls-o-stat-chart__second-value')?.textContent.trim() || '',
        }));
    """, group)
    

def get_player_team_blocks(driver):

    section = driver.find_element(
        By.CSS_SELECTOR,
        "div.mls-c-stats.mls-c-stats--match-hub-player-stats"
    )

    elems = section.find_elements(By.XPATH, "./*")

    teams = []
    i = 0

    while i < len(elems):

        el = elems[i]

        if "mls-c-stats__club-abbreviation" in el.get_attribute("class"):
            team = el.text.strip()

            main = None
            gk = None

            j = i + 1
            while j < len(elems):
                if "mls-c-stats__table" in elems[j].get_attribute("class"):
                    main = elems[j].find_element(By.CSS_SELECTOR, "table")
                    break
                j += 1

            j = j + 1
            while j < len(elems):
                if "mls-o-match-hub-container__mt-25" in elems[j].get_attribute("class"):
                    gk = elems[j].find_element(By.CSS_SELECTOR, "table")
                    break
                j += 1

            teams.append({
                "team": team,
                "main": main,
                "gk": gk
            })

            i = j
        else:
            i += 1

    return teams
    
    
def scrape_table(table_el):
    rows = []

    header_cells = table_el.find_elements(By.CSS_SELECTOR, "thead .mls-o-table__header")
    headers = [h.text.strip() for h in header_cells]

    for tr in table_el.find_elements(By.CSS_SELECTOR, "tbody .mls-o-table__row"):
        cells = tr.find_elements(By.CSS_SELECTOR, ".mls-o-table__cell")
        values = [c.text.strip() for c in cells]

        if len(values) < len(headers):
            values += [""] * (len(headers) - len(values))
        elif len(values) > len(headers):
            values = values[:len(headers)]

        rows.append(dict(zip(headers, values)))

    return rows



def _text(el) -> Optional[str]:
    if not el:
        return None
    t = el.get_text(" ", strip=True)
    return t if t else None