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


### SETUP AND UTILITY FUNCTIONS FOR SELENIUM ###

# Function to set up Selenium WebDriver with options to avoid detection and handle logging
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
    wait = WebDriverWait(driver, timeout)
    tried = []

    def _visible(el):
        try:
            return el.is_displayed() and el.is_enabled()
        except Exception:
            return False

    # Give the banner a second to mount
    driver.execute_script("window._probe = Date.now();")
    try:
        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    except TimeoutException:
        pass

    # Direct hit: common OneTrust IDs/classes
    candidates = [
        (By.ID, "onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR,
         "#onetrust-banner-sdk button#onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR, "button#onetrust-reject-all-handler"
         ),
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
            try:
                driver.execute_script("arguments[0].click();", btn)
                return True
            except Exception:
                continue

    # If not found, check for a banner container (present but hidden/animating)
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

    # Scan iframes and try inside.
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for i, frame in enumerate(iframes):
        src = (frame.get_attribute("src") or "").lower()
        name = (frame.get_attribute("name") or "").lower()
        if any(k in src + name
               for k in ("consent", "onetrust", "privacy", "cookie")):
            tried.append(f"iframe[{i}] src={src or name}")
            try:
                driver.switch_to.frame(frame)
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
                try:
                    driver.switch_to.default_content()
                except:
                    pass

    # Last resort: call OneTrust API if it exists, or remove the banner to unblock clicks
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

# Function to load the match feed by scrolling until no new content loads
def extract_match_links(driver):
    wait = WebDriverWait(driver, 10)
    
    ### Load the page
    driver.get('https://www.mlssoccer.com/schedule/scores#competition=MLS-COM-000001&club=all')
    
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))

    ### find last week's matches
    all_links = set()


    try:
        # Locate the "Previous results" button
        previous_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Previous results']"))) 
        
        # Click the button to get to last week's matches as scrape runs on Monday morning for the previous week's matches
        previous_button.click() 
        
        # Wait for the page to load after clicking
        time.sleep(3)  
        
        matches_table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'mls-c-schedule__matches')))
        if not matches_table:
            print("No matches table found on this page.")
            
        ### Extract all match links    
        hrefs = matches_table.find_elements(By.TAG_NAME, 'a')
        
        for href in hrefs:
            all_links.add(href.get_attribute('href'))
            
    except Exception as e:
        print(f"Error occurred while extracting match links: {e}")
            
    return list(all_links)


# Function to scroll by a certain amount of pixels
def js_scroll_by(driver, by):
    driver.execute_script("window.scrollBy(0, arguments[0]);", by)

# Function to scroll an element into view
def js_scroll_into_view(driver, el):
    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center', inline:'nearest'});", el)
    

# function to scrape stats cards from the team stats page using JavaScript execution to avoid issues with lazy loading and dynamic content
def scrape_cards(group, driver):
    return driver.execute_script("""
        const root = arguments[0];
        return [...root.querySelectorAll('.mls-o-stat-chart')].map(c => ({
        stat:   c.querySelector('.mls-o-stat-chart__header')?.textContent.trim() || '',
        first:  c.querySelector('.mls-o-stat-chart__first-value')?.textContent.trim() || '',
        second: c.querySelector('.mls-o-stat-chart__second-value')?.textContent.trim() || '',
        }));
    """, group)

# Function to load the full match feed by scrolling until the scroll height stabilizes, indicating all content is loaded
def load_full_feed_by_height(driver, step_px=1000, delay=1.5,
                             max_rounds=80, stable_rounds_required=10):
    prev_h = -1
    stable = 0

    for i in range(max_rounds):
        h = driver.execute_script("return document.body.scrollHeight;")

        # If we're at (or near) the footer, nudge up to trigger lazy-loading.
        scroll_y = driver.execute_script("return window.pageYOffset;")
        view_h = driver.execute_script("return window.innerHeight;")
        if scroll_y + view_h >= h - 5:
            driver.execute_script("window.scrollBy(0, arguments[0]);", -500)
            time.sleep(delay)

        # Re-check height after any footer nudge.
        h = driver.execute_script("return document.body.scrollHeight;")
        
        if h == prev_h:
            stable += 1
        else:
            stable = 0

        if stable >= stable_rounds_required:
            return h

        prev_h = h
        driver.execute_script("window.scrollBy(0, arguments[0]);", step_px)
        time.sleep(delay)

    return driver.execute_script("return document.body.scrollHeight;")



def _text(el) -> Optional[str]:
    if not el:
        return None
    t = el.get_text(" ", strip=True)
    return t if t else None