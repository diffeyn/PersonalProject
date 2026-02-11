from pathlib import Path
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


### Function to dismiss cookie popup
def dismiss_cookies(driver, timeout=8):
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

def js_scroll_by(driver, dy):
    driver.execute_script("window.scrollBy(0, arguments[0]);", dy)



def js_scroll_into_view(driver, el):
    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center', inline:'nearest'});", el)

    
def make_match_id(link: str) -> str:
    mid = link.rstrip("/").split("/")[-1].split("?")[0]
    return re.sub(r"[^A-Za-z0-9._-]+", "_", mid)

def scrape_cards(group, driver):
    return driver.execute_script("""
        const root = arguments[0];
        return [...root.querySelectorAll('.mls-o-stat-chart')].map(c => ({
        stat:   c.querySelector('.mls-o-stat-chart__header')?.textContent.trim() || '',
        first:  c.querySelector('.mls-o-stat-chart__first-value')?.textContent.trim() || '',
        second: c.querySelector('.mls-o-stat-chart__second-value')?.textContent.trim() || '',
        }));
    """, group)
    
def save_to_csv(df, filename):
    path = Path("data/github_actions") / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)