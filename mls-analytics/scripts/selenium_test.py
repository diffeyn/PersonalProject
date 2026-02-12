from selenium.webdriver.support.ui import WebDriverWait
from mls.utils.helpers import set_up_driver
from mls.scraping.match_team_stats import extract_team_stats  # wherever it lives

def main():
    driver = set_up_driver()
    wait = WebDriverWait(driver, 10)

    try:
        link = "https://www.mlssoccer.com/competitions/mls-regular-season/2025/matches/cinvsmtl-10-18-2025/"  # replace with match link
        match_id = "debug"

        print("[1] calling extract_team_stats")
        out = extract_team_stats(driver, link, match_id)
        print("[2] returned:", type(out))


        input("Browser is open. Press Enter to quit...")
    finally:
        driver.quit()

