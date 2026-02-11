import hashlib
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import time
import math
from mls.utils import utils

def set_up_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--force-device-scale-factor=1")

    # Look human
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )



    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    })
    
    return driver

###--------------EXTRACT MATCH LINKS----------------###
def extract_match_links(driver, url):
    wait = WebDriverWait(driver, 10)
    
    ### Load the page
    driver.get(url)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))

    ### Dismiss cookie popup if present
    utils.dismiss_cookies(driver)
    

    ### find last week's matches
    all_links = set()


    try:
        previous_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Previous results']"))) # Locate the "Previous results" button

        previous_button.click() # Click the button to get to last week's matches
        
        time.sleep(5)
        
        matches_table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'mls-c-schedule__matches')))
        if not matches_table:
            print("No matches table found on this page.")
            
        ### Extract all match links    
        hrefs = matches_table.find_elements(By.TAG_NAME, 'a')
        
        for href in hrefs:
            all_links.add(href.get_attribute('href'))
            
    except Exception as e:
        print(f"Error occurred: {e}")
            
    return list(all_links)


###--------------EXTRTACT MATCH ID -----------------###

def create_match_id(link):
    if (
        link is None
        or (isinstance(link, float) and math.isnan(link))
        or str(link).strip() == ''
        or str(link).strip().lower() == 'nan'
    ):
        return None

    return hashlib.md5(
        link.rstrip('/').split('/')[-1].split('?')[0].encode('utf-8')
    ).hexdigest()[:8]
    
###--------------HELPER FUNCTIONS FOR STATS-----------------###

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

def add_match_id(data, match_id):
    if isinstance(data, list):
        return [dict(item, match_id=match_id) for item in data]
    elif isinstance(data, dict):
        return dict(data, match_id=match_id)
    elif hasattr(data, "assign"):  # pandas DataFrame
        return data.assign(match_id=match_id)
    else:
        return data
    
    
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
    
###--------------EXTRACT MATCH DATA-----------------###
    
def extract_feed(driver, link, match_id):
    wait = WebDriverWait(driver, 5)
    driver.get(link)
    feed = []
    
    try:
        feed_button = driver.find_element(By.XPATH,
                            "//*[normalize-space(text())='Feed']")

        feed_button.click()
        
        utils.js_scroll_by(driver, 900)

        utils.js_scroll_by(driver, 3000)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[normalize-space(text())='First half begins.']")))

        first_half = driver.find_element(By.XPATH, "//*[normalize-space(text())='First half begins.']")

        utils.js_scroll_into_view(driver, first_half)
        if not first_half:
            print(f"First half element not found for link {link}")
        try:            
            cont = driver.find_element(By.CSS_SELECTOR, 'div[class="mls-o-match-feed"]')

            events = cont.find_elements(By.CSS_SELECTOR, 'div[class="mls-o-match-feed__container"]')

            for event in events:
                minute_el = event.find_elements(By.CSS_SELECTOR, ".mls-o-match-summary__regular-time")
                minute = minute_el[0].text.strip() if minute_el else None

                title_el = event.find_elements(By.CSS_SELECTOR, ".mls-o-match-feed__title")
                title = title_el[0].text.strip() if title_el else None

                comment_el = event.find_elements(By.CSS_SELECTOR, ".mls-o-match-feed__comment")
                comment = comment_el[0].text.strip() if comment_el else None
                
                players_wrap = event.find_elements(By.XPATH, ".//*[contains(@class,'mls-o-match-feed__players')]")

                out_player = None
                in_player = None

                if players_wrap:
                    out_nodes = players_wrap[0].find_elements(
                        By.CSS_SELECTOR, ".mls-o-match-feed__sub-out .mls-o-match-feed__player"
                    )
                    in_nodes = players_wrap[0].find_elements(
                        By.CSS_SELECTOR, ".mls-o-match-feed__sub-in .mls-o-match-feed__player"
                    )

                    out_player = out_nodes[0].text.strip() if out_nodes and out_nodes[0].text.strip() else None
                    in_player  = in_nodes[0].text.strip()  if in_nodes and in_nodes[0].text.strip()  else None
                else:
                    pass

                feed.append({
                    'match_id': match_id,
                    'minute': minute,
                    'title': title,
                    'comment': comment,
                    'out_player': out_player,
                    'in_player': in_player
                })
                if not feed:
                    print(f"No feed events found for link {link}")
        except Exception as e:
            print(f"Error extracting feed events for link {link}: {e}")
    except Exception as e:
        print(f"Error extracting feed for link {link}: {e}")
    return feed


def extract_player_stats(driver, link, match_id):
    wait = WebDriverWait(driver, 10)

    driver.get(link)
    time.sleep(3)

    try:
        date = driver.find_element(
            By.XPATH,
            "//div[contains(@class, 'mls-c-blockheader__subtitle')]"
        ).text.strip()
    except:
        date = None

    try:
        main_body = driver.find_element(By.TAG_NAME, 'main')
        stats_bttn = main_body.find_element(By.LINK_TEXT, 'Stats')
        stats_bttn.click()
    except Exception as e:
        print("Error clicking Stats:", e)

    try:
        player_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '.mls-o-buttons__segment[value="players"]')
        ))
        player_btn.click()
    except:
        print("Could not click players button")
        return pd.DataFrame()

    time.sleep(1)
    utils.js_scroll_by(driver, 1500)
    time.sleep(1)

    try:
        teams = get_player_team_blocks(driver)
    except Exception as e:
        print("ERROR grouping team blocks:", e)
        return pd.DataFrame()

    player_rows = []
    gk_rows = []


    for idx, t in enumerate(teams):
        side = "home" if idx == 0 else "away"

        # parse main
        for row in scrape_table(t["main"]):
            row.update({"side": side, "club": t["team"], "date": date})
            player_rows.append(row)

        # parse gk
        for row in scrape_table(t["gk"]):
            row.update({"side": side, "club": t["team"], "date": date})
            gk_rows.append(row)
            
        # add match_id column
        player_rows = add_match_id(player_rows, match_id)
        gk_rows = add_match_id(gk_rows, match_id)

    return pd.DataFrame(player_rows + gk_rows)


def extract_team_stats(driver, link, match_id):
    """
    Extract team statistics from an MLS match page using Selenium WebDriver.
    This function navigates to a match statistics page, clicks through the stats section,
    and scrapes various categories of team performance data including general stats,
    shooting, passing, possession, and expected goals (xG).
    Args:
        driver: Selenium WebDriver instance used for web scraping.
        link (str): URL of the match page to scrape statistics from.
        match_id: Unique identifier for the match (can be str or int).
    Returns:
        pandas.DataFrame: A DataFrame containing all scraped statistics with columns:
            - stat_name: Name of the statistic
            - home_value: Value for the home team
            - away_value: Value for the away team
            - category: Category of stat ('general', 'shooting', 'passing', 'possession', 'xg')
            - match_id: The provided match identifier
            - date: Date of the match
            - home_team: Short name of the home team
            - away_team: Short name of the away team
    Raises:
        Exception: Various exceptions may occur during scraping (caught and printed):
            - Element not found errors
            - Timeout errors
            - General scraping errors for each stats category
    Notes:
        - Uses explicit waits (WebDriverWait) with 10-second timeout
        - Scrolls elements into view for proper interaction
        - Possession stats include tip_id, possession percentages, and advantages
        - Each stats category is scraped separately with individual error handling
        - If extraction fails for a particular section, it prints an error and continues
    """
    
    wait = WebDriverWait(driver, 10)
    
    driver.get(link)
    
    general_stats = []
    shooting_stats = []
    passing_stats = []
    possession_stats = []
    xg_stats = []
    date = ''
    home_team = ''
    away_team = ''
    
    main_body = driver.find_element(By.TAG_NAME, 'main')
    stats_bttn = main_body.find_element(By.LINK_TEXT, 'Stats')
    
    title_head = driver.find_element(By.CSS_SELECTOR,
                                     "section.mls-l-module--match-hub-header-container"
                                     )
    teams = title_head.find_element(By.CSS_SELECTOR, 'div.mls-c-matchhub-tile')

    try:
        home_team = title_head.find_element(
            By.CSS_SELECTOR,
            "div.mls-c-club.--home span.mls-c-club__shortname"
            ).text.strip()

        away_team = title_head.find_element(
            By.CSS_SELECTOR,
            "div.mls-c-club.--away span.mls-c-club__shortname"
        ).text.strip()
        
    except:
        print("Could not extract team names.")

    try:
        date = driver.find_element(By.XPATH, "//div[contains(@class, 'mls-c-blockheader__subtitle')]").text.strip()
        stats_bttn.click()

        try:
            general_cont = wait.until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '//section[contains(@class,"mls-l-module--stats-comparison")'
                    ' and contains(@class,"mls-l-module--general")'
                    ' and not(contains(@style,"display: none"))]')))


            utils.js_scroll_into_view(driver, general_cont)
            general_cards = utils.scrape_cards(general_cont, driver)

            for it in general_cards:
                general_stats.append({
                    'stat_name': it['stat'],
                    'home_value': it['first'],
                    'away_value': it['second']
                })
        except Exception as e:
            print(f"Error occurred while scraping general stats: {e}")

        try:
            clubs_wrap = wait.until(
                EC.visibility_of_element_located((
                    By.XPATH,
                    '//section[contains(@class,"d3-l-section-row")][@data-toggle="clubs" and not(contains(@style,"display: none"))]'
                )))

            shooting_cont = clubs_wrap.find_element(
                By.XPATH,
                './/section[contains(@class,"mls-l-module--shooting-breakdown")]'
            )

            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});",
                shooting_cont)

            shooting_cards = utils.scrape_cards(shooting_cont, driver)

            for it in shooting_cards:
                shooting_stats.append({
                    'stat_name': it['stat'],
                    'home_value': it['first'],
                    'away_value': it['second']
                })

        except Exception as e:
            print(f"Error occurred while scraping shooting stats: {e}")

        try:
            passing_cont = driver.find_element(By.XPATH, '//section[contains(@class,"passing-breakdown")]')

            passing_cards = utils.scrape_cards(passing_cont, driver)
            for it in passing_cards:
                passing_stats.append({
                    'stat_name': it['stat'],
                    'home_value': it['first'],
                    'away_value': it['second']
                })

        except Exception as e:
            print(f"Error occurred while scraping passing stats: {e}")

        try:
            possession_cont = driver.find_element(By.XPATH, '//section[contains(@class,"--possession")]')
            bar_cont = possession_cont.find_element(By.XPATH, './/*[contains(@class,"mls-o-possession__intervals")]')

            driver.execute_script(
                "arguments[0].scrollIntoView({block:'center'});",
                bar_cont)


            for bar in bar_cont.find_elements(By.XPATH, './/div[contains(@class,"mls-o-possession__average-intervals")]'):
                tip_id = bar.get_attribute('data-for')

                tooltips = bar.find_elements(By.XPATH, './/div[contains(@class,"__react_component_tooltip")]')

                tip = wait.until(EC.presence_of_element_located((By.ID, tip_id)))

                spans = tip.find_elements(By.XPATH, './/span')

                texts = [s.get_attribute('textContent').strip() for s in spans]
                texts = [t for t in texts if t and t.upper() != 'SKIP TO MAIN CONTENT']

                if len(texts) >= 4:
                    home_poss, home_adv, away_poss, away_adv = texts[:4]
                else:
                    home_poss = home_adv = away_poss = away_adv = None

                possession_stats.append({
                    'tip_id': tip_id,
                    'home_possession': home_poss,
                    'home_advantage': home_adv,
                    'away_possession': away_poss,
                    'away_advantage': away_adv
                })
        except Exception as e:
            print(f"Error occurred while scraping possession stats: {e}")

        try:
            xg_mod_xpath = (
                '//section[@data-toggle="clubs" and not(contains(@style,"display: none"))]'
                '//section[contains(@class,"mls-l-module--expected-goals")]'
            )
            xg_mod = wait.until(EC.visibility_of_element_located((By.XPATH, xg_mod_xpath)))

            groups = xg_mod.find_elements(
                By.CSS_SELECTOR,
                '.mls-o-expected-goals__chart-group, .mls-o-expected-goals__club-group'
            )
            chart_group = next(
                (g for g in groups if 'mls-o-expected-goals__chart-group' in (g.get_attribute('class') or '')),
                None
            )
            if chart_group is None:
                raise Exception("xG chart-group not found")

            # ensure cards exist
            wait.until(lambda d: any(
                e.is_displayed() for e in chart_group.find_elements(By.CSS_SELECTOR, '.mls-o-stat-chart')
            ))

            for card in chart_group.find_elements(By.CSS_SELECTOR, '.mls-o-stat-chart'):
                header = card.find_element(By.CSS_SELECTOR,  '.mls-o-stat-chart__header')
                first  = card.find_element(By.CSS_SELECTOR,  '.mls-o-stat-chart__first-value')
                second = card.find_element(By.CSS_SELECTOR,  '.mls-o-stat-chart__second-value')

                stat_name  = (header.text or header.get_attribute('textContent') or '').strip()
                home_value = (first.text  or first.get_attribute('textContent')  or '').strip()
                away_value = (second.text or second.get_attribute('textContent') or '').strip()

                xg_stats.append({
                    'stat_name': stat_name,
                    'home_value': home_value,
                    'away_value': away_value
                })
        except Exception as e:
            print(f"Error occurred while scraping expected goals stats: {e}")

    except Exception as e:
        print(f"Error occurred while scraping stats: {e}")
        pass

        
    general_stats_df = pd.DataFrame(general_stats);  general_stats_df["category"] = "general"
    shooting_stats_df = pd.DataFrame(shooting_stats); shooting_stats_df["category"] = "shooting"
    passing_stats_df = pd.DataFrame(passing_stats);   passing_stats_df["category"] = "passing"
    possession_stats_df = pd.DataFrame(possession_stats); possession_stats_df["category"] = "possession"
    expected_goals_stats_df = pd.DataFrame(xg_stats); expected_goals_stats_df["category"] = "xg"
    
    all_stats = pd.concat(
        [general_stats_df, shooting_stats_df, passing_stats_df, possession_stats_df, expected_goals_stats_df],
        axis=0, ignore_index=True
    )

    all_stats['match_id'] = match_id
    all_stats['date'] = date
    all_stats['home_team'] = home_team
    all_stats['away_team'] = away_team
    return all_stats



def extract_match_data(links, driver):

    latest_stats = []
    latest_player_stats = []
    latest_feed = []

    for link in links:
        if (link is None or (isinstance(link, float) and math.isnan(link))
                or str(link).strip() == '' or str(link).strip().lower() == 'nan'):
            print(f"[skip] bad link: {link!r}")
            continue
        
        raw_id = link.rstrip('/').split('/')[-1].split('?')[0]
        match_id = hashlib.md5(raw_id.encode()).hexdigest()[:8]

        feed = extract_feed(driver, link, match_id)        
        team_stats = extract_team_stats(driver, link, match_id)
        player_stats = extract_player_stats(driver, link, match_id)        

        latest_team_stats.append(team_stats)
        latest_player_stats.append(player_stats)
        latest_feed.append(feed)

    latest_stats_df = pd.concat(latest_team_stats, axis=0, ignore_index=True) if latest_team_stats else pd.DataFrame(columns=['match_id'])
    latest_player_stats_df = pd.concat(latest_player_stats, axis=0, ignore_index=True) if latest_player_stats else pd.DataFrame(columns=['match_id'])
    latest_feed_df = pd.concat(latest_feed, axis=0, ignore_index=True) if latest_feed else pd.DataFrame(columns=['match_id'])
    
    driver.quit()

    return latest_stats_df, latest_player_stats_df, latest_feed_df



