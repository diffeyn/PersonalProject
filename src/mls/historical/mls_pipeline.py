"""
pipeline.py — MLS data pipeline: scrape, clean, save, upload
-----------------------------------------------------------------
Modes:
    --historical   Scrape all matches from a URL list CSV
    --weekly       Scrape last week's matches (default)
    --backfill     Discover and add missing URLs since a given date

Usage:
    python pipeline.py --historical --url-list data/raw/match_urls.csv --batch-size 50
    python pipeline.py --weekly
    python pipeline.py --backfill --stop-date 2026-02-21
"""

import argparse
import random
import time
import traceback
from datetime import datetime

from helpers import *
import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import ElementClickInterceptedException, StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait



MAX_ROUNDS       = 3
COOLDOWN_SECONDS = 30


# ─────────────────────────────────────────────
# Extractors — TODO: fill in
# ─────────────────────────────────────────────

def extract_team_stats(driver, match_id):
    
    wait = WebDriverWait(driver, 10)
    
    ### wait for main body to load to ensure page is ready before scraping
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
    general_stats = []
    shooting_stats = []
    passing_stats = [] 
    possession_stats = []
    xg_stats = []
    date = ''
    home_team = ''
    away_team = ''
    home_score = None
    away_score = None
    
    main_body = driver.find_element(By.TAG_NAME, 'main')
    try:
        
        ### extract match header info (teams, score, date) for context in team stats dataset and to link with other datasets using match_id
        hub = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "section[data-bucket-name='match-header']")
        ))

        home_team = wait.until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "section[data-bucket-name='match-header'] .mls-c-club.--home .mls-c-club__shortname")
        )).text.strip()

        away_team = wait.until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "section[data-bucket-name='match-header'] .mls-c-club.--away .mls-c-club__shortname")
        )).text.strip()

        scores = hub.find_elements(By.CSS_SELECTOR, ".mls-c-scorebug__score")
        home_score = scores[0].text.strip() if len(scores) > 0 else None
        away_score = scores[1].text.strip() if len(scores) > 1 else None
        date = hub.find_element(By.XPATH, "//div[contains(@class, 'mls-c-blockheader__subtitle')]").text.strip()
        
        date = clean_mls_date(date, 2025)

    except Exception as e:
        print(f"[ERR] extract_team failed for match {match_id}: {type(e).__name__}: {e}")
        traceback.print_exc()
    try:
        try:
            ### navigate to stats tab for the match to access team stats data
            stats_bttn = main_body.find_element(By.LINK_TEXT, 'Stats')

            stats_bttn.click()
            
            ### wait for stats content to load before attempting to scrape data            
            general_cont = wait.until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '//section[contains(@class,"mls-l-module--stats-comparison")'
                    ' and contains(@class,"mls-l-module--general")'
                    ' and not(contains(@style,"display: none"))]')))
            
            ### scroll to general stats section to ensure all content is loaded before scraping and extract general stats cards data
            js_scroll_into_view(driver, general_cont)
            
            general_cards = scrape_cards(general_cont, driver)

            for it in general_cards:
                general_stats.append({
                    'stat_name': it['stat'],
                    'home_value': it['first'],
                    'away_value': it['second']
                })
        except Exception as e:
            print(f"Error occurred while scraping general stats: {e}")
            
        ### repeat similar process for shooting, passing, possession, and expected goals stats with error handling to continue scraping other stats even if one category fails
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

            js_scroll_into_view(driver, shooting_cont)

            shooting_cards = scrape_cards(shooting_cont, driver)

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

            passing_cards = scrape_cards(passing_cont, driver)
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

            js_scroll_into_view(driver, bar_cont)


            for bar in bar_cont.find_elements(By.XPATH, './/div[contains(@class,"mls-o-possession__average-intervals")]'):
                tip_id = bar.get_attribute('data-for')

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

    ### combine all stats into single dataframe in long format with category column to differentiate stat types for easier analysis and link with other datasets using match_id, date, and team names
    
    general_stats_df = pd.DataFrame(general_stats);  general_stats_df["category"] = "general"
    shooting_stats_df = pd.DataFrame(shooting_stats); shooting_stats_df["category"] = "shooting"
    passing_stats_df = pd.DataFrame(passing_stats);   passing_stats_df["category"] = "passing"
    possession_stats_df = pd.DataFrame(possession_stats); possession_stats_df["category"] = "possession"
    expected_goals_stats_df = pd.DataFrame(xg_stats); expected_goals_stats_df["category"] = "xg"


    all_stats = pd.concat([
        general_stats_df,
        shooting_stats_df,
        passing_stats_df,
        possession_stats_df,
        expected_goals_stats_df
    ], ignore_index=True)
    
    ### add match_id, date, and team names to stats dataframe for context and to link with other datasets
    all_stats['match_id'] = match_id
    all_stats['date'] = date
    all_stats['home_team'] = home_team
    all_stats['away_team'] = away_team
    
    
    return all_stats, date, home_team, away_team, home_score, away_score


def extract_feed(driver, match_id, date):
    wait = WebDriverWait(driver, 10)
    rows = []

    try:
        time.sleep(2)
        # Make sure header exists (page loaded)
        title_head = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "section.mls-l-module--match-hub-header-container"))
        )
        
        js_scroll_into_view(driver, title_head)

        # Click Feed tab (safe click)
        feed_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space(.)='Feed'] | //a[normalize-space(.)='Feed']"))
        )
        driver.execute_script("arguments[0].click();", feed_button)

        # Wait until feed container exists before scrolling/loading
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.mls-o-match-feed")))
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.mls-o-match-feed__container")))

        # Now load everything (lazy load)
        load_full_feed_by_height(driver, step_px=1500, delay=0.6, max_rounds=80, stable_rounds_required=3)

        # Re-grab HTML after loading is complete
        html = driver.page_source

        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        cont = soup.select_one("div.mls-o-match-feed")
        if not cont:
            print(f"[WARN] feed container not found in HTML for match {match_id}")
            return pd.DataFrame(columns=["match_id","date","minute","title","comment","out_player","in_player"])
        
        # extract events from feed container
        events = cont.select("div.mls-o-match-feed__container")

        for ev in events:
            minute = get_html_text(ev.select_one(".mls-o-match-feed__regular-time")) or \
                     get_html_text(ev.select_one(".mls-o-match-feed__minute"))
            title = get_html_text(ev.select_one(".mls-o-match-feed__title"))
            comment = get_html_text(ev.select_one(".mls-o-match-feed__comment"))
            out_player = get_html_text(ev.select_one(".mls-o-match-feed__sub-out .mls-o-match-feed__player"))
            in_player  = get_html_text(ev.select_one(".mls-o-match-feed__sub-in  .mls-o-match-feed__player"))

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
        
    # convert to DataFrame and return
    feed = pd.DataFrame(rows)
    if feed.empty:
        print(f"[WARN] Feed DataFrame is empty for match ID {match_id}")
        return pd.DataFrame()

    return feed


def parse_stat_table(table) -> list:
    if not table:
        return []

    ths = table.select("thead th.mls-o-table__header")
    ths = [th for th in ths if "stats-type" not in (th.get("class") or [])]

    headers = []
    for i, th in enumerate(ths):
        # try class name first, fall back to text
        classes = th.get("class", [])
        col_class = next((c for c in classes if c not in 
                         ("mls-o-table__header", "shadow", "flex")), None)
        if col_class:
            headers.append(col_class)
        else:
            h = th.get_text(" ", strip=True)
            headers.append(h if h else f"col_{i}")
    out = []
    for tr in table.select("tbody tr.mls-o-table__row"):
        tds = tr.select("td.mls-o-table__cell")
        cells = [td.get_text(" ", strip=True) for td in tds]

        # Make lengths match headers (we keep the LEFT side aligned)
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        elif len(cells) > len(headers):
            cells = cells[:len(headers)]

        out.append(dict(zip(headers, cells)))

    return out


# ─────────────────────────────────────────────
# Cleaners — TODO: fill in (except outfield + gk)
# ─────────────────────────────────────────────

def reframe_stats(df):
    match_id = df['match_id'].iloc[0]
    out = {}

    for _, row in df.iterrows():
        stat = row.get("stat")
        if pd.isna(stat):
            continue
        stat = str(stat).strip().lower()
        if stat in ("", "nan", "none") or "nan" in stat:
            continue

        out[f"{stat}_home"] = row.get("home_value")
        out[f"{stat}_away"] = row.get("away_value")

    wide = pd.DataFrame([out]) if out else pd.DataFrame([{}])

    wide.columns = [
        re.sub(r"\s+", "_",
        c.replace('%', 'pct').replace('-', '_')).lower()
        for c in wide.columns
    ]
    wide["match_id"] = match_id
    return wide



def clean_match_team(df):
    df = df.copy()
    bar_dict = {
        '0-5': 'bar_0',
        '6-10': 'bar_1',
        '11-15': 'bar_2',
        '16-20': 'bar_3',
        '21-25': 'bar_4',
        '26-30': 'bar_5',
        '31-35': 'bar_6',
        '36-40': 'bar_7',
        '41-45': 'bar_8',
        '46-50': 'bar_2_0',
        '51-55': 'bar_2_1',
        '56-60': 'bar_2_2',
        '61-65': 'bar_2_3',
        '66-70': 'bar_2_4',
        '71-75': 'bar_2_5',
        '76-80': 'bar_2_6',
        '81-85': 'bar_2_7',
        '86-90': 'bar_2_8',
    }

    bar_dict_switched = {v: k for k, v in bar_dict.items()}
    df = df.drop(columns=['home_advantage', 'away_advantage'])
    df["category"] = df["category"].fillna("").astype(str).str.strip()
    df["stat_name"] = df["stat_name"].fillna("").astype(str).str.strip()

    df["stat"] = (df["category"] + "_" + df["stat_name"]).str.strip("_")

    # remove junk stats
    df = df[df["stat"].ne("") & df["stat"].ne("nan") & ~df["stat"].str.contains(r"\bnan\b", na=False)]
    df['stat'] = df['category'].astype(str) + '_' + df['stat_name'].astype(str)
    df = df.drop(columns=['category', 'stat_name'])
    df['tip_id'] = df['tip_id'].astype(str).str.strip() 
    df['tip_id'] = df['tip_id'].replace(bar_dict_switched)
    mask = df["tip_id"].astype(str).str.match(r"^\d{1,2}-\d{1,2}$", na=False)
    h_pct = df.loc[mask, "home_possession"].astype(str).str.extract(r"(\d+(?:\.\d+)?)")[0].astype(float)
    a_pct = df.loc[mask, "away_possession"].astype(str).str.extract(r"(\d+(?:\.\d+)?)")[0].astype(float)
    df.loc[mask, "home_value"] = h_pct.values 
    df.loc[mask, "away_value"] = a_pct.values
    df.loc[mask, "stat"] = "possession_" + df.loc[mask, "tip_id"].str.replace("-", "_", regex=False)
    df = df.drop(columns=['tip_id', 'home_possession', 'away_possession'])
    df = df[['match_id', 'stat', 'home_value', 'away_value']]
    df = reframe_stats(df)
    df = df.drop(columns=['general_blocked_home', 'general_blocked_away',
                      'general_goals_home', 'general_goals_away',
                        'general_off_target_home', 'general_off_target_away',
                        'general_on_target_home', 'general_on_target_away'])
    
    df.rename(columns={'general_shots_on_target_home': 'general_shots_on_goal_home',
                       'general_shots_on_target_away': 'general_shots_on_goal_away'}, inplace=True)
    
    return df




def clean_match_feed(df):
    df = df.copy()
    
    df['title'] = df.apply(lambda x: 'Corner' if pd.notna(x['comment']) and 'corner' in x['comment'].lower() else x['title'], axis=1)

    df['title'] = df.apply(lambda x: 'Foul' if pd.notna(x['comment']) and 'foul' in x['comment'].lower() else x['title'], axis=1)

    df['title'] = df.apply(lambda x: 'Offside' if pd.notna(x['comment']) and 'offside' in x['comment'].lower() else x['title'], axis=1)   
    
    df = df[~df['comment'].str.contains('Lineups', na=False)]

    df = df[~df['title'].str.contains('KICK OFF|HALF TIME|FULL TIME|END OF SECOND HALF', na=False)]
    
    df = df[df['minute'].notna()]
    
    df = df.iloc[::-1].reset_index(drop=True)
    
    df['title'] = df['title'].fillna('Substitution')

    df['comment'] = df.apply(lambda x: f"Substitution: {x['out_player']} out, {x['in_player']} in" if x['title'] == 'Substitution' else x['comment'], axis=1)
    
    df['feed_id'] = df.groupby('match_id').cumcount() + 1
        
    df = df.sort_values(by=['date', 'match_id', 'feed_id'], ascending=[False, True, False]).reset_index(drop=False)
    
    df = df.drop(columns=['in_player', 'out_player'])

    df = df[['match_id', 'date', 'feed_id', 'minute', 'title', 'comment']]
    
    df.rename(columns={'minute' : 'event_minute', 'title': 'event_type', 'comment' : 'event_comment', 'feed_id' : 'event_id'}, inplace=True)
    
    df = df[['event_id','match_id', 'event_minute', 'event_type', 'event_comment']]
    
    return df



team_fullnames_to_short = {
    "Columbus": "CLB",
    "Orlando": "ORL",
    "Dallas": "DAL",
    "New York City": "NYC",
    "LAFC": "LAFC",
    "Portland": "POR",
    "New England": "NE",
    "Montréal": "MTL",
    "San Diego": "SD",
    "Nashville": "NSH",
    "Colorado": "COL",
    "Kansas City": "SKC",
    "Minnesota": "MIN",
    "LA Galaxy": "LA",
    "Vancouver": "VAN",
    "San Jose": "SJ",
    "Atlanta": "ATL",
    "D.C. United": "DC",
    "Cincinnati": "CIN",
    "Seattle": "SEA",
    "Houston": "HOU",
    "Charlotte": "CLT",
    "Salt Lake": "RSL",
    "Philadelphia": "PHI",
    "New York": "RBNY",        
    "Toronto": "TOR",
    "Austin": "ATX",
    "Miami": "MIA",
    "Chicago": "CHI",
    "St. Louis": "STL"
}



def clean_match_data(df):
    df = df.copy()
    
    df['home_team'] = df['home_team'].replace(team_fullnames_to_short)
    df['away_team'] = df['away_team'].replace(team_fullnames_to_short)
    
    df = df.rename(columns={'home_team_score': 'home_score', 'away_team_score': 'away_score'})    
    
    return df


# ─────────────────────────────────────────────
# Parsing
# ─────────────────────────────────────────────

def parse_player_stats_from_html(html: str, match_id: str = None):
    soup = BeautifulSoup(html, "lxml")

    section = soup.select_one("div.mls-c-stats.mls-c-stats--match-hub-player-stats")
    if not section:
        return pd.DataFrame(), pd.DataFrame()

    team_divs = section.select("div.mls-c-stats__club-abbreviation")
    if not team_divs:
        return pd.DataFrame(), pd.DataFrame()

    outfield_rows = []
    gk_rows = []

    for idx, team_div in enumerate(team_divs):
        team = team_div.get_text(strip=True)
        side = "home" if idx == 0 else "away"

        tables = []
        gk_tables = []
        for sib in team_div.next_siblings:
            if getattr(sib, "name", None) is None:
                continue
            if sib.name == "div" and "mls-c-stats__club-abbreviation" in (sib.get("class", []) or []):
                break
            # outfield table
            tables.extend(sib.select("table.mls-o-table.match-hub-player-stats"))
            # GK table — inside the mt-25 container
            gk_tables.extend(sib.select("div.mls-o-match-hub-container__mt-25 table"))

        main_table = tables[0] if tables else None
        gk_table   = gk_tables[0] if gk_tables else None

        for row in (parse_stat_table(main_table) or []):
            row.update({"club": team, "side": side})
            if match_id is not None:
                row["match_id"] = match_id
            outfield_rows.append(row)

        for row in (parse_stat_table(gk_table) or []):
            row.update({"club": team, "side": side})
            if match_id is not None:
                row["match_id"] = match_id
            gk_rows.append(row)

    return pd.DataFrame(outfield_rows), pd.DataFrame(gk_rows)


# ─────────────────────────────────────────────
# Player extraction
# ─────────────────────────────────────────────

def extract_players(driver, match_id: str, date):
    wait = WebDriverWait(driver, 10)

    js_scroll_into_view(driver, driver.find_element(
        By.CSS_SELECTOR, "section.mls-l-module--match-hub-header-container"
    ))

    try:
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'section[data-react="mls-match-hub-stats-toggle"]')
        ))

        player_btn = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR,
             'section[data-react="mls-match-hub-stats-toggle"] button.mls-o-buttons__segment[value="players"]')
        ))

        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", player_btn)

        player_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR,
             'section[data-react="mls-match-hub-stats-toggle"] button.mls-o-buttons__segment[value="players"]')
        ))

        try:
            player_btn.click()
        except (ElementClickInterceptedException, StaleElementReferenceException):
            player_btn = driver.find_element(
                By.CSS_SELECTOR,
                'section[data-react="mls-match-hub-stats-toggle"] button.mls-o-buttons__segment[value="players"]'
            )
            driver.execute_script("arguments[0].click();", player_btn)

    except TimeoutException:
        print("Players toggle not found/clickable.")
        return None, None

    driver.implicitly_wait(2)
    js_scroll_by(driver, 1500)

    html = driver.page_source
    return parse_player_stats_from_html(html, match_id)


# ─────────────────────────────────────────────
# Cleaning — outfield
# ─────────────────────────────────────────────

def clean_match_players(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "match_id" not in df.columns:
        raise ValueError(f"No match_id column. Columns: {df.columns.tolist()}")

    df = df[["match_id"] + [c for c in df.columns if c != "match_id"]]
    df = map_club(df)
    df = df.sort_values("match_id", ascending=False).reset_index(drop=True)
    df.columns = [col.split()[0] for col in df.columns]

    df = df.rename(columns={
        "player":                       "player_name",
        "mins_played":                  "minutes",
        "saves":                        "gk_goals_saved",
        "goals_conceded":               "gk_goals_against",
        "expected_goals_conceded":      "gk_expected_goals_against",
        "successful_passes":            "gk_pass_count",
        "accurate_pass_per_match_hub":  "gk_pass_perc",
        "keeper_throws":                "gk_throws",
        "total_long_balls":             "gk_long_balls",
        "total_launches":               "gk_launches",
        "goal_kicks":                   "gk_goal_kicks",
        "fouls":                        "fouls",
        "was_fouled":                   "fouls_against",
        "yellow_card":                  "yellow_card",
        "total_red_card":               "red_card",
        "lost_corners":                 "corners_conceded",
        "punches":                      "gk_punches",
    })

    gk_cols = [c for c in ["gk_goals_saved", "gk_goals_against", "gk_expected_goals_against",
                            "gk_pass", "gk_throws", "gk_long_balls", "gk_launches",
                            "GK", "corners_conceded"] if c in df.columns]
    df = df.drop(columns=gk_cols)

    return df


# ─────────────────────────────────────────────
# Cleaning — GK
# ─────────────────────────────────────────────

def clean_match_gk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df = df.loc[:, ~df.columns.duplicated(keep='first')]

    if "match_id" not in df.columns:
        raise ValueError(f"No match_id column. Columns: {df.columns.tolist()}")

    df = df[["match_id"] + [c for c in df.columns if c != "match_id"]]
    df = map_club(df)
    df = df.sort_values("match_id", ascending=False).reset_index(drop=True)
    
    print(f"  [DEBUG] gk columns before strip: {df.columns.tolist()}")
    df.columns = [col.split()[0] for col in df.columns]
    print(f"  [DEBUG] gk columns after strip: {df.columns.tolist()}")
    dupes = df.columns[df.columns.duplicated()].tolist()
    print(f"  [DEBUG] dupes after strip: {dupes}")

    df = df.loc[:, ~df.columns.duplicated(keep='first')]
    
    df = df.rename(columns={
        "player":                        "player_name",
        "mins_played":                   "minutes",
        "saves":                         "gk_goals_saved",
        "goals_conceded":                "gk_goals_against",
        "expected_goals_conceded":       "gk_expected_goals_against",
        "successful_passes":             "gk_pass_count",        # was "successful_pass"
        "accurate_pass_per_match_hub":   "gk_pass_perc",         # was truncated
        "keeper_throws":                 "gk_throws",
        "total_long_balls":              "gk_long_balls",
        "total_launches":                "gk_launches",
        "goal_kicks":                    "gk_goal_kicks",
        "fouls":                         "fouls",
        "was_fouled":                    "fouls_against",
        "yellow_card":                   "yellow_card",
        "total_red_card":                "red_card",             
        "lost_corners":                  "corners_conceded",     
        "punches":                       "gk_punches",           
    })

    print(f"  [DEBUG] gk columns after rename: {df.columns.tolist()}")
    dupes2 = df.columns[df.columns.duplicated()].tolist()
    print(f"  [DEBUG] dupes after rename: {dupes2}")

    df = df.loc[:, ~df.columns.duplicated(keep='first')]

    outfield_cols = [c for c in ["goals", "assists", "total_scoring_att",
                                "passes", "shot_conv_perc", "expected_goals", 
                                "on_target", "cross", "corner_kick", "key_pass", 
                                "aerial", "aerial_perc", "offside", "GK",
                                "P", "A", "G", "Conv%"]
                    if c in df.columns]
    df = df.drop(columns=outfield_cols)

    int_cols   = ["minutes", "gk_goals_saved", "gk_goals_against", "gk_throws",
                  "gk_long_balls", "gk_launches", "fouls", "fouls_against",
                  "yellow_card", "red_card", "corners_conceded"]
    float_cols = ["gk_expected_goals_against", "gk_pass_count", "gk_pass_perc"]

    for c in int_cols:
        if c in df.columns and isinstance(df[c], pd.Series):
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in float_cols:
        if c in df.columns and isinstance(df[c], pd.Series):
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
            
    print(f"  [DEBUG] gk final columns: {df.columns.tolist()}")

    return df


# ─────────────────────────────────────────────
# Clean all
# ─────────────────────────────────────────────

CLEANERS = {
    "outfield":   clean_match_players,
    "gk":         clean_match_gk,
    "team_stats": clean_match_team,
    "feed":       clean_match_feed,
    "match_data": clean_match_data,
}


def clean_all(raw: dict) -> dict:
    cleaned = {}
    for key, cleaner in CLEANERS.items():
        df = raw.get(key)
        if df is None or df.empty:
            print(f"  [SKIP] No data to clean for {key}")
            cleaned[key] = pd.DataFrame()
            continue
        try:
            cleaned[key] = cleaner(df)
            print(f"  [CLEANED] {key} → {len(cleaned[key])} rows")
        except Exception as e:
            print(f"  [CLEAN ERROR] {key} | {e}")
            traceback.print_exc()
            cleaned[key] = pd.DataFrame()
    return cleaned


# ─────────────────────────────────────────────
# Scraping
# ─────────────────────────────────────────────

def extract_match_links(driver) -> list:
    wait = WebDriverWait(driver, 10)
    driver.get("https://www.mlssoccer.com/schedule/scores#competition=MLS-COM-000001&club=all")
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "body")))
    time.sleep(2)

    all_links = set()
    try:
        previous_button = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[@aria-label='Previous results']")
        ))
        previous_button.click()
        time.sleep(3)

        matches_table = wait.until(EC.presence_of_element_located(
            (By.CLASS_NAME, 'mls-c-schedule__matches')
        ))
        hrefs = matches_table.find_elements(By.TAG_NAME, 'a')
        for href in hrefs:
            all_links.add(href.get_attribute('href'))

    except Exception as e:
        print(f"Error extracting match links: {e}")

    return list(all_links)


def scrape_missing_links(driver, stop_date="2026-02-21"):
    wait = WebDriverWait(driver, 10)
    driver.get("https://www.mlssoccer.com/schedule/scores#competition=MLS-COM-000001&club=all")
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(2)
    dismiss_cookies(driver)

    all_links = set()
    stop = pd.to_datetime(stop_date)
    week_num = 0

    while True:
        try:
            matches_table = wait.until(EC.presence_of_element_located(
                (By.CLASS_NAME, 'mls-c-schedule__matches')
            ))
            hrefs = matches_table.find_elements(By.TAG_NAME, 'a')
            new_links = [h.get_attribute('href') for h in hrefs]
            before = len(all_links)
            for link in new_links:
                all_links.add(link)
            added = len(all_links) - before
            
            
            date_els = driver.find_elements(By.CSS_SELECTOR, "h2.sc-hLBbgP")
            dates = []
            for el in date_els:
                text = el.text.strip()
                dt = pd.to_datetime(f"{text} 2026", format="%A %b %d %Y", errors="coerce")
                if pd.notna(dt):
                    dates.append(dt)

            week_num += 1
            if dates:
                print(f"  [WEEK {week_num}] {min(dates).date()} → {max(dates).date()} | +{added} links | total={len(all_links)}")
            else:
                print(f"  [WEEK {week_num}] no dates found | +{added} links | total={len(all_links)}")

            if dates and min(dates) <= stop:
                print(f"  [STOP] Reached stop date {stop_date}. Done.")
                break

            prev_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[@aria-label='Previous results']")
            ))
            prev_btn.click()
            time.sleep(3)

        except Exception as e:
            print(f"  [ERROR] Stopped early: {e}")
            break

    print(f"\n[DONE] {len(all_links)} total links collected.")
    
    return list(all_links)


def scrape_match_list(driver, links: list) -> dict:
    wait = WebDriverWait(driver, 10)
    combined = {k: pd.DataFrame() for k in PATHS.keys()}
    failed = []
    remaining_links = list(links)
    
    driver.get('https://www.mlssoccer.com/schedule/scores#competition=MLS-COM-000001&club=all')
    
    time.sleep(2)
    
    dismiss_cookies(driver)

    for round_num in range(1, MAX_ROUNDS + 1):
        print(f"\n=== Round {round_num}/{MAX_ROUNDS} | {len(remaining_links)} matches ===")
        next_remaining = []

        for link in remaining_links:
            match_id = make_match_id(link)

            try:
                driver.get(link)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                time.sleep(2)
                
                dismiss_cookies(driver)
                
                time.sleep(2)
                
                dismiss_cookies(driver)
                
                time.sleep(2)

                match_team_data, date, home_team, away_team, home_score, away_score = extract_team_stats(driver, match_id)
                combined["team_stats"] = pd.concat([combined["team_stats"], match_team_data], ignore_index=True)

                match_row = pd.DataFrame([{
                    "match_id":        match_id,
                    "date":            date,
                    "home_team":       home_team,
                    "away_team":       away_team,
                    "home_team_score": home_score,
                    "away_team_score": away_score,
                }])
                combined["match_data"] = pd.concat([combined["match_data"], match_row], ignore_index=True)

                df_outfield, df_gk = extract_players(driver, match_id, date)
                if df_outfield is not None and not df_outfield.empty:
                    combined["outfield"] = pd.concat([combined["outfield"], df_outfield], ignore_index=True)
                if df_gk is not None and not df_gk.empty:
                    combined["gk"] = pd.concat([combined["gk"], df_gk], ignore_index=True)

                feed_data = extract_feed(driver, match_id, date)
                if feed_data is not None and not feed_data.empty:
                    feed_data["home_team"] = home_team
                    feed_data["away_team"] = away_team
                    combined["feed"] = pd.concat([combined["feed"], feed_data], ignore_index=True)

                print(f"  [OK] {match_id}")

            except Exception as e:
                print(f"  [FAILED] {match_id} | {link} | {e}")
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
            print(f"Cooling down {sleep_time:.1f}s before retry...")
            time.sleep(sleep_time)

    if failed:
        from helpers import RAW_BASE
        failures_path = RAW_BASE / f"{datetime.today().strftime('%Y-%m-%d')}_failures.csv"
        pd.DataFrame(failed).to_csv(failures_path, index=False)
        print(f"\n[FAILURES] {len(failed)} failed matches → {failures_path}")

    return combined


# ─────────────────────────────────────────────
# Save + upload
# ─────────────────────────────────────────────

def save_raw_backups(raw: dict):
    print("\n--- Saving raw backups ---")
    for key, df in raw.items():
        write_csv(df, dated_filename(key))


def save_masters(cleaned: dict):
    print("\n--- Updating master CSVs ---")
    for key, df in cleaned.items():
        append_to_master(df, key)



# ─────────────────────────────────────────────
# Main run
# ─────────────────────────────────────────────

def run(mode: str, url_list: str = None, batch_size: int = 50, stop_date: str = "2026-02-21"):
    ensure_dirs()
    driver = set_up_driver()
    dismiss_cookies(driver)

    try:
        if mode == "backfill":
            print(f"[BACKFILL] Discovering match URLs since {stop_date}...")
            new_links = scrape_missing_links(driver, stop_date=stop_date)
            add_links_to_master(new_links)
            print("[BACKFILL] Done. Run --historical to scrape them.")
            return

        if mode == "historical":
            path = url_list or str(URL_LIST_PATH)
            df_urls = pd.read_csv(path)
            if "url" not in df_urls.columns:
                raise ValueError(f"URL list must have a 'url' column. Found: {df_urls.columns.tolist()}")
            all_links = df_urls["url"].dropna().unique().tolist()
            print(f"[HISTORICAL] {len(all_links)} match URLs loaded from {path}")
        else:
            all_links = extract_match_links(driver)
            print(f"[WEEKLY] {len(all_links)} match URLs found")

        if not all_links:
            print("No match links found. Exiting.")
            return

        batches = [all_links[i:i+batch_size] for i in range(0, len(all_links), batch_size)] \
                  if mode == "historical" else [all_links]

        for batch_num, batch in enumerate(batches, 1):
            if len(batches) > 1:
                print(f"\n{'='*50}")
                print(f"BATCH {batch_num}/{len(batches)} | {len(batch)} matches")
                print(f"{'='*50}")

            print("\n--- Scraping ---")
            raw = scrape_match_list(driver, batch)

            save_raw_backups(raw)

            print("\n--- Cleaning ---")
            cleaned = clean_all(raw)

            save_masters(cleaned)

            if len(batches) > 1 and batch_num < len(batches):
                sleep_time = 60 + random.random() * 30
                print(f"\nBatch cooldown {sleep_time:.1f}s...")
                time.sleep(sleep_time)

    finally:
        driver.quit()
        print("\n[DONE] Pipeline complete.")


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MLS Data Pipeline")
    parser.add_argument("--historical",  action="store_true", help="Scrape all matches from URL list")
    parser.add_argument("--weekly",      action="store_true", help="Scrape last week's matches")
    parser.add_argument("--backfill",    action="store_true", help="Discover missing URLs since stop-date")
    parser.add_argument("--url-list",    type=str, default=None,          help="Path to URL list CSV (historical mode)")
    parser.add_argument("--batch-size",  type=int, default=50,            help="Matches per batch (historical mode)")
    parser.add_argument("--stop-date",   type=str, default="2026-02-21",  help="Stop date for backfill (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.backfill:
        run(mode="backfill", stop_date=args.stop_date)
    elif args.historical:
        run(mode="historical", url_list=args.url_list, batch_size=args.batch_size)
    else:
        run(mode="weekly")