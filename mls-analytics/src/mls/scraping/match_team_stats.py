from asyncio import wait
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from mls.utils import utils


def extract_team_stats(driver, link, match_id):   
    
    wait = WebDriverWait(driver, 10)
      
    driver.get(link)
    
    print("[OK] loaded:", driver.title)
    
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    
    utils.dismiss_cookies(driver)
        
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
        home_team = teams.find_element(
            By.CSS_SELECTOR,
            "div.mls-c-club.--home span.mls-c-club__shortname"
            ).text.strip()

        away_team = teams.find_element(
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

            utils.js_scroll_into_view(driver, shooting_cont)

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

            utils.js_scroll_into_view(driver, bar_cont)


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