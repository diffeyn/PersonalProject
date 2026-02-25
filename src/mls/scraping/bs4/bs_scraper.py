import os
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

api_key = os.getenv("SECRET_API_KEY")

# This file contains functions to scrape data from the web using BeautifulSoup and the scraping.narf.ai API. It includes functions to get the HTML content of a page, parse tables, and extract player stats from match pages.

# Function to get the HTML content of a page using the scraping.narf.ai API

def get_soup(url, tries=3, timeout=30):
    payload = {
        "api_key": api_key,
        "url": url,
    }

    last_err = None

    for attempt in range(1, tries + 1):
        try:
            response = requests.get(
                "https://scraping.narf.ai/api/v1/",
                params=payload,
                timeout=timeout
            )

            response.raise_for_status()

            if not response.text or len(response.text) < 500:
                raise RuntimeError("Empty or suspiciously short HTML")

            soup = BeautifulSoup(response.text, "html.parser")

            if soup.find("table") is None:
                raise RuntimeError("Expected table not found (partial or blocked page)")

            return soup

        except Exception as e:
            last_err = e

            if attempt == tries:
                break

            sleep_s = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            print(f"[retry {attempt}/{tries}] Failed: {url} | {e} | sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed after {tries} tries: {url}") from last_err

#----MLS-SPECIFIC SCRAPING FUNCTIONS----#

### Function to parse a stats table from the HTML content of a match page, extracting relevant information such as player names, teams, stats, and linking with match_id and date for context in the dataset. It handles both main stats and goalkeeper stats tables if present.
def parse_stat_table(table):
    if not table:
        return []

    # --- HEADER: skip ONLY the fake stats-type TH ---
    ths = table.select("thead th.mls-o-table__header")

    # drop header ths with class stats-type (usually the first one)
    ths = [th for th in ths if "stats-type" not in (th.get("class") or [])]

    headers = []
    for i, th in enumerate(ths):
        h = th.get_text(" ", strip=True)
        if not h:
            h = f"col_{i}"   # keep the column, don’t drop it
        headers.append(h)

    # --- BODY: keep ALL real tds as-is ---
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


# function to parse player stats from the HTML content of a match page, extracting relevant information such as player names, teams, stats, and linking with match_id and date for context in the dataset. It handles both main stats and goalkeeper stats tables if present.
def parse_player_stats_from_html(html, match_id=None):
    soup = BeautifulSoup(html, "lxml")

    section = soup.select_one("div.mls-c-stats.mls-c-stats--match-hub-player-stats")
    if not section:
        return pd.DataFrame()

    team_divs = section.select("div.mls-c-stats__club-abbreviation")
    if not team_divs:
        return pd.DataFrame()

    all_rows = []

    for idx, team_div in enumerate(team_divs):
        team = team_div.get_text(strip=True)
        side = "home" if idx == 0 else "away"  # keep your assumption for now

        # collect everything until next team marker
        tables = []
        for sib in team_div.next_siblings:
            if getattr(sib, "name", None) is None:
                continue
            if sib.name == "div" and "mls-c-stats__club-abbreviation" in (sib.get("class", []) or []):
                break  # next team starts
            tables.extend(sib.select("table.mls-o-table.match-hub-player-stats"))

        main_table = tables[0] if len(tables) > 0 else None
        gk_table   = tables[1] if len(tables) > 1 else None

        for row in parse_stat_table(main_table):
            row.update({"club": team, "side": side})
            if match_id is not None:
                row["match_id"] = match_id
            all_rows.append(row)

        for row in parse_stat_table(gk_table):
            row.update({"club": team, "side": side})
            if match_id is not None:
                row["match_id"] = match_id
            all_rows.append(row)

    return pd.DataFrame(all_rows)



# function to parse team stats from the HTML content of team pages, extracting relevant information such as team names, stats, and linking with date for context in the dataset. It handles the main team stats table and extracts links to team pages for further scraping of player data.
def scrape_team_table(soup):
    teams_table = soup.find('table')
    if teams_table is None:
        raise ValueError("No table found in the scraped HTML.")

    rows = teams_table.find_all('tr')
    if not rows or not rows[0].find_all('th'):
        raise ValueError("No header row found in the table.")

    teams_data = []
    headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]

    for row in rows[1:]:
        cols = [td.get_text(strip=True) for td in row.find_all('td')]
        if cols:
            teams_data.append(cols)

    teams_df = pd.DataFrame(teams_data, columns=headers)
    
    teams_df['date'] = None  # initialize date column
    
    
    date = soup.find('select', {'name': 'roster'})
    if date:
        date = date.find('option', selected=True).text.strip()
        ### find # in date and extract first part before it 
        date = date.split("#")[0].strip()
        safe_date = date.replace("/", "-").replace(":", "-").strip()
        teams_df['date'] = safe_date
    else:
        date = datetime.now().strftime("%Y-%m-%d")
        teams_df['date'] = date
        
    teams_df['date'] = date
        
    team_links = []
    
    for td in teams_table.find_all('td', class_='s20'):
        a = td.find('a', href=True)
        if a:
            team_links.append(a['href'])
            
    return teams_df, team_links
    
    
#----SOFIFA SCRAPING FUNCTIONS----#

    
## column names to filter results on SOFIFA.com to get more detailed player stats, these are the same columns that are available on the website when you click "show more" on the player stats table, we can add or remove columns from this list as needed to get the desired level of detail in our dataset without overwhelming it with too many columns that may not be relevant for analysis. The add_columns_to_url function will append these columns as query parameters to the team URLs before scraping to ensure we get all the detailed stats for each player.


COLS = [
    "pi","ae","hi","wi","pf","oa","bo","bp","vl","wg","ta","cr","fi","he","sh","vo","ts",
    "dr","cu","fr","lo","bl","to","ac","sp","ag","re","ba","tp","so","ju","st","ln","te",
    "ar","in","po","vi","pe","cm","td","ma","sa","sl","tg","gd","gh","gc","gp","gr"
]

### function to add query parameters to the team URLs to specify which columns we want to scrape from the team pages on SOFIFA.com, this allows us to get more detailed player stats without having to scrape the entire
def add_columns_to_url(u: str, cols) -> str:
    pu = urlparse(u)
    pairs = parse_qsl(pu.query, keep_blank_values=True)
    pairs += [("showCol[]", c) for c in cols]
    result = urlunparse(pu._replace(query=urlencode(pairs, doseq=True)))
    return result


### function to scrape team stats from the HTML content of a match page, extracting relevant information such as team names, stats, and linking with match_id and date for context in the dataset. It handles the main team stats table and extracts links to team pages for further scraping of player data.
def extract_players(team_links):
    all_players = []
    failed_links = []

    def get_soup_with_retry(url, tries=8, base_sleep=2, max_sleep=60):
        last_err = None
        for attempt in range(1, tries + 1):
            try:
                return get_soup(url)
            except Exception as e:
                last_err = e
                sleep = min(max_sleep, base_sleep * (2 ** (attempt - 1))) + random.random()
                print(f"[retry {attempt}/{tries}] FAILED {url} | sleeping {sleep:.2f}s")
                time.sleep(sleep)
        raise RuntimeError(f"Failed after {tries} tries") from last_err

    for link in team_links:

        team_url = f"https://sofifa.com{link}"
        team_url = add_columns_to_url(team_url, COLS)

        try:
            soup = get_soup_with_retry(team_url)
        except Exception as e:
            print("FINAL FAIL:", team_url, e)
            failed_links.append(link)
            continue

        # --- team name ---
        h1 = soup.select_one("header h1")
        team = h1.get_text(strip=True) if h1 else None

        # --- roster date ---
        date_elem = soup.find('select', {'name': 'roster'})
        selected_option = None

        if date_elem:
            selected_option = (
                date_elem.find('option', selected=True)
                or date_elem.find('option')
            )

        if selected_option:
            date = selected_option.text.strip()
            safe_date = date.replace("/", "-").replace(":", "-").strip()
        else:
            safe_date = datetime.now().strftime("%Y-%m-%d")

        # --- player table ---
        players_table = soup.find("table")
        if players_table is None:
            print(f"No table found for team URL: {team_url}")
            continue

        rows = players_table.find_all("tr")
        if not rows or not rows[0].find_all("th"):
            print(f"No header row found for team URL: {team_url}")
            continue

        headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

        for row in rows[1:]:
            tds = row.find_all("td")
            if not tds:
                continue

            cols = []
            extracted_position = None

            for i, td in enumerate(tds):
                header = headers[i] if i < len(headers) else f"col_{i}"

                if header.lower() == "name":
                    name_a = td.select_one('a[href^="/player/"]')
                    pos_span = td.select_one("span.pos")

                    name = name_a.get_text(strip=True) if name_a else td.get_text(" ", strip=True)
                    pos = pos_span.get_text(strip=True) if pos_span else None

                    cols.append(name)
                    extracted_position = pos
                else:
                    cols.append(td.get_text(strip=True))

            player_data = dict(zip(headers, cols))
            player_data["position"] = extracted_position
            player_data["date"] = safe_date
            player_data["team"] = team

            all_players.append(player_data)

    # --- second pass retry after cooldown ---
    if failed_links:
        print(f"\nCooling down before second pass ({len(failed_links)} failed)...")
        time.sleep(90)

        for link in failed_links:
            team_url = f"https://sofifa.com{link}"
            team_url = add_columns_to_url(team_url, COLS)

            try:
                soup = get_soup_with_retry(team_url, tries=6)
            except Exception as e:
                print("FAILED AGAIN:", team_url, e)
                continue

            h1 = soup.select_one("header h1")
            team = h1.get_text(strip=True) if h1 else None

            date_elem = soup.find('select', {'name': 'roster'})
            selected_option = None

            if date_elem:
                selected_option = (
                    date_elem.find('option', selected=True)
                    or date_elem.find('option')
                )

            if selected_option:
                date = selected_option.text.strip()
                safe_date = date.replace("/", "-").replace(":", "-").strip()
            else:
                safe_date = datetime.now().strftime("%Y-%m-%d")

            players_table = soup.find("table")
            if players_table is None:
                continue

            rows = players_table.find_all("tr")
            if not rows or not rows[0].find_all("th"):
                continue

            headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

            for row in rows[1:]:
                tds = row.find_all("td")
                if not tds:
                    continue

                cols = []
                extracted_position = None

                for i, td in enumerate(tds):
                    header = headers[i] if i < len(headers) else f"col_{i}"

                    if header.lower() == "name":
                        name_a = td.select_one('a[href^="/player/"]')
                        pos_span = td.select_one("span.pos")

                        name = name_a.get_text(strip=True) if name_a else td.get_text(" ", strip=True)
                        pos = pos_span.get_text(strip=True) if pos_span else None

                        cols.append(name)
                        extracted_position = pos
                    else:
                        cols.append(td.get_text(strip=True))

                player_data = dict(zip(headers, cols))
                player_data["position"] = extracted_position
                player_data["date"] = safe_date
                player_data["team"] = team

                all_players.append(player_data)

    return pd.DataFrame(all_players)