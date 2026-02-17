import os
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

api_key = os.getenv("SECRET_API_KEY")

# This file contains functions to scrape data from the web using BeautifulSoup and the scraping.narf.ai API. It includes functions to get the HTML content of a page, parse tables, and extract player stats from match pages.

# Function to get the HTML content of a page using the scraping.narf.ai API
def get_soup(url):
    # Use the API to fetch the page content
    payload = {
        "api_key": api_key,
        "url": url,
    }

    response = requests.get("https://scraping.narf.ai/api/v1/", params=payload)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

#----MLS-SPECIFIC SCRAPING FUNCTIONS----#

### Function to parse a stats table from the HTML content of a match page, extracting relevant information such as player names, teams, stats, and linking with match_id and date for context in the dataset. It handles both main stats and goalkeeper stats tables if present.
def parse_stat_table(table):
    if not table:
        return []

    ths = table.select("thead th.mls-o-table__header")

    # Identify which header indices to KEEP
    keep_idx = []
    headers = []

    for i, th in enumerate(ths):
        classes = th.get("class", []) or []

        # Skip the fake stats-type column
        if "stats-type" in classes:
            continue

        header_text = th.get_text(strip=True)

        # Skip blank headers
        if not header_text:
            continue

        keep_idx.append(i)
        headers.append(header_text)

    out = []

    for tr in table.select("tbody tr.mls-o-table__row"):
        tds = tr.select("td.mls-o-table__cell")

        # Pad or trim to match header count
        if len(tds) < len(ths):
            tds += [None] * (len(ths) - len(tds))
        elif len(tds) > len(ths):
            tds = tds[:len(ths)]

        # Now extract ONLY the kept indices
        cells = []
        for i in keep_idx:
            td = tds[i]
            cells.append(td.get_text(strip=True) if td else "")

        out.append(dict(zip(headers, cells)))

    return out


# function to parse player stats from the HTML content of a match page, extracting relevant information such as player names, teams, stats, and linking with match_id and date for context in the dataset. It handles both main stats and goalkeeper stats tables if present.
def parse_player_stats_from_html(html, date, match_id=None):
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
            row.update({"club": team, "side": side, "date": date})
            if match_id is not None:
                row["match_id"] = match_id
            all_rows.append(row)

        for row in parse_stat_table(gk_table):
            row.update({"club": team, "side": side, "date": date})
            if match_id is not None:
                row["match_id"] = match_id
            all_rows.append(row)

    return pd.DataFrame(all_rows)


# function to parse team stats from the HTML content of a match page, extracting relevant information such as team names, stats, and linking with match_id and date for context in the dataset. It handles the main team stats table and extracts links to team pages for further scraping of player data.
def scrape_team_table(soup):
    print("Scraping team stats table...")
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
    
    date = soup.find('select', {'name': 'roster'})
    if date:
        date = date.find('option', selected=True).text.strip()
        safe_date = date.replace("/", "-").replace(":", "-").strip()
        teams_df['date'] = safe_date
    else:
        date = datetime.now().strftime("%Y-%m-%d")
        teams_df['date'] = date
        
    team_links = []
    
    for td in teams_table.find_all('td', class_='s20'):
        a = td.find('a', href=True)
        if a:
            team_links.append(a['href'])
            
    print(f"Found {len(team_links)} team links for player extraction.")
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
    
    count = 1

    for link in team_links:
        print(f"Processing team {count}/{len(team_links)}: {link}")
        
        count += 1
        
        ### construct team URL and get soup for the team page
        team_url = f"https://sofifa.com{link}"
        
        ### add query parameters to team URL to specify which columns we want to scrape for player stats, this allows us to get more detailed stats without having to scrape the entire page and then filter, which can be more efficient and reduce the amount of data we need to process while still getting all the relevant stats for our analysis.
        team_url = add_columns_to_url(team_url, COLS)
        
        ## get soup for team page and extract player stats from the page
        soup = get_soup(team_url)
        
        ## extract team name
        h1 = soup.select_one("header h1")
        team = h1.get_text(strip=True) if h1 else None
        
        ### extract date from the team page using roster date
        date_elem = soup.find('select', {'name': 'roster'})
        if date_elem:
            date = date_elem.find('option', selected=True).text.strip()
            safe_date = date.replace("/", "-").replace(":", "-").strip()
        else:
            safe_date = datetime.now().strftime("%Y-%m-%d")
        
        ## find table within html
        players_table = soup.find("table")
        if players_table is None:
            print(f"No table found for team URL: {team_url}")
            continue

        rows = players_table.find_all("tr")
        if not rows or not rows[0].find_all("th"):
            print(f"No header row found in the table for team URL: {team_url}")
            continue

        headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

        for row in rows[1:]:
            tds = row.find_all("td")
            if not tds:
                continue

            cols = []
            for i, td in enumerate(tds):
                header = headers[i] if i < len(headers) else f"col_{i}"

                # The "Name" cell contains both name + position in nested tags
                if header.lower() == "name":
                    name_a = td.select_one('a[href^="/player/"]')
                    pos_span = td.select_one("span.pos")

                    name = name_a.get_text(strip=True) if name_a else td.get_text(" ", strip=True)
                    pos = pos_span.get_text(strip=True) if pos_span else None

                    cols.append(name)

                    # also stash position as its own field (not part of cols)
                    extracted_position = pos
                else:
                    cols.append(td.get_text(strip=True))

            player_data = dict(zip(headers, cols))

            # add extracted position as a new column
            
            player_data["position"] = extracted_position
            player_data["date"] = safe_date
            player_data["team"] = team
            all_players.append(player_data)


    return pd.DataFrame(all_players)