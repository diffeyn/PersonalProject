import os
import pandas as pd
import requests
from bs4 import BeautifulSoup 
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode



api_key = os.getenv("SECRET_API_KEY")

def get_soup(url):
    payload = {
        "api_key": api_key,
        "url": url,
    }

    response = requests.get("https://scraping.narf.ai/api/v1/", params=payload)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


def scrape_team_table(soup):
    """
    Scrape team information from an HTML table using BeautifulSoup.
    This function extracts team data from the first table found in the provided
    BeautifulSoup object, including team details and associated links. It also
    attempts to extract and format a date from a roster dropdown selector.
    ! Args:
        soup (BeautifulSoup): A BeautifulSoup object containing the parsed HTML
            with a teams table.
   !  Returns:
        tuple: A tuple containing:
            - teams_df (pd.DataFrame): A DataFrame with team information where each
              row represents a team and columns correspond to table headers. Includes
              a 'date' column with the roster date or current date.
            - team_links (list): A list of href URLs extracted from anchor tags within
              table cells with class 's20'.
    ! Raises:
        ValueError: If no table is found in the HTML.
        ValueError: If no header row is found in the table.
    ! Notes:
        - The function looks for a 'select' element with name='roster' to extract
          the date. If not found, it uses the current date.
        - Date strings are sanitized by replacing '/' and ':' with '-'.
        - Only the first table in the soup object is processed.
    """
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

    return teams_df, team_links
    

COLS = [
    "pi","ae","hi","wi","pf","oa","bo","bp","vl","wg","ta","cr","fi","he","sh","vo","ts",
    "dr","cu","fr","lo","bl","to","ac","sp","ag","re","ba","tp","so","ju","st","ln","te",
    "ar","in","po","vi","pe","cm","td","ma","sa","sl","tg","gd","gh","gc","gp","gr"
]

def add_columns_to_url(u: str, cols) -> str:
    pu = urlparse(u)
    pairs = parse_qsl(pu.query, keep_blank_values=True)
    pairs += [("showCol[]", c) for c in cols]
    return urlunparse(pu._replace(query=urlencode(pairs, doseq=True)))

def extract_players(team_links):
    """
    ! Extract player information from a list of team URLs on SoFiFA.
    ! Args:
        team_links (list): A list of relative URLs (paths) to team pages on SoFiFA.
                          Each link should be in the format "/team/XXXXX/team-name".
    ! Returns:
        pd.DataFrame: A DataFrame containing player information with the following structure:
                     - Columns correspond to the headers found in the players table on each team page
                     - Additional 'date' column: The roster date from the page, or current date if not found
                     - Additional 'team' column: The team name extracted from the last processed team page
                       Note: The 'team' column will only reflect the last team processed, which may
                       cause data inconsistency if multiple teams are scraped.
    ! Raises:
        None: Errors are printed to console but do not stop execution.
              - Missing tables or headers are logged with print statements
              - Teams without valid data are skipped
    ! Side Effects:
        - Makes HTTP requests to SoFiFA website for each team link
        - Prints warning messages when tables or headers are not found
    ! Notes:
        - Depends on helper functions: add_columns_to_url(), get_soup()
        - Expects global constant: COLS
    """
    all_players = []

    for link in team_links:
        team_url = f"https://sofifa.com{link}"
        
        team_url = add_columns_to_url(team_url, COLS)
        
        soup = get_soup(team_url)
        
        team = soup.find('h1').get_text()
        
        date_elem = soup.find('select', {'name': 'roster'})
        if date_elem:
            date = date_elem.find('option', selected=True).text.strip()
            safe_date = date.replace("/", "-").replace(":", "-").strip()
        else:
            safe_date = datetime.now().strftime("%Y-%m-%d")
        
        players_table = soup.find('table')
        if players_table is None:
            print(f"No table found for team URL: {team_url}")
            continue
        
        rows = players_table.find_all('tr')
        if not rows or not rows[0].find_all('th'):
            print(f"No header row found in the table for team URL: {team_url}")
            continue

        headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
        for row in rows[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all('td')]
            if cols:
                player_data = dict(zip(headers, cols))
                player_data['date'] = safe_date
                player_data['team'] = team
                all_players.append(player_data)

    players_df = pd.DataFrame(all_players)

    return players_df