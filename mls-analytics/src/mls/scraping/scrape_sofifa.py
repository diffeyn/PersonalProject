import mls.scraping.bs4.bs_scraper as bscraper



def scrape_sofifa():
    sofifa_url = 'https://sofifa.com/teams?type=all&lg%5B0%5D=39&showCol%5B%5D=ti&showCol%5B%5D=fm&showCol%5B%5D=oa&showCol%5B%5D=at&showCol%5B%5D=md&showCol%5B%5D=df&showCol%5B%5D=ps&showCol%5B%5D=dm'
    soup = bscraper.get_soup(sofifa_url)
    sofifa_teams_df, team_links = bscraper.scrape_team_table(soup)
    sofifa_players_df = bscraper.extract_players(team_links)
    return sofifa_teams_df, sofifa_players_df 