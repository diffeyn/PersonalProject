import mls.scraping.bs4.bs_scraper as bscraper



def scrape_sofifa():
    ## base sofifa url for teams with query parameters to specify which columns we want to scrape for team stats, this allows us to get more detailed stats without having to scrape the entire page and then filter, which can be more efficient and reduce the amount of data we need to process while still getting all the relevant stats for our analysis.
    sofifa_url = 'https://sofifa.com/teams?type=all&lg%5B0%5D=39&showCol%5B%5D=ti&showCol%5B%5D=fm&showCol%5B%5D=oa&showCol%5B%5D=at&showCol%5B%5D=md&showCol%5B%5D=df&showCol%5B%5D=dm&showCol%5B%5D=ps&showCol%5B%5D=cw'
    
    ## get soup for the sofifa teams page and extract team stats and links to team pages for further scraping of player data
    soup = bscraper.get_soup(sofifa_url)
    
    ### extract team stats and links to team pages for further scraping of player data
    sofifa_teams_df, team_links = bscraper.scrape_team_table(soup)
    
    ### --- TEMPORARY --- ###
    # keep first 2 teams for testing
    team_links = team_links[:2]
    ### --- TEMPORARY --- ###
    
    ### extract player stats from team pages using the links extracted from the teams page, this will give us a dataframe with player stats for all players in the teams we scraped, and we can link this with the team stats using the team name and date for context in our dataset.
    sofifa_players_df = bscraper.extract_players(team_links)
    
    return sofifa_teams_df, sofifa_players_df 