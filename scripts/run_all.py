from mls.cleaning.clean import clean_data
from mls.scraping.scrape_all import scrape_all
from mls.database.upload import upload_to_sql

def main():
    ## scrape matches and most recent player/team data from SoFIFA and save to interim data folder
    scrape_all()
    
    clean_data()
    
    upload_to_sql()
    
if __name__ == "__main__":
    main()