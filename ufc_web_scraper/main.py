import logging
import time
from scraper import get_urls, events, fights, fightstats, fighters, normalise_tables

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ufc_scraper.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    logger = setup_logging()
    start_time = time.time()
    
    logger.info("=== UFC Web Scraper Started ===")
    
    try:
        # Scrapes all urls from ufcstats.com
        logger.info("Phase 1: Getting URLs from ufcstats.com")
        
        logger.info("Getting event URLs...")
        get_urls.get_event_urls()
        logger.info("Event URLs completed")
        
        logger.info("Getting fight URLs...")
        get_urls.get_fight_urls()
        logger.info("Fight URLs completed")
        
        logger.info("Getting fighter URLs...")
        get_urls.get_fighter_urls()
        logger.info("Fighter URLs completed")
        
        # Iterates through urls and scrapes key data into csv files
        logger.info("Phase 2: Scraping data from URLs")
        
        logger.info("Scraping events data...")
        events.scrape_events()
        logger.info("Events scraping completed")
        
        logger.info("Scraping fights data...")
        fights.scrape_fights()
        logger.info("Fights scraping completed")
        
        logger.info("Scraping fight stats data...")
        fightstats.scrape_fightstats()
        logger.info("Fight stats scraping completed")
        
        logger.info("Scraping fighters data...")
        fighters.scrape_fighters()
        logger.info("Fighters scraping completed")
        
        # Normalises tables for clean final output
        logger.info("Phase 3: Normalising tables")
        normalise_tables.normalise_tables()
        logger.info("Table normalisation completed")
        
        elapsed_time = time.time() - start_time
        logger.info(f"=== UFC Web Scraper Completed Successfully in {elapsed_time:.2f} seconds ===")
        
    except Exception as e:
        logger.error(f"Error during scraping process: {e}")
        raise

if __name__ == '__main__':
    main()
