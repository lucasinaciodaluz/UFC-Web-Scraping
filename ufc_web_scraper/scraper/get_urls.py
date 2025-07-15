#Import libraries for web-scraping and saving to CSV file
import requests
import bs4
import csv
import os
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Configure session with retry strategy
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Helper function to write URLs to a CSV file
def write_urls_to_csv(file_name, urls):
    
    #Create a new directory for urls
    os.makedirs('urls',exist_ok=True)
    path = os.getcwd() + '/urls'
    
    #Save file to new directory and add urls to file
    with open(path + '/' + file_name, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        for url in urls:
            writer.writerow([url])
    
    logger.info(f'Saved {len(urls)} URLs to {file_name}')

# Helper function to save progress checkpoint
def save_checkpoint(file_name, urls, current_index):
    #Create a new directory for checkpoints
    os.makedirs('checkpoints',exist_ok=True)
    checkpoint_path = os.getcwd() + '/checkpoints'
    
    # Save current progress
    with open(checkpoint_path + '/' + file_name, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['current_index', 'total_urls_collected'])
        writer.writerow([current_index, len(urls)])
        
    # Save collected URLs so far
    urls_file = file_name.replace('_checkpoint.csv', '_partial.csv')
    with open(checkpoint_path + '/' + urls_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        for url in urls:
            writer.writerow([url])
    
    logger.info(f'Checkpoint saved: {current_index} processed, {len(urls)} URLs collected')

# Helper function to load progress checkpoint
def load_checkpoint(file_name):
    checkpoint_path = os.getcwd() + '/checkpoints'
    checkpoint_file = checkpoint_path + '/' + file_name
    
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)  # Skip header
            row = next(reader)
            return int(row[0]), int(row[1])
    return 0, 0

# Helper function to load partial URLs
def load_partial_urls(file_name):
    checkpoint_path = os.getcwd() + '/checkpoints'
    urls_file = file_name.replace('_checkpoint.csv', '_partial.csv')
    partial_file = checkpoint_path + '/' + urls_file
    
    if os.path.exists(partial_file):
        with open(partial_file, 'r') as csv_file:
            reader = csv.reader(csv_file)
            return [row[0] for row in reader]
    return []

#Scrapes url of each UFC event from ufcstats.com
def get_event_urls():
    
    logger.info('Starting to scrape event links from ufcstats.com')
    try:
        session = create_session()
        main_url = session.get('http://ufcstats.com/statistics/events/completed?page=all', timeout=30)
        logger.info(f'Got response with status code: {main_url.status_code}')
        main_event_soup = bs4.BeautifulSoup(main_url.text, 'lxml')
        
        #Adds href to list if href contains a link with keyword 'event-details'
        all_event_urls = [item.get('href') for item in  main_event_soup.find_all('a') 
                          if type(item.get('href')) == str 
                          and 'event-details' in item.get('href')]
        
        logger.info(f'Found {len(all_event_urls)} event URLs')
        
        #Creates csv file and adds each event url to file as a new row
        write_urls_to_csv('event_urls.csv', all_event_urls)
        
        logger.info(f'{len(all_event_urls)} event links successfully scraped')
        
    except requests.RequestException as e:
        logger.error(f'Error making request to ufcstats.com: {e}')
        raise
    except Exception as e:
        logger.error(f'Error scraping event URLs: {e}')
        raise
    

#Scrapes url of each UFC fight from ufcstats.com 
def get_fight_urls():

    #Get event URLs from file
    path = os.getcwd() + '/urls'
    
    if 'event_urls.csv' in os.listdir(path):
        with open(path + '/' + 'event_urls.csv','r') as events_csv:
            reader = csv.reader(events_csv)
            event_urls = [row[0] for row in reader]
        logger.info(f'Loaded {len(event_urls)} event URLs from file')
    else:
        logger.error("Unable to scrape ufc fights due to missing file 'event_urls.csv'. Try running 'get_event_urls.py'")
        return

    logger.info('Starting to scrape fight links from ufcstats.com')
    session = create_session()

    # Check for existing checkpoint
    start_index, existing_urls_count = load_checkpoint('fight_urls_checkpoint.csv')
    all_fight_urls = load_partial_urls('fight_urls_checkpoint.csv')
    
    if start_index > 0:
        logger.info(f'Resuming from checkpoint: {start_index}/{len(event_urls)} events processed, {existing_urls_count} URLs already collected')
    
    #Iterates through each event URL
    for i, url in enumerate(event_urls, 1):
        # Skip already processed events
        if i <= start_index:
            continue
            
        try:
            # Progress logging every 50 events
            if i % 50 == 0 or i == 1:
                logger.info(f'Processing event {i}/{len(event_urls)} ({i/len(event_urls)*100:.1f}%)')
            else:
                logger.debug(f'Processing event {i}/{len(event_urls)}: {url}')
            
            event_url = session.get(url, timeout=30)
            event_soup = bs4.BeautifulSoup(event_url.text,'lxml')

           #Scrapes fight URLs from event pages and adds to list
            fight_count = 0
            for item in event_soup.find_all('a', class_='b-flag b-flag_style_green'):
                all_fight_urls.append(item.get('href'))
                fight_count += 1
            logger.debug(f'Found {fight_count} fights in event {i}')
            
            # Save checkpoint every 100 events
            if i % 100 == 0:
                save_checkpoint('fight_urls_checkpoint.csv', all_fight_urls, i)
            
            # Add delay to avoid being blocked (1-2 seconds)
            time.sleep(1.5)
            
        except requests.RequestException as e:
            logger.error(f'Error requesting event {url}: {e}')
            continue
        except Exception as e:
            logger.error(f'Error processing event {url}: {e}')
            continue
    
    # Save final checkpoint
    save_checkpoint('fight_urls_checkpoint.csv', all_fight_urls, len(event_urls))

    #Creates csv file and adds each fight url to file as a new row
    write_urls_to_csv('fight_urls.csv', all_fight_urls)

    logger.info(f'{len(all_fight_urls)} fight links successfully scraped from {len(event_urls)} events')
    

#Scrapes url of each UFC fighter from ufcstats.com
def get_fighter_urls():

    logger.info('Starting to scrape fighter links from ufcstats.com')
    session = create_session()

    #Creates a list of each fighter page alphabetically
    main_url_list = []
    for i, letter in enumerate('abcdefghijklmnopqrstuvwxyz', 1):
        try:
            logger.info(f'Requesting fighters page for letter: {letter} ({i}/26)')
            response = session.get(f'http://ufcstats.com/statistics/fighters?char={letter}&page=all', timeout=30)
            main_url_list.append(response)
            logger.debug(f'Got response for letter {letter} with status: {response.status_code}')
            #Adds 1.5s delay to avoid response(429)
            time.sleep(1.5)
        except requests.RequestException as e:
            logger.error(f'Error requesting fighters page for letter {letter}: {e}')
            continue
        
    logger.info(f'Successfully retrieved {len(main_url_list)} fighter pages')
    
    #Iterates through each page and scrapes fighter links
    main_soup_list = [bs4.BeautifulSoup(url.text,'lxml') for url in main_url_list]
    fighter_urls = []
    for i, main_link in enumerate(main_soup_list):
        try:
            links = main_link.select('a.b-link')[1::3]
            for link in links:
                fighter_urls.append(link.get('href'))
            logger.debug(f'Found {len(links)} fighters on page {i+1}')
        except Exception as e:
            logger.error(f'Error processing fighter page {i+1}: {e}')
            continue

    #Adds each link as a new row to a csv file
    write_urls_to_csv('fighter_urls.csv', fighter_urls)

    logger.info(f'{len(fighter_urls)} fighter links successfully scraped')
