#Import libraries for web-scraping and saving to CSV file.

import requests
import bs4
import csv
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

#Creates a directory for scraped files if it doesn't already exist
os.makedirs('scraped_files',exist_ok=True)

#Define paths for url folder and scraped files folder
url_path = os.getcwd() + '/urls'
file_path = os.getcwd() + '/scraped_files'

#Creates csv file for scraped data
def create_csv_file():
    
    #If file does not exist, create a new CSV file with column headers
    
    if 'ufc_event_data.csv' not in os.listdir(file_path):
        with open(file_path + '/' + 'ufc_event_data.csv','w', newline='',encoding='UTF8') as ufc_event_data:
            writer = csv.writer(ufc_event_data)
            writer.writerow(['event_name',
                             'event_date',
                             'event_city',
                             'event_state',
                             'event_country',
                             'event_url'])
        logger.info('New File Created - ufc_event_data.csv')
    else:
        logger.info('Scraping to Existing File - ufc_event_data.csv')

#Ensure each url is only scraped once when script is run multiple times
def filter_duplicate_urls(event_urls):
    if 'ufc_event_data.csv' in os.listdir(file_path):
        with open(file_path + '/' + 'ufc_event_data.csv','r') as csv_file:
            reader = csv.DictReader(csv_file)
            
            #List of previously scraped urls:
            
            scraped_event_urls = [row['event_url'] for row in reader]
            #Removes scraped urls from event_urls
            for url in scraped_event_urls:
                if url in event_urls:
                    event_urls.remove(url)

#Scrapes details of each UFC event appends to CSV file 'ufc_event_data'
def scrape_events():

    #Get event URLs from file 'event_urls.csv'
    if 'event_urls.csv' in os.listdir(url_path):
        with open(url_path + '/' + 'event_urls.csv','r') as events_csv:
            reader = csv.reader(events_csv)
            event_urls = [row[0] for row in reader]
        logger.info(f'Loaded {len(event_urls)} event URLs from file')
    else:
        logger.error("Missing file - event_urls.csv. Try running 'get_urls.get_event_urls()'")
        return

    #Removes urls that have been scraped already
    filter_duplicate_urls(event_urls)
 
    urls_to_scrape = len(event_urls)
    logger.info(f'Found {urls_to_scrape} new events to scrape')

    if urls_to_scrape == 0:
        logger.info('Event data already scraped')
        
    else:
        create_csv_file()

        logger.info(f'Starting to scrape {urls_to_scrape} event URLs...')
        urls_scraped = 0
        
        with open(file_path + '/' + 'ufc_event_data.csv','a+') as csv_file:
            writer = csv.writer(csv_file)
        
            #Iterates through each event url to scrape key details
            for i, event in enumerate(event_urls, 1):
                try:
                    logger.debug(f'Processing event {i}/{urls_to_scrape}: {event}')
                    event_request = requests.get(event)
                    event_soup = bs4.BeautifulSoup(event_request.text,'lxml')
                    event_full_location = event_soup.select('li')[4].text.split(':')[1].strip().split(',')

                    event_name = event_soup.select('h2')[0].text
                    event_date = str(datetime.strptime(event_soup.select('li')[3].text.split(':')[-1].strip(), '%B %d, %Y'))
                    event_city = event_full_location[0]
                    event_country = event_full_location[-1]
                    
                    #Check event location contains state details
                    if len(event_full_location)>2:
                        event_state = event_full_location[1]
                    else:
                        event_state = 'NULL'
                    
                    #Adds new row to csv file
                    writer.writerow([event_name.strip(), 
                                     event_date[0:10], 
                                     event_city.strip(), 
                                     event_state.strip(), 
                                     event_country.strip(), 
                                     event])
                    
                    urls_scraped += 1
                    logger.debug(f'Successfully scraped event: {event_name.strip()}')
                        
                except IndexError as e:
                    logger.error(f"IndexError scraping event page: {event} - {e}")
                    continue
                except requests.RequestException as e:
                    logger.error(f"Request error for event: {event} - {e}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error scraping event: {event} - {e}")
                    continue
                
            logger.info(f'{urls_scraped}/{urls_to_scrape} events successfully scraped')
            

