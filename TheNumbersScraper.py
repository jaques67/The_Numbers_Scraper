# -*- coding: utf-8 -*-
"""
Created on Thu Jul 26 21:06:55 2018

@author: jaques
"""

import requests
import bs4 as bs
from urllib.parse import urlparse
import time
import logging
import csv
import numpy as np
from datascience import *


# Create log message format
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
# Setup logger with a file and the log level. Set the message format
# and clear the log each time the program is run by setting filemode=w  
logging.basicConfig(filename="Scraper.log", 
                    level=logging.INFO,
                    format=LOG_FORMAT,
                    filemode = 'w')
# Create a logger object
logger = logging.getLogger()


class Throttle:
    """Add a delay between downloads to the same domain to prevent
       overloading the system being scraped."""
    def __init__(self, delay):
        # amount of delay between download for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}
        
    def wait(self, url):
        # get the domain name. 
        # https://www.the-numbers.com/movie/budgets/all/501 will become 
        # www.the-numbers.com
        domain = urlparse(url).netloc
        logger.debug('The domain name, {}, is derived from {}'
                     .format(domain, url))
        
        # check if the domain is in the domains dictionary
        last_accessed = self.domains.get(domain)
        
        # check when the last time the domain was accessed.
        # If recently, sleep for a while (set by delay)
        if self.delay > 0 and last_accessed is not None:
            print('Throttling domain: {} to {} seconds'
                  .format(domain, self.delay))
            logger.info("Throttling domain: {} to {} seconds"
                        .format(domain, self.delay))
            
            sleep_secs = self.delay - (time.time() - last_accessed)
            if sleep_secs > 0:
                # domains has been accessed recently
                # so need to sleep
                time.sleep(sleep_secs)

        # update the last accessed time
        logger.debug("Time of domain in dict: {}".format(self.domains))
        self.domains[domain] = time.time()
        logger.debug("New time of domain in dict: {}".format(self.domains))


# load the page that was sent to the function
def download_page(url, throttle, user_agent='wswp',
                  num_retries=2, proxies=None):
    """Download the source of the URL page provided. This is then
       converted into a BeautifulSoup object, prettified and then
       converted into a BeautifulSoup object again. After that it is
       returned for processing"""
       
    print('Downloading page:', url)
    logger.info('Downloading page is: {}'.format(url))
    
    headers = {'User-Agent': user_agent}
    try:
        throttle.wait(url)
        resp = requests.get(url, headers=headers, proxies=proxies)
        html = resp.text
        if resp.status_code >= 400:
            logger.error('Website, {}, retrieval failed with {} error'
                         .format(url, resp.text))
            print('Download error:', resp.text)
            html = None
            if num_retries and 500 >= resp.status_code < 600:
                # recursively retry 5xx HTML errors
                return download_page(url, num_retries - 1)
    except requests.exceptions.RequestException as e:
        print('Download error:', e)
        logger.error('Page download failed: {}'.format(e))
        html = None
        
    if html != None:
        soup = bs.BeautifulSoup(html, 'lxml')
        soup = bs.BeautifulSoup(soup.prettify(), 'lxml')
    
        return soup
    else:
        return None


def open_html_page(path):
    """Load the html data from a file. Mostly for debugging"""
    
    logger.debug('about to open and read file: {}'.format(path))
    try:
        with open(path, "r", encoding='utf-8') as f:
            html = f.read()
    
        if html != None:
            soup = bs.BeautifulSoup(html, 'lxml')
            soup = bs.BeautifulSoup(soup.prettify(), 'lxml')
        
            return soup
        else:
            return None
    except FileNotFoundError as e:
        logger.error('The file, {}, does not exist and gave error: {}'
                     .format(path, e))
    
    logger.debug('finished reading file. Should not get here to write log')

# find the next page to load
def find_next_url(soup):
    """Retrieve the url of the next page to download"""
    print('About to get the next URL')
    logger.debug('About to get the next URL')
    found = False
    
    for pages in soup.find_all('div', attrs={'class':'pagination'}):
        for idx, page in enumerate(pages.find_all('a')):
            if page.has_attr('class'):
                found = True
                continue
            # the continue will continue the loop as the next one is the
            # page the page to get
            # check what happens at last page
            if found:
                logger.debug('The next page url to load is: {}'.
                      format(page.get('href')))
                print('The next page url to load is: {}'.
                      format(page.get('href')))
                return page.get('href')
        else:
            # No URL was found in loop, so exit
            logger.debug('no page found. idx is {} and page is: {}'
                         .format(idx, page))
            return None
    else:
        logger.error('No paging data was found to determine the next page')
    
    logger.error('How did it get here? No pagination class?')
    return None


def extract_page_data(soup, get_header=True):
    """Extract all movie information from the page that has been downloaded"""
    
    logger.debug('About to extract page data')
    movielist = []
    for table in soup.find_all('table'):
        for row in table.find_all('tr'):
            movieinfo = []
            # find the data in the header row
            
            # Check if headers have been retrieved during first page extract
            if get_header:
                for idx, header in enumerate(row.find_all('th')):
                    if len(header.text.strip()) == 0:
                        movieinfo.append('Column')
                    else:
                        # Add the url of the date and summary to header
                        if idx == 1:
                            movieinfo.append('date URL')
                        if idx == 2:
                            movieinfo.append('summary URL')
                        logger.debug('Found a header: '
                                     .format(header.text.encode('utf-8').strip()))                            
                        movieinfo.append(header.text.encode('utf-8').strip())
                        
            for data in row.find_all('td'):
                anchor = data.find('a')
                if anchor != None:
                    ref = anchor.get('href')
                    if ref != '':
                        movieinfo.append(ref.encode('utf-8'))

                # force to utf-8 or errors occur when trying to convert a movie
                # name to into/from unicode. Now it just gives a funny string
                movieinfo.append(str(data.text.encode('utf-8').strip()))
    
            # remove the first column as all pages go from 1 to 100
            # so the column number will repeat
            if len(movieinfo) != 0:
                movielist.append(movieinfo[1:])

    logger.debug('data extraction successful. Returning movie list')
    return movielist


def get_movie_data(url, delay=5, from_local_drive=False):
    """Main processing loop to scrape the data from the webpage"""
    
    logger.debug('About to get movie data from initial url: {}'
                 .format(url))
    
    base_url = 'https://' + urlparse(url).netloc
    logger.debug('The base url is: {}'.format(base_url))
    
    page_to_download = url
    
    # change local file name here. It's mostly for debugging a single page
    # so I'm not passing it in as a parameter.
    if from_local_drive:
        page_to_download = '5501.html'
        logger.debug('change loading page to load from local drive instead: {}'
                     .format(page_to_download))
        
    movie_list = []
    throttle = Throttle(delay)
    
    get_header_data = True
    while page_to_download != None:
        # load the next page
        if from_local_drive:
            # Reads from a file, not a webpage
            logger.debug('Loading source from local drive: {}'
                         .format(page_to_download))
            page_soup = open_html_page(page_to_download)
        else:
            logger.debug('Loading source from web URL: {}'
                         .format(page_to_download))
            page_soup = download_page(page_to_download, throttle)
        
        # extract the movie data from the page, but only get the header
        # data during the first page scrape
        if page_soup != None:
            movie_data = extract_page_data(page_soup, get_header_data)
            get_header_data = False
        
            # append the movie data to the list
            # I think this will prevent lists of lists. Could use global here.
            for row in movie_data:
                movie_list.append(row)

            # check if there is a next page to load
            next_page = find_next_url(page_soup)
            if next_page != None:
                logger.info('Returned URL is: {}'.format(next_page))
                page_to_download = base_url + next_page
            else:
                logger.info('Returned URL is None')
                page_to_download = None
            
        if from_local_drive:
            break
   
    return movie_list


def write_csv_output(movie, output_file):
    """Write all of the data retrieved to a csv file"""
    
    try:
        logger.debug('Opening csv output file')
        file = open(output_file, 'w', newline='')
        writer = csv.writer(file)
        
        for movie in all_movies:
            # Write each movie to the output file, also,
            # log any movies that experience problems when the csv module
            # is trying to convert the title as they might be skipped
            try:
                writer.writerow(movie)
            except UnicodeEncodeError as ex:
                logger.error("Error {} occurred in movie: {}"
                     .format(ex, movie))
    except Exception as ex:
        logger.error('Error occurred writing to csv file: {}'
                     .format(ex))
    finally:
        logger.info('Closing csv output file')
        file.close()

    
if __name__ == '__main__':
    # First page to load
    page_to_load = 'https://www.the-numbers.com/movie/budgets/all'
    
    all_movies = get_movie_data(page_to_load)
#    all_movies = get_movie_data(page_to_load, from_local_drive=True)
    
#    # write the csv file after all processing have been completed
#    # if there is data to write.
    if len(all_movies) > 0:
        print('writing movie list to csv file')
        write_csv_output(all_movies, 'movie_data.csv')
    else:
        logger.error('No movie information was written to the csv file')

    # returning movie data in case further processing might be required.
    logger.debug('Processing of get movie data is complete')
