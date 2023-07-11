# -*- coding: utf-8 -*-
"""
Created on Thu Jul 26 21:06:55 2018

@author: jaques
"""
import argparse
from urllib.parse import urlparse
# import time
import logging
import csv
import requests
import bs4 as bs
# import numpy as np
# from datascience import *

from throttle import Throttle


TIME_OUT=10
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


# load the page that was sent to the function
def download_page(url, domain_throttle, user_agent='wswp',
                  num_retries=2, proxies=None):
    """Download the source of the URL page provided. This is then
       converted into a BeautifulSoup object, prettified and then
       converted into a BeautifulSoup object again. After that it is
       returned for processing"""

    print(f"Downloading page: { url }")
    logger.info("Downloading page is: %s", url)

    headers = {"User-Agent": user_agent}
    try:
        domain_throttle.wait(url)
        resp = requests.get(url, headers=headers, proxies=proxies, timeout=TIME_OUT)
        html = resp.text

        if resp.status_code >= 400:
            logger.error("Website, %s, retrieval failed with %s error", url, resp.text)
            print(f"Download error: { resp.text }")
            html = None

            if num_retries and 500 >= resp.status_code < 600:
                # recursively retry 5xx HTML errors
                return download_page(url, num_retries - 1)
    except requests.exceptions.RequestException as ex:
        print(f"Download error: { ex }")
        logger.error("Page download failed: %s", ex)
        return None

    if html:
        soup = bs.BeautifulSoup(html, 'lxml')
        soup = bs.BeautifulSoup(soup.prettify(), 'lxml')

        return soup

    return None


def open_html_page(path):
    """Load the html data from a file. Mostly for debugging"""

    logger.debug("about to open and read file: %s", path)
    try:
        with open(path, 'r', encoding='utf-8') as file:
            html = file.read()

        if html:
            soup = bs.BeautifulSoup(html, 'lxml')
            soup = bs.BeautifulSoup(soup.prettify(), 'lxml')

            return soup
        else:
            return None
    except FileNotFoundError as ex:
        logger.error("The file, %s, does not exist and gave error: %s", path, ex)

    logger.debug("finished reading file. Should not get here to write log")


# find the next page to load
def find_next_url(soup):
    """Retrieve the url of the next page to download"""
    print("About to get the next URL")
    logger.debug("About to get the next URL")
    found = False

    for pages in soup.find_all('div', attrs={'class':'pagination'}):
        for _, page in enumerate(pages.find_all('a')):
            if page.has_attr('class'):
                found = True
                continue
            # the continue will continue the loop as the next one is the
            # page the page to get
            # check what happens at last page
            if found:
                logger.debug("The next page url to load is: %s", page.get('href'))
                print(f"The next page url to load is: { page.get('href') }")
                return page.get('href')

        logger.debug("no page was found.")
        return None

    logger.error('No paging data was found to determine the next page')

    return None


def extract_page_data(soup, get_header=True):
    """Extract all movie information from the page that has been downloaded"""

    logger.debug("About to extract page data")
    movielist = []
    summary_string = "#tab=summary"
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
                        elif idx == 2:
                            movieinfo.append('summary URL')

                        logger.debug("Found a header: %s",
                                     header.text.encode('utf-8').strip().decode())

                        movieinfo.append(clean_input_string(
                                header.text.encode('utf-8').strip()))

            for idx, data in enumerate(row.find_all('td')):
                anchor = data.find('a')
                if anchor:
                    ref = anchor.get('href')
                    if ref != '':
                        index = ref.find(summary_string)
                        if index != -1:
                            ref = ref[:index]
                        movieinfo.append(ref.encode('utf-8').decode())

                # force to utf-8 or errors occur when trying to convert a movie
                # name to into/from unicode. Now it just gives a funny string
                movieinfo.append(str(data.text.encode('utf-8').strip().decode()))

            # remove the first column as all pages go from 1 to 100
            # so the column number will repeat
            if len(movieinfo) != 0:
                movielist.append(movieinfo[1:])

    logger.debug("data extraction successful. Returning movie list")
    return movielist


def clean_input_string(input_string):
    """
        Remove all new line characters and all double spaces. 
        We also convert the byte string to a normal string
    """
    clean_string = input_string.replace(b"\n", b"")
    clean_string = b' '.join(clean_string.split())
    clean_string = clean_string.decode()
    return clean_string


def get_movie_data(url, delay, from_local_drive=False):
    """Main processing loop to scrape the data from the webpage"""

    logger.debug("About to get movie data from initial url: %s", url)

    base_url = f"https://{ urlparse(url).netloc }"
    logger.debug("The base url is: %s", base_url)

    page_to_download = url

    # change local file name here. It's mostly for debugging a single page
    # so I'm not passing it in as a parameter.
    if from_local_drive:
        page_to_download = "5501.html"
        logger.debug("change loading page to load from local drive instead: %s", page_to_download)

    movie_list = []
    domain_throttle = Throttle(delay, logger)

    get_header_data = True
    while page_to_download:
        # load the next page
        if from_local_drive:
            # Reads from a file, not a webpage
            logger.debug("Loading source from local drive: %s", page_to_download)
            page_soup = open_html_page(page_to_download)
        else:
            logger.debug("Loading source from web URL: %s", page_to_download)
            page_soup = download_page(page_to_download, domain_throttle)

        # extract the movie data from the page, but only get the header
        # data during the first page scrape
        if page_soup:
            movie_data = extract_page_data(page_soup, get_header_data)
            get_header_data = False

            # append the movie data to the list
            # I think this will prevent lists of lists. Could use global here.
            for row in movie_data:
                movie_list.append(row)

            # check if there is a next page to load
            next_page = find_next_url(page_soup)
            if next_page:
                logger.info("Returned URL is: %s", next_page)
                page_to_download = base_url + next_page
            else:
                logger.info("Returned URL is None")
                page_to_download = None

        if from_local_drive:
            break

    return movie_list


def write_csv_output(movie, output_file):
    """Write all of the data retrieved to a csv file"""

    try:
        logger.debug("Opening csv output file")
        with open(output_file, 'w', newline='', encoding="utf-8") as file:
            writer = csv.writer(file)

            for movie in all_movies:
                # Write each movie to the output file, also,
                # log any movies that experience problems when the csv module
                # is trying to convert the title as they might be skipped
                try:
                    writer.writerow(movie)
                except UnicodeEncodeError as ex:
                    logger.error("Error %s occurred in movie: %s", ex, movie)

    except IOError as ex:
        logger.error("Error occurred writing to csv file: %s", ex)
    finally:
        logger.info("Closing csv output file")
        file.close()


if __name__ == '__main__':
    # First page to load
    logger.info("Process started")

    page_to_load = "https://www.the-numbers.com/movie/budgets/all"
    throttle_delay = 1

    parser = argparse.ArgumentParser(
        description="Sets the proper temperatures to the corresponding layers of a gcode"
                    " file exported from Slic3r. This allows the temperature tower to have"
                    " different temperatures per block.")
    requiredNamed = parser.add_argument_group('required arguments')
    requiredNamed.add_argument('-u', '--url',
                               type=str,
                               help="Url of site to be scraped.",
                               required=False)
    requiredNamed.add_argument('-d', '--delay',
                               type=int,
                               help="Delay between page retrievals.",
                               required=False)
    args = parser.parse_args()

    logger.debug("URL argument is: %s", args.url)
    logger.debug("Delay argument : %d", args.delay)
    print(f"URL: { args.url }")
    print(f"Delay: { args.delay }")

    if args.url:
        page_to_load = args.url

    if args.delay:
        throttle_delay = args.delay

    all_movies = get_movie_data(page_to_load, throttle_delay)
#    all_movies = get_movie_data(page_to_load, from_local_drive=True)

#    # write the csv file after all processing have been completed
#    # if there is data to write.
    if len(all_movies) > 0:
        print('writing movie list to csv file')
        write_csv_output(all_movies, 'movie_data.csv')
    else:
        logger.error('No movie information was written to the csv file')

    logger.debug('Process completed')
