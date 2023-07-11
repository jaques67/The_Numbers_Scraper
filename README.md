# Movie Scraper

This scraper uses a class to verify when a page was scraped from a domain and then waits for a specified period to expire before retrieving the next page.
This is used to prevent overwhelming a website.

## Usage
`python [--url=URL] [--delay=DELAY] TheNumberScraper.py`

Both url and delay is optional:

* url - URL of website to be scraped - Default value of "https://www.the-numbers.com/movie/budgets/all"
* delay - DELAY in seconds between fetching each web page - Default value of 5 seconds
