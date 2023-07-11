"""
Module throttles pages per domain to prevent overwhelming the domain.
"""
import time
from urllib.parse import urlparse


class Throttle:
    """Add a delay between downloads to the same domain to prevent
       overloading the system being scraped."""
    def __init__(self, delay, logger):
        # amount of delay between download for each domain
        self.delay = delay
        self.logger = logger
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """Wait between retrieval of web pages."""
        # extract domain from url.
        domain = urlparse(url).netloc
        self.logger.debug("The domain name, %s, is derived from %s", domain, url)

        # check if the domain is in the domains dictionary
        last_accessed = self.domains.get(domain)

        # check when the last time the domain was accessed.
        # If recently, sleep for a while (set by delay)
        if self.delay > 0 and last_accessed is not None:
            self.logger.info("Throttling domain: %s to %d seconds", domain, self.delay)

            sleep_secs = self.delay - (time.time() - last_accessed)
            if sleep_secs > 0:
                # domains has been accessed recently so need to sleep
                time.sleep(sleep_secs)

        # update the last accessed time
        self.logger.debug("Time of domain in dict: %s", self.domains)
        self.domains[domain] = time.time()
        self.logger.debug("New time of domain in dict: %s", self.domains)
