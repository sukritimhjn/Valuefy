# WEB SCRAPING
Scrapping 'https://www.medium.com'

Extracting all internal URLs from all pages of medium.com, recursively without using any external scraping library;
and keeping the connection pooling size equal to 5 at all times.

1) The programe runs best in Python 3.7.0
2) Keep 'thread.py' and 'scrapper.py' in same folder
3) Run by typing : ' python thread.py' from the same folder on terminal
4) All the internal URLs will be saved on 'internalURLS.csv' and scraped medium pages on 'scraped_pages.csv' in the same folder
