from scrapper import ReusableConnection, ReusablePool
import re
import sys
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
#import urllib.request
import csv
from threading import Lock

sys.setrecursionlimit(1000)

class MultiThreadScraper:

    def __init__(self, base_url):
        self.base_url = base_url
        self.root_url = urlparse(self.base_url).netloc
        self.pool = ThreadPoolExecutor(max_workers=5)
        self._allInternalURLS = set([])
        self.scraped_pages = set([])
        self.to_crawl = Queue()
        self.to_crawl.put(self.base_url)
        #Pool object
        self.ConnectionPool = ReusablePool(5)

    def parse_links(self, responseData):
        links = re.findall('"((http)s?://.*?)"', responseData)
        internalLinks = list(set([l[0] for l in links if l[0] not in self.scraped_pages]))

        for link in internalLinks:
            self._allInternalURLS.add(link)
            if(re.findall(self.root_url, link)):
                #print(link)
                #print(re.findall(self.root_url, link))
                if(not(re.search(r'(^[\S]+(\.(?i)(jpg|jpeg|png|gif|bmp|m4a|ico))$)', link))):
                    #print('None')
                    self.to_crawl.put(link)
                

    def post_scrape_callback(self, res):
        if res.cancelled():
            print('Future object was cancelled')
        elif res.done():
            error = res.exception()
            if error:
                print('Future threw exception')
            else:
                result = res.result()
                resp = result 
                if resp :
                    try:
                        respData = resp.decode('utf-8')
                        self.parse_links(str(respData))
                    except Exception as e:
                        #swallow exception
                        #print('post_scrape_callback Error : ' + str(e) + ' hello' + str(resp.decode('utf-8')))
                        return
                
    def scrape_page(self, url):
        #try: get_connection, get_data; finally: release connection
        try:
            #connection pooling 
            conn = self.ConnectionPool.get_connection()
            resp = conn.get_data(url)
            '''
            headers = {}
            headers['User-Agent'] = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"
            #connection pooling
            req = urllib.request.Request(url, headers = headers)
            resp = urllib.request.urlopen(req, timeout=10)
            '''
            return resp
        except Exception as e:
            #print('scrape_page error : '+ str(e))
            # log exception
            # swallow exception
            pass
        finally:
            self.ConnectionPool.release_connection(conn)

    def run_scraper(self):
        while True:
            try:
                target_url = self.to_crawl.get(timeout=60)
                if target_url not in self.scraped_pages:
                    self.scraped_pages.add(target_url)
                    job = self.pool.submit(self.scrape_page, target_url)
                    job.add_done_callback(self.post_scrape_callback)
            except Empty:
                self.ConnectionPool.end_pool()
                return (self._allInternalURLS, self.scraped_pages)
            except Exception as e:
                #print('run_scraper error : '+ str(e))
                continue

if __name__ == '__main__':
    s = MultiThreadScraper('https://www.medium.com')
    #'https://www.untravel.com/')
    # 'https://nutrabay.com/'
    allInternalURLs, scraped_pages = s.run_scraper()

    with open('internalURLs.csv', 'w') as csvFile:
        writer = csv.writer(csvFile)
        for url in list(allInternalURLs):
            writer.writerow([url])
    csvFile.close()
    with open('scraped_pages.csv', 'w') as csvFile:
        writer = csv.writer(csvFile)
        for url in list(scraped_pages):
            writer.writerow([url])

    csvFile.close()
