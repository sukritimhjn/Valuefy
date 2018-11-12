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

    def __init__(self, base_url, root_url, file_internal_urls, file_scraped_pages, ConnectionPoolSize):
        self.base_url = base_url
        self.root_url = root_url
        self.pool = ThreadPoolExecutor(max_workers=5)
        self._allInternalURLS = set([])
        self.scraped_pages = set([])
        self.to_crawl = Queue()
        self.to_crawl.put(self.base_url)
        #Pool object
        self.ConnectionPoolSize = ConnectionPoolSize
        self.ConnectionPool = ReusablePool(self.ConnectionPoolSize)
        self.file_internal_urls =  file_internal_urls
        self.file_scraped_pages = file_scraped_pages
        self._lock = Lock()

    def parse_links(self, responseData):
        #print("\n IN PARSE LINKS")
        #print(responseData)
        links = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', responseData)
        #print("\n LINKS = ")
        print(links)
        internalLinks = list(set([l for l in links if l.strip('/') not in self.scraped_pages]))
        print("\n INTERNAL = ")
        #print(internalLinks)
        self.write_to_file(internalLinks, self.file_internal_urls)

        #print("\n root_url")
        #print(self.root_url)
        for link in internalLinks:
            self._lock.acquire()
            self._allInternalURLS.add(link)
            self._lock.release()
            if(re.findall(self.root_url, link)):
                #print(link)
                #print(re.findall(self.root_url, link))
                if(not(re.search(r'((.jpg|.jpeg|.png|.gif|.bmp|.m4a|.ico|.css|.js))', link))):
                    #https://miro.medium.com/max/304/1*rO16Pz6DxyDNCRjLbF0bOA.jpeg)
                    #print('\n===LINK===' + link)
                    #print(link)
                    self._lock.acquire()
                    self.to_crawl.put(link)
                    self._lock.release()
                

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
                print('in post_scrape_callback')
                #print(resp)
                if resp :
                    try:
                        respData = resp.decode('utf-8')
                        self.parse_links(str(respData))
                    except Exception as e:
                        #swallow exception
                        #print('post_scrape_callback Error :')
                        return
                
    def scrape_page(self, url):
        #try: get_connection, get_data; finally: release connection
        try:
            #connection pooling 
            conn = self.ConnectionPool.get_connection()
            resp = conn.get_data(url)
            print('in scrape page')
            #print(resp)
            '''
            headers = {}
            headers['User-Agent'] = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"
            #connection pooling
            req = urllib.request.Request(url, headers = headers)
            resp = urllib.request.urlopen(req, timeout=10)
            '''
        except Exception as e:
            print('scrape_page error : '+ str(e))
            # swallow exception
            pass
        finally:
            self.ConnectionPool.release_connection(conn)
            #print("released connection")
        #print(resp)
        return resp

    def run_scraper(self):
        while True:
            try:
                target_url = self.to_crawl.get(timeout=60)
                #print("\n === TARGET URL ===")
                #print(target_url)
                #print("\n")
                if target_url not in self.scraped_pages:
                    self.scraped_pages.add(target_url)
                    self.write_to_file([target_url], self.file_scraped_pages)
                    job = self.pool.submit(self.scrape_page, target_url)
                    job.add_done_callback(self.post_scrape_callback)
            except Empty:
                self.ConnectionPool.end_pool()
                return (self._allInternalURLS, self.scraped_pages)
            except Exception as e:
                print('run_scraper error : '+ str(e))
                continue

    def write_to_file(self, urls, file_name):
        self._lock.acquire()
        try:
            with open(file_name, 'a') as csvFile:
                writer = csv.writer(csvFile)
                for url in list(urls):
                    writer.writerow([url])
            csvFile.close()
        finally:
            self._lock.release()

if __name__ == '__main__':
    internalURLs = 'internalURLs.csv'
    scraped_pages = 'scraped_pages.csv'
    ConnectionPoolSize = 5
    s = MultiThreadScraper('https://www.medium.com', 'medium.com', internalURLs, scraped_pages, ConnectionPoolSize)
    #'https://www.untravel.com/')
    # 'https://nutrabay.com/'
    allInternalURLs, scraped_pages = s.run_scraper()

    # with open('internalURLs.csv', 'w') as csvFile:
    #     writer = csv.writer(csvFile)
    #     for url in list(allInternalURLs):
    #         writer.writerow([url])
    # csvFile.close()

    # with open('scraped_pages.csv', 'w') as csvFile:
    #     writer = csv.writer(csvFile)
    #     for url in list(scraped_pages):
    #         writer.writerow([url])
    # csvFile.close()
