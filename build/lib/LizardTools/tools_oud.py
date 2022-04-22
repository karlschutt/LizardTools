# -*- coding: utf-8 -*-
"""
Created on Fri May 21 10:50:32 2021

@author: Karl.Schutt
"""

import requests
import pandas as pd
import math
import threading
import time
from queue import Queue
import concurrent.futures
from itertools import repeat

"""
NOTE: compatible with api v3 & v4

"""


class lizard_api_downloader():
    """
    Class voor het efficient downloaden van asset data vanuit Lizard
    """

    def __init__(self,url,headers=None,print_log='NO',page_size=5000):
        
        self.headers = headers
        self.print_log=print_log
        if url[len(url)-1]=='/':        
            self.base_url = '{}?page_size={}'.format(url, page_size)
        else:
            self.base_url = '{}&page_size={}'.format(url, page_size)
            
        
        if headers == None:
            self.info = requests.get(url = self.base_url, headers = self.headers).json()
        else:
            self.info = requests.get(url = self.base_url, headers = self.headers).json()            
        self.count = self.info['count']
        self.nr_pages = math.ceil(self.count/page_size) 
        if self.print_log!='NO':
            print("Aantal assets: {}".format(self.count))
            print("Aantal pagina's: {}".format(self.nr_pages))
        self.results = []
        self.end = False
        self.queue = Queue()
        self.threads = []
        self.lock = threading.Lock()
        self.page = 0
        self.prepare()
        self.succes = 0 # Aantal pagina's met geslaagde download
        self.fail = 0 # Aantal pagina's met gefaalde download

    def download(self):
        while self.page != self.nr_pages: # Het paginanummer betreft het echte paginanummer voor Lizard
            page, url = self.queue.get()

            try:
                data = requests.get(url = url, headers = self.headers)
                self.succes += 1
                data = data.json()['results']
            except:
                data = []
                self.fail +=1
            if self.print_log!='NO':
                print('Error: problems while retrieving data')
            
            self.lock.acquire()
            self.results+=data
            self.page+=1
            self.lock.release()

    
    def prepare(self):
        for page in range(self.nr_pages):
            true_page = page+1 # Het echte paginanummer wordt aan de import thread gekoppeld
            url = self.base_url+'&page={}'.format(true_page)
            item = [true_page,url]
            self.queue.put(item) # Het echtepaginanummer en url in de queue toevoegen. 

    def execute(self, nr_threads):
        if self.print_log!='NO':
            print("Download started")
        self.results = [] # Resultaten van vorige zoekopdracht schoonmaken
        self.succes = 0
        self.fail = 0
        if nr_threads >= self.nr_pages:
            nr_threads = self.nr_pages # Voorkomen dat er te veel threads zijn voor het aantal pagina's
        for thread_nr in range(nr_threads):
            thread = threading.Thread(target=self.download, args=[], daemon = True)   
            thread.start()
            thread.join()
        # while self.page != self.nr_pages:
        #     continue 
        self.results = pd.DataFrame(self.results)
        # Maak na afloop alles schoon:
        self.clear()
        if self.print_log!='NO':
            print('Download finished')
        
    def clear(self):
        self.page = 1
        self.end = False
        self.queue = Queue()
        self.threads = []


"""
NOTE: compatible with api v4

"""

class lizard_timeseries_downloader():
    
    # =============================================================================
    # Initialise
    # =============================================================================

    def __init__(self,uuid,headers,page_size=10000,print_log='NO', startdate='inf',enddate='inf',startdate_modified=None, enddate_modified=None):
        
        self.headers = headers
        self.print_log=print_log
        
        if startdate == 'inf' and enddate == 'inf':
            self.base_url = 'https://demo.lizard.net/api/v4/timeseries/{}/events/?page_size={}'.format(uuid, page_size)
        elif startdate == 'inf':
            self.base_url = 'https://demo.lizard.net/api/v4/timeseries/{}/events/?time__lte={}&page_size={}'.format(uuid,enddate, page_size)
        elif enddate == 'inf':
            self.base_url = 'https://demo.lizard.net/api/v4/timeseries/{}/events/?time__gte={}&page_size={}'.format(uuid,startdate, page_size)     
        else:
            self.base_url = 'https://demo.lizard.net/api/v4/timeseries/{}/events/?time__gte={}&time__lte={}&page_size={}'.format(uuid,startdate,enddate, page_size) 
        if self.print_log!='NO':        
            print(self.base_url)
			
        if startdate_modified != None:
            self.base_url = self.base_url + '&last_modified__gte={}'.format(startdate_modified)	
        if enddate_modified != None:
            self.base_url = self.base_url + '&last_modified__lte={}'.format(enddate_modified)				
			
        self.info = requests.get(url = self.base_url, headers = self.headers).json()
        self.count = self.info['count']
        self.nr_pages = math.ceil(self.count/page_size) 
        if self.print_log!='NO':
            print(self.nr_pages)
        self.results = []
        self.end = False
        self.N2 = 50
        self.queue = Queue()
        self.queue2 = Queue(self.N2)
        self.threads = []
        self.lock = threading.Lock()
        self.page = 0
        self.prepare()
        self.succes = 0 # Aantal pagina's met geslaagde download
        self.fail = 0 # Aantal pagina's met gefaalde download

    def download(self):
        while self.page != self.nr_pages: # Het paginanummer betreft het echte paginanummer voor Lizard
            page, url = self.queue.get()
            if self.print_log!='NO':
                print(url)
                print('page {} download started'.format(page))
            try:
                data = requests.get(url = url, headers = self.headers)
                if self.print_log!='NO':
                    print(data)
                self.succes += 1
                data = data.json()['results']
            except:
                data = []
                self.fail +=1
                if self.print_log!='NO':
                    print('problems while retrieving data')
            
            self.lock.acquire()
            self.results+=data
            self.page+=1
            self.lock.release()
			
            if self.print_log!='NO':
                print('stored data from page: {}'.format(page))
		
        if self.print_log!='NO':
            print('Download finished')
    
    def prepare(self):
        for page in range(self.nr_pages):
            true_page = page+1 # Het echte paginanummer wordt aan de import thread gekoppeld
            url = self.base_url+'&page={}'.format(true_page)
            item = [true_page,url]
            if self.print_log!='NO':
                print(item)
            self.queue.put(item) # Het echtepaginanummer en url in de queue toevoegen. 

    def execute(self, nr_threads):
        self.results = [] # Resultaten van vorige zoekopdracht schoonmaken
        self.succes = 0
        self.fail = 0
        if nr_threads >= self.nr_pages:
            nr_threads = self.nr_pages # Voorkomen dat er te veel threads zijn voor het aantal pagina's
        for thread_nr in range(nr_threads):
            time.sleep(1) # Niet downloads te snel tegenlijk laten beginnen
            thread = threading.Thread(target=self.download, args=[], daemon = True)   
            thread.start()
        while self.page != self.nr_pages:
            continue 
        self.results = pd.DataFrame(self.results)
        # self.results.index = pd.to_datetime(
            # self.results["time"], format="%Y-%m-%dT%H:%M:%SZ"
        # )
        self.results = self.results.sort_index(ascending=True)

        # Maak na afloop alles netjes schoon:
        self.clear()
        
        

    def clear(self):
        self.page = 1
        self.end = False
        self.queue = Queue()
        self.threads = []


