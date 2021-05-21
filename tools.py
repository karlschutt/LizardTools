# -*- coding: utf-8 -*-
"""
Created on Fri May 21 10:50:32 2021

@author: Karl.Schutt
"""

import requests
import pandas as pd
import xmltodict
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
import datetime
import pytz
import sys, traceback
import math
import threading
import time
from queue import Queue
import os
from requests.auth import HTTPBasicAuth
import json


"""
NOTE: compatible with api v3 & v4

"""


class lizard_api_downloader():
    

    def __init__(self,url,headers=None,page_size=5000):
        
        self.headers = headers
        
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
        page, url = self.queue.get()

        try:
            data = requests.get(url = url, headers = self.headers)
            self.succes += 1
            data = data.json()['results']
        except:
            data = []
            self.fail +=1
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
        print("Download started")
        self.results = [] # Resultaten van vorige zoekopdracht schoonmaken
        self.succes = 0
        self.fail = 0
        if nr_threads >= self.nr_pages:
            nr_threads = self.nr_pages # Voorkomen dat er te veel threads zijn voor het aantal pagina's
        for thread_nr in range(self.nr_threads):
            time.sleep(1) # Niet downloads te snel tegenlijk laten beginnen
            self.threads.append(threading.Thread(target=self.download, args=[], daemon = True)) 
            self.threads[thread_nr].start()

        while self.page != self.nr_pages:
            continue 
            
        for thread_nr in range(self.nr_threads):
            self.threads[thread_nr].join()
            print(self.threads[thread_nr])
            
        self.results = pd.DataFrame(self.results)
        # Maak na afloop alles schoon:
        self.clear()
        print('Download finished')
        
    def clear(self):
        self.page = 1
        self.end = False
        self.queue = Queue()
        self.threads = []


"""
NOTE: functions and classers below are only compatible with api v4

"""


class lizard_timeseries_downloader():

    def __init__(self,uuid,headers,page_size=10000, startdate='inf',enddate='inf'):
        
        self.headers = headers
        
        if startdate == 'inf' and enddate == 'inf':
            self.base_url = 'https://utrecht.lizard.net/api/v4/timeseries/{}/events/?page_size={}'.format(uuid, page_size)
        elif startdate == 'inf':
            self.base_url = 'https://utrecht.lizard.net/api/v4/timeseries/{}/events/?time__lte={}&page_size={}'.format(uuid,enddate, page_size)
        elif enddate == 'inf':
            self.base_url = 'https://utrecht.lizard.net/api/v4/timeseries/{}/events/?time__gte={}&page_size={}'.format(uuid,startdate, page_size)     
        else:
            self.base_url = 'https://utrecht.lizard.net/api/v4/timeseries/{}/events/?time__gte={}&time__lte={}&page_size={}'.format(uuid,startdate,enddate, page_size) 
        
        self.info = requests.get(url = self.base_url, headers = self.headers).json()
        self.count = self.info['count']
        self.nr_pages = math.ceil(self.count/page_size)        
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
        page, url = self.queue.get()
        print(url)
        print('page {} download started'.format(page))
        try:
            data = requests.get(url = url, headers = self.headers)
            print(data)
            self.succes += 1
            data = data.json()['results']
        except:
            data = []
            self.fail +=1
            print('Error: problems while retrieving data')
        
        self.lock.acquire()
        self.results+=data
        self.page+=1
        self.lock.release()

        print('stored data from page: {}'.format(page))
        print('Download finished')
    
    def prepare(self):
        for page in range(self.nr_pages):
            true_page = page+1 # Het echte paginanummer wordt aan de import thread gekoppeld
            url = self.base_url+'&page={}'.format(true_page)
            item = [true_page,url]
            print(item)
            self.queue.put(item) # Het echtepaginanummer en url in de queue toevoegen. 

    def execute(self, nr_threads):
        self.results = [] # Resultaten van vorige zoekopdracht schoonmaken
        self.succes = 0
        self.fail = 0
        if nr_threads >= self.nr_pages:
            nr_threads = self.nr_pages # Voorkomen dat er te veel threads zijn voor het aantal pagina's
        for thread_nr in range(self.nr_threads):
            time.sleep(1) # Niet downloads te snel tegenlijk laten beginnen
            self.threads.append(threading.Thread(target=self.download, args=[], daemon = True)) 
            self.threads[thread_nr].start()

        while self.page != self.nr_pages:
            continue 
            
        for thread_nr in range(self.nr_threads):
            self.threads[thread_nr].join()
            print(self.threads[thread_nr])
            
        self.results = pd.DataFrame(self.results)
        self.results.index = pd.to_datetime(
            self.results["time"], format="%Y-%m-%dT%H:%M:%SZ"
        )
        self.results = self.results.sort_index(ascending=True)

        # Maak na afloop alles netjes schoon:
        self.clear()
        
        

    def clear(self):
        self.page = 1
        self.end = False
        self.queue = Queue()
        self.threads = []



class lizard_timeseries_poster():
    
    def __init__(self,uuid,data,headers,max_upload_size=10000):
        
        self.headers = headers
        self.url = 'https://utrecht.lizard.net/api/v4/timeseries/{}/events/'.format(uuid)       
        self.data = data
        self.nr_chunks = math.ceil(len(self.data)/max_upload_size)
        self.nr_threads = min(self.nr_chunks,10)
        self.chunk_size = max_upload_size
        
        self.queue = Queue()
        self.threads = []
        self.lock = threading.Lock()
        self.chunk = 0
        self.prepare() # Alle datachunks in de queu zetten
        self.succes = 0 # Aantal chunks geslaagde post
        self.fail = 0 # Aantal chunks gefaaldepost
        self.results =[]
        
    def prepare(self):
        for chunk in range(self.nr_chunks):
            true_chunk = chunk+1 # Het echte chunk wordt aan de upload thread gekoppeld
            data = self.data[chunk*self.chunk_size:true_chunk*self.chunk_size]
            item = [true_chunk,data]
            print(item[0])
            self.queue.put(item) # Het echtepaginanummer en url in de queue toevoegen.     
            
    def upload(self):
        #while self.chunk != self.nr_chunks: # Het paginanummer betreft het echte paginanummer voor Lizard
        chunk, data = self.queue.get()
        print('chunk {} upload started'.format(chunk))
        
        # Geen foutmelding:
        data = data.to_json(orient="records")
        #data = data.replace('"geen data"', "null")
        #data = eval(data)
        res = requests.post(url = self.url, data = data, headers = self.headers)
        #res = requests.post(url = url, data = data, headers = headers)
       
        print(res)
        
        try:
            print(res.json())
        except:
            print('-')
        
        self.lock.acquire()
        self.results.append(res)
        self.chunk+=1
        self.lock.release()

        print(self.chunk)
        print(self.nr_threads)
        print('uploaded data from page: {}'.format(chunk))
        
        print('Download finished')    
 

    def execute(self):
        self.results = [] # Resultaten van vorige zoekopdracht schoonmaken
        self.succes = 0
        self.fail = 0
        for thread_nr in range(self.nr_threads):
            time.sleep(1) # Niet downloads te snel tegenlijk laten beginnen
            self.threads.append(threading.Thread(target=self.upload, args=[], daemon = True)) 
            self.threads[thread_nr].start()
            
           
        while self.chunk != self.nr_chunks: 
            continue
                    
        for thread_nr in range(self.nr_threads):
            self.threads[thread_nr].join()
            print(self.threads[thread_nr])


        self.clear()
        
        
    def clear(self):
        self.chunk = 0
        self.end = False
        self.queue = Queue()
        self.threads = []

