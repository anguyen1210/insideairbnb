# -*- coding: utf-8 -*-
"""
Webscraping InsideAirBnb Data, 2/2

@anguyen1210

This script, the second of two parts, extends our InsideAirBnB (http://insideairbnb.com/get-the-data.html) 
webscraper to extract the complete historical listings of the specified files 
of interest. Additionally, we implement an email alert system that will notify
us when new cities and/or new data is made available on the InsideAirBnb site.
"""

from bs4 import BeautifulSoup
import requests
import requests_cache


# =============================================================================
# Get historical data:
#     Load all historical data for the countries of interest
#     Make eventual changes in the database
# =============================================================================

requests_cache.install_cache('insideairbnb')

source = requests.get('http://insideairbnb.com/get-the-data.html')

if source.from_cache:
    print("Retrieved from cache")
else: 
    print("Retrieved from website")
    
source.encoding = 'utf-8'
source = source.text

content = BeautifulSoup(source, 'lxml')

# -----------------------------------------------------------------------------

#Here we create an index of all files available for download
from insideairbnb_tools import make_files_index

all_files_index = make_files_index(content)


#Next we get all the city names from the countries we are interested in
from insideairbnb_tools import list_cities

target_cities = list_cities(all_files_index, 'france|switzerland')

#Next, we download the historical listings for all cities in france and switzerland
from insideairbnb_tools import extract_file_url

import_historical_listings =  extract_file_url(content, 'listings.csv', target_cities, current=False)

#Finally, we can save these historical listings

from insideairbnb_tools import save_inside_airbnb

save_inside_airbnb(import_historical_listings) #We don't actually save all of these here, because it's alot


# =============================================================================
# Receive email alerts:
#     Send emails if new data for existing cities or new cities is avilable.
#     Distinguish between new cities and new data for old cities.
# =============================================================================

#here we create two different import lists that we can use to check our functions
target_newcities = list_cities(all_files_index, 'france|switzerland|belgium')

import_test_new = extract_file_url(content, 'listings.csv', target_newcities, current=False)
import_test_existing = extract_file_url(content, 'listings.csv', 'vaud')

#Here we do a simple check to see if there are new files in the two test lists
from insideairbnb_alert import check_import_list_for_new

check_import_list_for_new(import_test_new)
check_import_list_for_new(import_test_existing)

#Here we do a check on the two lists, and send an email if there's new files by
#changing the default 'send_email' argument to true.

check_import_list_for_new(import_test_new, send_email = True)
check_import_list_for_new(import_test_existing, send_email = True)




