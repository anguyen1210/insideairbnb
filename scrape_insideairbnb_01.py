# -*- coding: utf-8 -*-
"""
Webscraping InsideAirBnb Data, 1/2

Created on Mon Nov 11 09:00:00 2019

@anguyen1210

This script, the first of two parts, will automatically extract specified files from the InsideAirBnb 
site (http://insideairbnb.com/get-the-data.html), transform, and upload it to a
locally saved SQLITE database. Exact file names can be specified. All cities 
from specified countries will automatically be extracted.
"""
import pandas as pd
from bs4 import BeautifulSoup
import requests
import requests_cache
import sqlite3

# =============================================================================
# Set-up cache
# =============================================================================

requests_cache.install_cache('insideairbnb')

# =============================================================================
# Scrape InsideAirBnb: 
#     d/l html from website, 
#     identify regions, 
#     identify most recent "listings.csv" files
# ============================================================================

"""
Here we download the raw html from Inside AirBnB and then parse it
into text that we can work with. The final saved object, `html` will be a special
BeautifulSoup object, from which we can 'scrape' or extract the relevant 
information we are looking for.
"""

source = requests.get('http://insideairbnb.com/get-the-data.html')

if source.from_cache:
    print("Retrieved from cache")
else: 
    print("Retrieved from website")

# the response object has various attributes:
print("The status code for this get request was: ", source.status_code)
# or
print("The url has",len(source.text),"characters")

source.encoding = 'utf-8'
source = source.text

content = BeautifulSoup(source, 'lxml')
#print(content.prettify())

# regions = content.find_all('h2')
# for name in regions: print(name.text)

"""
Here we create an index of all the files that are available to download from 
the InsideAirBnB website
"""
from insideairbnb_tools import make_files_index

all_files_index = make_files_index(content)

"""
Here we get the list of city names from the countries we are intersted in that 
we will use as input for the `extract_file_url()` function.
"""

from insideairbnb_tools import list_cities

target_cities = list_cities(all_files_index, 'france|switzerland')

"""
Assuming the the most recent 'listings.csv' file is always the first listings file
listed for each city, we can just use the `find()` function from BS4 which returns
the first occurence of the search term.

One strategy is to extract all of the 'table' tags, which has the table for each city, 
and then iterate over each of these city table to `find()` the first `listings.csv`
for each city, which should be the most current. This assumes that the `listings.csv`
file is always at the top of the table for each city, and that this file is the most 
current one.
"""
#To get the most current listings:

from insideairbnb_tools import extract_file_url

import_list = extract_file_url(content, 'listings.csv', target_cities) 
  
        
# =============================================================================
# Download specified city files from countries of interest: 
#     input region name, 
#     d/l current `listings.csv` for input, 
#     store in local folder, (folder structure should match the country/state/city/date)
# =============================================================================

from insideairbnb_tools import save_insideairbnb_file

save_insideairbnb_file(import_list)         


# =============================================================================
# Transform locally saved .csv files: 
#     input downloaded listings.csv filename, 
#     opens the csv file, 
#     makes the necessary modifications (adding the source, date, region, etc), 
#     saves the content into a new, transformed csv file in the same directory)
# =============================================================================

"""First we create a list of all the local filenames that we want to update"""
from insideairbnb_tools import get_local_filenames

local_files = get_local_filenames(import_list)

"""Next we combine our `local_files` with our `import_list`, and iterate over 
all the rows to update, where the first column is the file we will update, and 
the second column is the source info"""    

from insideairbnb_tools import add_source_info

filename_source = pd.DataFrame.merge(local_files, import_list, left_index=True, right_index=True)
  
filename_source.apply(lambda filename_source: add_source_info(filename_source['local_filename'], filename_source['source_url']), axis=1)


# =============================================================================
# Load tranformed data into local database: 
#     input transformed table from D5, 
#     insert into SQLITE database
#     (#first create the SQLITE database)
# =============================================================================

"""First we read all of our downloaded listings.csv files into one big table"""

from insideairbnb_tools import read_csv_to_bigtable

listings = read_csv_to_bigtable(local_files)

"""Additionally, we can convert our `import_list` file as a separate `source_info`
table that we can reference later."""

from insideairbnb_tools import split_source_url

source_info = split_source_url(import_list)


"""Next we create the SQLITE file and insert the table"""

#connect to the new database
#note, python will create a new database if it does not find the name entered here
conn = sqlite3.connect('insideairbnb.db') 

#insert the data frames of the latest listings of the 5 cities in the database
listings.to_sql("listings", conn, index=False, if_exists="replace")

#insert the data frame with the source info on these listings into the database
source_info.to_sql("source_info", conn, index=False, if_exists="replace")


# =============================================================================
# Check and close the database
# =============================================================================

#create a cursor to execute sqlite commands
c = conn.cursor()

#display the name of the tables present in the database
c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
)
available_sqlite_tables = (c.fetchall())

#display the name of the 5 first listings in the city of Lausanne
pd.read_sql_query("select * from listings limit 5;", conn) 
pd.read_sql_query("select * from source_info;", conn)

#to commit and close the changes done in the data base
conn.commit()
conn.close()