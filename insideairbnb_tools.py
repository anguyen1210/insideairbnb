# -*- coding: utf-8 -*-
"""
INSIDEAIRBNB TOOLS 

Created on Mon Nov 11 09:00:00 2019

@author: GROUP (Anthony & Idy)

Here we define some custom functions that will be used for our work with
'http://insideairbnb.com/get-the-data.html'.
"""
import pandas as pd
import requests
import re
import pathlib
import os.path


# =============================================================================
# This function creates a table with that lists all of the city, region, and countries 
# where we have data. We can use this table, among other things, to automatically 
# extract the names of cities in countries we are intersted in.
# =============================================================================

def make_files_index(bs_object):
    """This function accepts the BS4 object created from InsideAirBnB data and
    returns a pandas dataframe with rows listing all of the files available for
    download on the site.
    """
    regions = bs_object.find_all('h2')
    
    all_files_index = []   
    
    for r in regions:
        all_files_index.append(r.text)

    all_files_index = pd.DataFrame(all_files_index)
    all_files_index = all_files_index[0].str.rsplit(',', n = 2, expand = True)
    all_files_index.columns = ['city', 'region', 'country']
    all_files_index = all_files_index.sort_values('country')
    all_files_index.reset_index(drop=True, inplace=True)
    
    return all_files_index


# =============================================================================
# This function is used to create a string of city names that can be used as an 
# input for the 'extract_files_url()' function. The function takes as an input the 
# object created by the `make_files_index()` function, along with a string with 
# name(s) of the countries desired. Multiple country names should be separated by 
# the '|' operator.
# =============================================================================

def list_cities(files_index, country_name=None, as_list = True):
    """This function takes as input the dataframe index of all files from InsideAirBnB
    along with the countrynames of interest (separated with "|") and returns a
    list of all cities for which there is data available. If no country is 
    specified, all cities are returned. When default value `as_list` is changed 
    to False, a pandas series is returned instead.
    """
    if country_name is None:
        target = files_index.city.str.lower()
        target_list = list(target)
        target_list = "|".join(target_list)
    
    else:        
        target = files_index.city[files_index['country'].str.contains(country_name, regex=True, case=False, na=False)].str.lower()
        target_list = list(target)
        target_list = "|".join(target_list)
    
    if as_list:
        return target_list
    else: 
        return target

 
# =============================================================================
# This function extracts the url of the desired file to be extracted, and saves 
# it in a pandas dataframe. It takes, as an input, the BS4 object extracted from 
# InsideAirBnB, and the specific filename to be extracted as a string, and the 
# city name of interest. If no city is specified, the function returns the most 
# recent file from all cities. To get the full historical record of all cities, 
# set the argument 'current=False'.
# =============================================================================

def extract_file_url(bs_object, filename, city_name=None, current=True):
    """
    This function extracts the specified filename from the BS4 'content' object 
    containing the parsed InsideAirBnB webpage. If no arguments are entered, 
    the function will return all cities with the most current listings urls. 
    Otherwise, specific city names can be entered as strings. Default is to 
    return the most current listing, otherwise, all listings can be returned by 
    setting 'current=False'
    """  
    links = []
    
    if city_name is None:     
        if current:
            for table in bs_object.find_all('table'):
                a_tag = table.find('a', href=re.compile("{filename}$".format(filename=filename)))
                if a_tag:
                    listings_link = a_tag.attrs['href']
                    links.append(listings_link)
            links = pd.DataFrame(links)            
        else:
            for table in bs_object.find_all('table'):
                a_tag = table.find_all('a', href=re.compile("{filename}$".format(filename=filename)))
                for a in a_tag:
                    listings_link = a['href']
                    links.append(listings_link)
            links = pd.DataFrame(links)
    else:         
        if current:
            for table in bs_object.find_all('table', class_=re.compile(city_name, re.IGNORECASE)):
                a_tag = table.find('a', href=re.compile("{filename}$".format(filename=filename)))
                if a_tag:
                    listings_link = a_tag.attrs['href']
                    links.append(listings_link)
            links = pd.DataFrame(links)
        else:
            for table in bs_object.find_all('table', class_=re.compile(city_name, re.IGNORECASE)):
                a_tag = table.find_all('a', href=re.compile("{filename}$".format(filename=filename)))
                for a in a_tag:
                    listings_link = a['href']
                    links.append(listings_link)
            links = pd.DataFrame(links)            
    
    links.columns = ['source_url']
    return links


# =============================================================================
# This function takes as an input the dataframe created by `extract_file_url()` 
# and downloads and saves locally all of the files specificed in the 'source_url'
# column. The default behavior is to preserve existing local files. To overwrite
# local files, change the default argument to `replace=True`.
# =============================================================================

def save_insideairbnb_file(extract_file_df, replace=False):
    """
    This function takes as an input, the dataframe created by the extract_file_url() 
    function and downloadsthe file(s) from each row in the 'source_url' column, 
    and saves it locally. Locally saved files will not be overwritten unless
    the 'replace' argument is changed to 'replace=True'.
    """
    for i in range(len(extract_file_df)):
        url = extract_file_df.iloc[i,0]
        localpath = "/".join(url.split("/")[3:-3])
        date = url.split("/")[6] 
        file = url.split("/")[-1]
        filename = localpath + "/" + date + "_" + file
  
        if replace:       
            pathlib.Path(localpath).mkdir(parents=True, exist_ok=True)        
            with open(filename, "wb") as f:
                req = requests.get(url)
                f.write(req.content)        
            print('File saved locally to: ', filename)
        
        else: 
            if os.path.isfile(filename):
                print(filename, '--this file already exists locally')
            else:
                pathlib.Path(localpath).mkdir(parents=True, exist_ok=True)        
                with open(filename, "wb") as f:
                    req = requests.get(url)
                    f.write(req.content)        
                print('File saved locally to: ', filename)


# =============================================================================
# This is a helper function that takes the dataframe returned by `extract_file_url()` as
# an input, and returns a dataframe with the local filenames where these files are
# saved.
# =============================================================================
                                
def get_local_filenames(extract_file_df):
    """This helper function takes as input the df returned by `extract_file_url` and 
    returns a df with the local filenames where these url's are saved.
    """
    list_files = []
            
    for i in range(len(extract_file_df)):
            url = extract_file_df.iloc[i,0]
            localpath = "/".join(url.split("/")[3:-3])
            date = url.split("/")[6] 
            file = url.split("/")[-1]
            filename = localpath + "/" + date + "_" + file
            list_files.append(filename)
    
    list_files = pd.DataFrame(list_files)
    list_files.columns = ['local_filename']
    return list_files


# =============================================================================
# This is another helper function that reads in our locally saved csv files, adds
# a column with the source information, and then re-saves the csv file to the same
# location.
# =============================================================================

def add_source_info(filename, source_info): 
    """This is helper function that reads in our locally saved csv files, adds 
    a column with the source information, and then re-saves the csv file to the 
    same location.
    """
    df = pd.read_csv(filename, encoding='utf-8-sig')
    df['source'] = source_info
    df.to_csv(filename, encoding='utf-8-sig', index=False)
  

# =============================================================================
# This function can be used to read in multiple csv files stored locally into 
# one big pandas dataframe that can be uploaded into the SQLITE database. In 
# takes the df of local filenames returned by `get_local_filenames()` as an 
# input, and returns one large pandas df.
# =============================================================================

def read_csv_to_bigtable(local_filenames_df):
    """
    This function takes a dataframe returned from `get_local_filenames`, iterates
    over each row to read in the different csv files, and then saves all of these 
    csv files as one large dataframe.
    """
    big_table = []  
    for i in range(len(local_filenames_df)):
        df = pd.read_csv(local_filenames_df.iloc[i,0], index_col=None, header=0)
        big_table.append(df)

    big_table = pd.concat(big_table, axis = 0, ignore_index=True)
    
    return big_table


# =============================================================================
# This function can be used to convert the df returned by `extract_file_url` into 
# a table with source info that can be uploaded into the SQLITE database.
# =============================================================================

def split_source_url(import_list_df):
    """This function takes as an input the df returned by `extract_file_url` 
    and returns a df with the original 'source_url' column, along with new columns
    containing the relevant source meta information (country, region, city, last_update)     
    """
    
    split_columns = import_list_df['source_url'].str.rsplit('/', 6 ,expand=True)
    split_columns.columns = ['source', 'country', 'region', 'city','last_update', 'source_folder', 'source_filename']
    split_columns = split_columns.drop(columns = ['source', 'source_folder', 'source_filename'])
    split_columns = pd.merge(import_list_df, split_columns, left_index=True, right_index=True)    
    
    return split_columns