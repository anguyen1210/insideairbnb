# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 16:01:31 2019

@author: anguyen1210
"""
import pandas as pd
import re
import numpy as np


# =============================================================================
# This function returns the distance in kilometers between two points given in 
# latititue and longitude using the Haversine formula, as described on
# https://en.wikipedia.org/wiki/Haversine_formula.
# =============================================================================

def haversine(lat0, lon0, lat1, lon1):
    """Returns the distance in kilometers between two points given in 
latitude and longitude"""
    lat0, lon0, lat1, lon1 = map(np.radians, [lat0, lon0, lat1, lon1]) #convert to radians
    h = (np.sin((lat1-lat0)/2)**2) + np.cos(lat0) * np.cos(lat1) * (np.sin((lon1-lon0)/2)**2)
    d = 2*6371* np.arcsin(np.sqrt(h))
    return(d)



# =============================================================================
# This function extracts the entire table listings all available files for each
# city on the InsideAirBnb database. It takes the BS4 object as an input and
# returns a pandas dataframe (df) as a default.
# =============================================================================

def extract_table(bs_object, city_name, df=False):
    """This function accepts the city name as a string and extracts the entire 
    city table from the BS4 object `html` containing the InsideAirBnB html.
    df=False returns a BS4 object. ds=True returns a pandas dataframe"""
    
    table_city = bs_object.find('table', class_=re.compile(city_name, re.IGNORECASE))
    results = pd.DataFrame()
    rows = table_city.find_all('tr')

    for row in rows:        
        row_data = [td.a['href'] if td.find('a') else 
                 ''.join(td.stripped_strings)
                 for td in row.find_all('td')]
        temp_df = pd.DataFrame([row_data])
        results = results.append(temp_df)

    cols = table_city.find_all('th')
    col_names = [c.text for c in cols]
    col_names = [x.strip() for x in col_names]

    results.columns = col_names
    results = results.dropna(how = 'all')    
    results = results.reset_index(drop = True)
    
    if df:
        return results
    else:
        return table_city
   
