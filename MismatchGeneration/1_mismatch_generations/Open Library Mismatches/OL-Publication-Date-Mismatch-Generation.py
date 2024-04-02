#!/usr/bin/env python
# coding: utf-8

# # Mismatch Generation

# In[1]:


import ast
import json
import sys
import requests

import numpy as np
import pandas as pd
import pymysql
from datetime import datetime
from SPARQLWrapper import JSON, POST, SPARQLWrapper

PATH_TO_UTILS = "../"
sys.path.append(PATH_TO_UTILS)

from utils import check_mf_formatting
from tqdm import tqdm


# In[2]:


# The csv is split into 3 parts in order to split processing into smaller chunks.


# In[3]:


wikidata_data_1 = pd.read_csv("wikidata_query_results_works_only-1.csv")
wikidata_data_2 = pd.read_csv("wikidata_query_results_works_only-2.csv")
wikidata_data_3 = pd.read_csv("wikidata_query_results_works_only-3.csv")


# In[4]:


def get_first_publication_date(external_id):
    """
    This function accepts a work's id on an external source as input and retrieves its first
    publication date by making an API request.
 
    Parameters:
    external_id (str): This is the corresponding id on Open Library, the external source.
 
    Returns:
    first_publication_date: This is the first date of publication according to Open Library.
    """
    url = f"https://openlibrary.org/works/{external_id}.json"
    request = requests.get(url)
    book_data = request.json()
    
    if "first_publish_date" in book_data:
        first_publication_date = book_data["first_publish_date"]
        return first_publication_date
    return None


# In[5]:


# Testing get_first_publication_date().
print(get_first_publication_date("OL27448W"))


# In[6]:


def convert_to_datetime(date_str):
    """
    This function accepts a publication date and converts it to ISO 8601 format
    (the format used by Wikidata).
 
    Parameters:
    date_str (date_str): This is a publication date from the external source.
 
    Returns:
    iso_date_str: This is the original date_str converted to ISO 8601 format.
    """
    if len(date_str) == 4:
        date_obj = datetime.strptime(date_str, "%Y")
    elif ',' in date_str:
        date_obj = datetime.strptime(date_str, "%B %d, %Y")
    else:
        date_obj = datetime.strptime(date_str, "%B %Y")
    # Convert the parsed date object to a string in ISO 8601 format.
    iso_date_str = date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    return iso_date_str


# In[7]:


# Testing convert_to_datetime().
date_str = "1996"
print(f"1996 is {convert_to_datetime(date_str)} in datetime")
date_str = "January 1996"
print(f"January 1996 is {convert_to_datetime(date_str)} in datetime")
date_str = "January 01, 1996"
print(f"January 01, 1996 is {convert_to_datetime(date_str)} in datetime")
date_str = "January 1, 1996"
print(f"January 1, 1996 is {convert_to_datetime(date_str)} in datetime")


# In[8]:


mismatches_1 = []
mismatches_2 = []
mismatches_3 = []


# In[9]:


def find_mismatches(wikidata_data, dataframe):
    """
    This function accepts a dataframe with the item's Wikidata data and a dataframe which the
    mismatches will be appended to, and crosschecks the item's publication date on Wikidata
    with the data on the external source. If the corresponding item has no publication date on
    the external source, it will not be appended to the dataframe as a mismatch.
 
    Parameters:
    wikidata_data: The pandas dataframe which contains the Wikidata ID, publication date,
    and external source ID of the items of interest.
    dataframe: The pandas dataframe which the mismatches will be appended to.
    """
    total_rows = len(wikidata_data)
    for index, row in tqdm(wikidata_data.iterrows(), total=total_rows, desc="Processing"):
        external_id = row["openLibraryID"]
        wd_date = row["publicationDate"]
        while True:
            try:
                pub_date = get_first_publication_date(external_id)
                if pub_date is None:  # Skip if pub_date is None.
                    print(f"Publication date not found on Open Library for item {row['item']}")
                    break
                try:
                    ol_formatted_date = convert_to_datetime(pub_date)
                except Exception as e:
                    print(f"Error converting date for item {row['item']}: {e}")
                    continue
                if (wd_date != ol_formatted_date):
                    try:
                        date_str = datetime.strptime(wd_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%SZ")
                    except ValueError as e:
                        print(f"Error: Time converting date for item {row['item']}: {e}")
                        continue
                    dataframe.append({
                        "item_id": str(row["item"]),
                        "statement_guid": str(""),
                        "property_id": str("P577"),
                        "wikidata_value": str(date_str),
                        "meta_wikidata_value": str(""), 
                        "external_value": str(pub_date),
                        "external_url": str(""),
                        "type": str("statement")
                    })      
                break
            except JSONDecodeError as e:
                print(f"Error decoding JSON for item {row['item']}: {e}")
                break
            except RateLimitException as e:
                if e.status_code == 403:
                    print("Rate limited. Waiting and retrying...")
                    time.sleep(60) # Wait for 60 seconds before retrying.
                else:
                    raise e


# In[10]:


find_mismatches(wikidata_data_1, mismatches_1)


# In[11]:


find_mismatches(wikidata_data_2, mismatches_2)


# In[12]:


find_mismatches(wikidata_data_3, mismatches_3)


# In[21]:


mismatches_1_df = pd.DataFrame(mismatches_1)


# In[23]:


# Checking mismatch accuracy.
mismatches_1_df.head()


# In[24]:


mismatches_2_df = pd.DataFrame(mismatches_2)
mismatches_3_df = pd.DataFrame(mismatches_3)


# In[25]:


combined_mismatches_df = pd.concat([mismatches_1_df, mismatches_2_df, mismatches_3_df], ignore_index=True)


# In[27]:


combined_mismatches_df.shape


# In[28]:


mismatch_dataframe = pd.DataFrame(combined_mismatches_df)


# In[29]:


mismatch_dataframe.dtypes


# In[30]:


check_mf_formatting(mismatch_dataframe)


# In[31]:


mismatch_dataframe.to_csv('openlibrary_publication_date_mismatches.csv', index=False)

