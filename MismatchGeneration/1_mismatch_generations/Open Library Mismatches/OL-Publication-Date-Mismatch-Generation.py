#!/usr/bin/env python
# coding: utf-8

# # Mismatch Generation

# In[1]:


import certifi
import os
import pandas as pd
import pytz
import requests
import sys
import time

PATH_TO_UTILS = "../"
sys.path.append(PATH_TO_UTILS)

from datetime import datetime
from json.decoder import JSONDecodeError
from tqdm import tqdm
from utils import check_mf_formatting


# In[2]:


"""
The code in this block and the following block is copied from a solution from the following StackOverFlow post:
https://stackoverflow.com/questions/46119901/python-requests-cant-find-a-folder-with-a-certificate-when-converted-to-exe

I consistently ran into the error 'OSError: Could not find a suitable TLS CA certificate bundle', particularly when
trying to make API requests. This code solved the issue.
"""

def override_where():
    """ overrides certifi.core.where to return actual location of cacert.pem"""
    # Change this to match the location of cacert.pem
    return os.path.abspath("cacert.pem")


# In[3]:


# Is the program compiled?
if hasattr(sys, "frozen"):
    import certifi.core

    os.environ["REQUESTS_CA_BUNDLE"] = override_where()
    certifi.core.where = override_where

    # Delay importing until after where() has been replaced.
    import requests.utils
    import requests.adapters
    # Replace these variables in case these modules were
    # imported before we replaced certifi.core.where
    requests.utils.DEFAULT_CA_BUNDLE_PATH = override_where()
    requests.adapters.DEFAULT_CA_BUNDLE_PATH = override_where()


# In[4]:


# The csv is split into 3 parts in order to split processing into smaller chunks.
wikidata_data_1 = pd.read_csv("wikidata_query_results_works_only-1.csv")
wikidata_data_2 = pd.read_csv("wikidata_query_results_works_only-2.csv")
wikidata_data_3 = pd.read_csv("wikidata_query_results_works_only-3.csv")


# In[5]:


def convert_to_iso_8601(date_str):
    """
    This function accepts a publication date and converts it to a string
    ISO 8601 format (the format used by Wikidata).
 
    Parameters:
    date_str (str): This is a publication date from the external source.
 
    Returns:
    iso_date_str: This is the original date_str converted to ISO 8601 format.
    """
    formats = ["%Y", "%Y-%m", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y", "%d %B %Y", "%d %b %Y", "%B %Y", "%b %Y", "%Y %B", "%Y %b"]
    date_obj = None
    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
        except ValueError as e:
            continue
    if date_obj == None:      
        # If none of the formats match.
        print(f"Unrecognized date format: {date_str}")
        return None
    # Convert the date object to a string in ISO 8601 format.
    iso_date_str = date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    return iso_date_str


# In[6]:


# Testing convert_to_iso_8601().
date_str = "1996"
print(f"1996 is {convert_to_iso_8601(date_str)} in ISO 8601")
date_str = "January 1996"
print(f"January 1996 is {convert_to_iso_8601(date_str)} in ISO 8601")
date_str = "January 01, 1996"
print(f"January 01, 1996 is {convert_to_iso_8601(date_str)} in ISO 8601")
date_str = "January 1, 1996"
print(f"January 1, 1996 is {convert_to_iso_8601(date_str)} in ISO 8601")
date_str = "Jan 1 1996"
print(f"Jan 1 1996 is {convert_to_iso_8601(date_str)} in ISO 8601")


# In[7]:


def datetime_to_str(datetime):
    """
    This function converts a given datetime object into a string in ISO 8601 format.

    Parameters:
    datetime: The datetime object to be converted.

    Returns:
    str: The datetime string in ISO 8601 format.
    """
    return datetime.isoformat() + "Z"


# In[8]:


# Testing datetime_to_str().
now = datetime.now()
formatted_str = datetime_to_str(now)
print(formatted_str)


# In[9]:


def iso_8601_to_datetime(iso_string):
    """
    This function converts a string in ISO 8601 format to a datetime object.

    Parameters:
    iso_string (str): The string in ISO 8601 format.

    Returns:
    datetime: The datetime object parsed from the ISO 8601 string.
    """
    return datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%SZ")


# In[10]:


def get_editions_publication_dates(external_id):
    """
    This function accepts a work's id on an external source as input and retrieves the
    publication dates of all editions by making API requests.
 
    Parameters:
    external_id (str): This is the corresponding id on Open Library, the external source.
 
    Returns:
    pub_dates: These are the publication dates of the editions of the work according to
    Open Library in datetime format.
    """
    url = f"https://openlibrary.org/works/{external_id}/editions.json"   
    try:
        request = requests.get(url)
        request.raise_for_status()  # Raise exception for non-2xx status codes.
        editions_data = request.json()
        pub_dates = []

        for edition in editions_data["entries"]:  # Loop through the 'entries' array.
            try:
                # Access the "publish_date" attribute of the edition.
                publish_date = edition.get("publish_date")  # Using .get() to handle missing keys.
                if publish_date == None:
                    continue
                else:
                    # Convert the date to ISO 8601 format.
                    formatted_pub_date = convert_to_iso_8601(publish_date)
                    pub_dates.append(formatted_pub_date)
            except Exception as e:
                # Handle exceptions during processing.
                print(f"Error processing edition data: {e}\n")
                return None
        if pub_dates:
            return pub_dates
        else:
            # If no dates are found at all.
            return None

    except JSONDecodeError as json_err:
        print(f"JSON decode error: {json_err}\n")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}\n")
        return None


# In[11]:


# Testing get_editions_publication_dates().

# Test case when there are two editions with the same publication date.
print("Test case: Two editions with the same publication date")
external_id = "OL137253W"
print("Expected: ['2005-01-01T00:00:00Z', '2005-01-01T00:00:00Z']")
print(f"Actual: {get_editions_publication_dates(external_id)}\n")

# Test case with six editions.
print("Test case: Six editions")
external_id = "OL2963717W"
print("Expected: ['2001-01-01T00:00:00Z', '2000-11-13T00:00:00Z', '2000-11-13T00:00:00Z', '2001-01-01T00:00:00Z', '1990-01-01T00:00:00Z', '1953-01-01T00:00:00Z']")
print(f"Actual: {get_editions_publication_dates(external_id)}\n")


# In[12]:


def find_earliest_date(edition_pub_dates):
    """
    This function accepts an array of the publication dates of the editions of a work in
    datetime format, then compares the dates and returns the earliest one.
 
    Parameters:
    edition_pub_dates: These are the publication dates of the editions of a work.
 
    Returns:
    earliest_date: This is the earliest date of the publication dates.
    """
    # Initialize an empty list to store non-None dates.
    valid_dates = []
    
    # Iterate over edition_pub_dates and add non-None dates to valid_dates.
    if edition_pub_dates == None:
        return None
    for date in edition_pub_dates:
        if date is not None:
            valid_dates.append(date)
    if valid_dates:
        # Find the earliest date among the valid dates
        earliest_date = min(valid_dates)
        return earliest_date
    else:
        # If there are no valid dates, return None
        return None


# In[13]:


# Testing find_earliest_date().

# Test case when the input array contains a single date.
edition_pub_dates = [datetime(2021, 2, 1)]
earliest_date = find_earliest_date(edition_pub_dates)
print("Test case: Single date")
print("Expected:2021-01-01 00:00:00")
print("Actual:", earliest_date, "\n")

# Test case when the input array contains multiple dates.
edition_pub_dates = [
        datetime(2021, 1, 1),
        datetime(2022, 2, 2),
        datetime(2020, 12, 31),
        datetime(2021, 6, 3)
    ]
earliest_date = find_earliest_date(edition_pub_dates)
print("Test case: Multiple dates")
print("Expected: 2020-12-31 00:00:00")
print("Actual:", earliest_date)


# In[14]:


def compare_values(wd_date, ol_date):
    """
    This function compares two datetime objects and determines if they are equal.
    If the values differ as a result of the Wikidata item having a more detailed value
    (i.e. year, month, and day) while the Open Library's date matches with the existing
    detail (i.e. same year but no month or day specified), the dates are assumed to be
    the same.
 
    Parameters:
    wd_date: This is the publication date associated with the Wikidata entity.
    ol_date: This is the publication date associated with the Open Library entity.
 
    Returns:
    This function returns True if the values are the same and False if they are not.
    """
    if wd_date != ol_date:
        if wd_date.year == ol_date.year:
            if (ol_date.day == 1 and ol_date.month == 1) and ((wd_date.month != 1) or (wd_date.month == 1 and wd_date.day != 1)):
                return True
        return False
    return True


# In[15]:


# Testing compare_values()

# Test case with same dates.
wd_date = datetime(2022, 4, 15)
ol_date = datetime(2022, 4, 15)
print("Test case: Same dates")
print("Expected output: True")
print(f"Actual value: {compare_values(wd_date, ol_date)}\n")

# Test case with different years.
wd_date = datetime(2022, 4, 15)
ol_date = datetime(2023, 4, 15)
print("Test case: Different years")
print("Expected output: False")
print(f"Actual value: {compare_values(wd_date, ol_date)}\n")

# Test case with different months.
wd_date = datetime(2022, 4, 15)
ol_date = datetime(2022, 3, 15)
print("Test case: Different months")
print("Expected output: False")
print(f"Actual value: {compare_values(wd_date, ol_date)}\n")

# Test case with different days.
wd_date = datetime(2022, 4, 20)
ol_date = datetime(2022, 4, 15)
print("Test case: Different days")
print("Expected output: False")
print(f"Actual value: {compare_values(wd_date, ol_date)}\n")

# Test case with detailed WD date vs. less detailed but same OL date.
wd_date = datetime(2022, 4, 15)
ol_date = datetime(2022, 1, 1)
print("Test case: Detailed WD date vs. Less detailed OL date (same)")
print("Expected output: True")
print(f"Actual value: {compare_values(wd_date, ol_date)}\n")

# TEst case with less detailed WD date vs. detailed OL date.
wd_date = datetime(2022, 1, 1)
ol_date = datetime(2022, 4, 15)
print("Test case: Less detailed WD date vs. Detailed OL date")
print("Expected output: False")
print(f"Actual value: {compare_values(wd_date, ol_date)}\n")


# In[16]:


mismatches_1 = []
mismatches_2 = []
mismatches_3 = []


# In[17]:


class RateLimitException(Exception):
    pass


# In[19]:


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
                wd_date_datetime = datetime.fromisoformat(wd_date)
                pub_date = get_editions_publication_dates(external_id)
                ol_earliest_date = find_earliest_date(pub_date)
                if ol_earliest_date is None:  # Skip if no dates are found.
                    print(f"Publication date not found on Open Library for item {row['item']}")
                    break
                utc_timezone = pytz.utc
                ol_earliest_date = utc_timezone.localize(iso_8601_to_datetime(ol_earliest_date))
                
                if (compare_values(wd_date_datetime, ol_earliest_date) == False):
                    ol_earliest_date_str = datetime_to_str(ol_earliest_date)
                    dataframe.append({
                        "item_id": str(row["item"]),
                        "statement_guid": str(row["item"]),
                        "property_id": str("P577"),
                        "wikidata_value": str(wd_date),
                        "meta_wikidata_value": str(""), 
                        "external_value": str(ol_earliest_date_str),
                        "external_url": "https://openlibrary.org/works/" + external_id,
                        "type": str("statement")
                    })      
                break
            except ValueError:
                print(f"Invalid date format for item {row['item']}: {wd_date} or {ol_earliest_date}")
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


# In[20]:


find_mismatches(wikidata_data_1, mismatches_1)


# In[21]:


find_mismatches(wikidata_data_2, mismatches_2)


# In[22]:


find_mismatches(wikidata_data_3, mismatches_3)


# In[23]:


mismatches_1_df = pd.DataFrame(mismatches_1)


# In[24]:


# Checking mismatch accuracy.
mismatches_1_df.head()


# In[25]:


mismatches_2_df = pd.DataFrame(mismatches_2)
mismatches_3_df = pd.DataFrame(mismatches_3)


# In[26]:


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

