"""
Utils
-----

This notebooks contains utility functions for accessing Wikidata's data and checking Mismatch Finder submissions.

Contents:
    download_wikidata_json_dump,
    parse_dump_for_entries,
    validate_url,
    mf_file_creation_directions,
    check_mf_formatting
"""

import bz2
import logging
import os
import requests
from urllib.parse import urlparse
import warnings

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

logging.disable(logging.WARNING)
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
import tensorflow as tf


def download_wikidata_json_dump(target_dir="Data", dump_id=False):
    """
    Downloads the most recent stable dump of Wikidata if it is not already in the specified directory.

    Parameters
    ----------
        target_dir : str (default=wiki_dump)
            The directory in the pwd into which files should be downloaded.

        dump_id : str (default=False)
            The id of an explicit Wikidata dump that the user wants to download.

    Returns
    -------
        A downloaded bz2 compressed Wikidata dump with printed information on the downloaded file.
    """
    if not os.path.exists(target_dir):
        print(f"Making {target_dir} directory")
        os.makedirs(target_dir)

    # Find all files to download.
    base_url = "https://dumps.wikimedia.org/other/wikibase/wikidatawiki"
    dumps_index = requests.get(base_url).text
    dumps_soup_index = BeautifulSoup(dumps_index, "html.parser")

    all_dumps = [
        a["href"].split("/")[0]
        for a in dumps_soup_index.find_all("a")
        if a.has_attr("href")
    ]

    # Derive URL for the dump to download.
    if not dump_id:
        target_dump_id = "latest-all.json.bz2"
        target_local_file_path = f"{target_dir}/{target_dump_id}"
        target_dump_url = f"{base_url}/{target_dump_id}"

    else:
        if dump_id in all_dumps:
            target_dump_dir_url = f"{base_url}/{dump_id}"
            target_dump_dir_index = requests.get(target_dump_dir_url).text

            target_dump_dir_soup_index = BeautifulSoup(
                target_dump_dir_index, "html.parser"
            )
            all_dump_files = [
                a["href"].split("/")[0]
                for a in target_dump_dir_soup_index.find_all("a")
                if a.has_attr("href")
            ]

            target_dump_id = f"wikidata-{dump_id}-all.json.bz2"
            if target_dump_id in all_dump_files:
                target_local_file_path = f"{target_dir}/{target_dump_id}"
                target_dump_url = f"{target_dump_dir_url}/{target_dump_id}"

            else:
                raise ValueError(
                    "The passed value for `dump_id` does not have a bz2 compressed dump for all of Wikidata."
                )

        else:
            raise ValueError(
                "The passed value for `dump_id` is not a valid Wikidata dump."
            )

    print(f"Target Wikidata dump file is '{target_dump_id}'.")

    # Check if the dump has already been downloaded.
    if os.path.exists(target_local_file_path):
        file_size = os.stat(target_local_file_path).st_size / 1e9
        print(
            f"The desired dump already exists locally at {target_local_file_path} ({round(file_size, 2):,} GBs). Skipping download."
        )

    else:
        print(
            f"The desired dump does not exist locally. Starting download to {target_local_file_path}..."
        )

        cache_subdir = target_dir.split("/")[-1]
        cache_dir = "/".join(target_dir.split("/")[:-1])
        if cache_dir == "":
            cache_subdir = target_dir
            cache_dir = "."

        # Use Tensorflow for the download so we download iteratively and have a progress bar.
        saved_file_path = tf.keras.utils.get_file(
            fname=target_dump_id,
            origin=target_dump_url,
            extract=True,
            archive_format="auto",
            cache_subdir=cache_subdir,
            cache_dir=cache_dir,
        )

        file_size = os.stat(target_local_file_path).st_size / 1e9
        print(
            f"Downloaded a compressed dump of Wikidata QIDs ({round(file_size, 2):,} GBs)."
        )


def parse_dump_for_entries():
    """
    Parse a bz2 Wikidata dump for specific entries based on properties.
    """
    return


def validate_url(url):
    """
    Check that a value is not null and is a valid URL if so.
    """
    if not pd.isnull(url):
        try:
            url_parse = urlparse(url)
            return all([url_parse.scheme, url_parse.netloc])
        except:
            return False

    else:
        return True


def mf_file_creation_directions():
    """
    Retuns the first part of the message to users that tells them that their mismatch file isn't formatted properly.
    """
    return """
There's a problem with the DataFrame. Please see the Mismatch Finder file creation directions on GitHub:

https://github.com/wmde/wikidata-mismatch-finder/blob/main/docs/UserGuide.md#creating-a-mismatches-import-file

Directions on how to fix the DataFrame are also detailed below:
"""


def check_mf_formatting(df: pd.DataFrame):
    """
    Checks a Pandas DataFrame to see whether it will produce a valid CSV for Mismatch Finder.

    For conditions, please see:
    https://github.com/wmde/wikidata-mismatch-finder/blob/main/docs/UserGuide.md#creating-a-mismatches-import-file

    Parameters
    ----------
        df: pandas.DataFrame
            A DataFrame for which we want to run df.to_csv().

    Returns
    -------
        A message of whether or not the DataFrame is valid and directions to fix any issues if needed.
    """
    df_formatted_correctly = True
    correction_instruction = []

    # 1. Check that all required columns are included.
    required_columns = [
        "item_id",
        "statement_guid",
        "property_id",
        "wikidata_value",
        "meta_wikidata_value",
        "external_value",
        "external_url",
        "type",
    ]

    if list(df.columns) != required_columns:
        df_formatted_correctly = False
        required_columns_string = "'" + "', '".join(required_columns) + "'"
        correction_instruction.append(
            f"Please check that the following columns are present in this exact order:\n    {required_columns_string}"
        )

    # 2. Check that all QIDs and PIDs are formatted correctly.
    id_columns = ["item_id", "property_id"]
    id_columns_included = [c for c in id_columns if c in df.columns]
    columns_with_invalid_ids = []
    for c in id_columns_included:
        if c == "item_id":
            if not df[c].astype(str).str.match(r"^Q\d+$").all() == True:
                columns_with_invalid_ids.append(c)

        elif c == "property_id":
            if not df[c].astype(str).str.match(r"^P\d+$").all() == True:
                columns_with_invalid_ids.append(c)

    if columns_with_invalid_ids:
        df_formatted_correctly = False
        invalid_id_correction_message = (
            "Please assure that the following columns have valid ids:"
        )
        for c in columns_with_invalid_ids:
            invalid_id_correction_message += f"\n    - {c}"

        correction_instruction.append(invalid_id_correction_message)

    # 3. Check that there are no nulls in non-optional columns.
    required_value_columns = ["item_id", "property_id", "external_value"]
    required_value_columns_included = [
        c for c in required_value_columns if c in df.columns
    ]
    columns_with_nulls = []
    for c in required_value_columns_included:
        if df[c].isnull().values.any() == True:
            columns_with_nulls.append(c)

    if columns_with_nulls:
        df_formatted_correctly = False
        null_value_correction_message = (
            "Please assure that the following columns do not have null values:"
        )
        for c in columns_with_nulls:
            null_value_correction_message += f"\n    - {c}"

        correction_instruction.append(null_value_correction_message)

    # 4. Check that values exist for all rows where there is a statement.
    if "statement_guid" in df.columns and "wikidata_value" in df.columns:
        guids = df["statement_guid"].values
        wd_values = df["wikidata_value"].values

        check_empty_value_list = [
            not pd.isnull(wd_values[i]) and pd.isnull(guids[i])
            for i in range(len(guids))
        ]

        if True in check_empty_value_list:
            df_formatted_correctly = False
            correction_instruction.append(
                "Please assure that `statement_guid` is null only in cases where `wikidata_value` is as well."
            )

    # 5. Check that all external URLs are valid.
    if "external_url" in df.columns:
        url_validation_checks = [validate_url(u) for u in df["external_url"]]
        if False in url_validation_checks:
            df_formatted_correctly = False
            invalid_urls = [
                df["external_url"][i]
                for i in range(len(url_validation_checks))
                if url_validation_checks[i] == False
            ]
            url_correction_message = "Please check the following URLs in `external_url` to make sure that they're valid:"
            for u in invalid_urls:
                url_correction_message += f"\n    - {u}"

            correction_instruction.append(url_correction_message)

    # 6. Check that all type values are 'statement', 'qualifier' or a null value that will be made 'statement'.
    if "type" in df.columns:
        allowed_types = set(["statement", "qualifier", np.nan])
        included_types = set(df["type"].unique())
        if not set(included_types).issubset(allowed_types):
            df_formatted_correctly = False
            correction_instruction.append(
                "Please check that the `type` column contains only: 'statement', 'qualifier' or a null value."
            )

    # 7. Check that values for certain columns are less than 1,500 characters.
    check_value_length_columns = ["wikidata_value", "external_value", "external_url"]
    check_value_length_columns_included = [
        c for c in check_value_length_columns if c in df.columns
    ]
    columns_with_too_long_values = []
    for c in check_value_length_columns_included:
        if (df[c].str.len() > 1500).any() == True:
            columns_with_too_long_values.append(c)

    if columns_with_too_long_values:
        df_formatted_correctly = False
        too_long_value_correction_message = "Please assure that the following columns do not have values over 1,500 characters:"
        for c in columns_with_too_long_values:
            too_long_value_correction_message += f"\n    - {c}"

        correction_instruction.append(too_long_value_correction_message)

    # Raise exception if there's a data formatting issue or print that all checks have passed.
    if not df_formatted_correctly:
        value_error_message = mf_file_creation_directions() + "".join(
            f"\n{i+1}. {correction_instruction[i]}\n"
            for i in range(len(correction_instruction))
        )
        raise ValueError(value_error_message)

    else:
        print(
            "All checks have passed! The data is ready to be uploaded to Mismatch Finder."
        )
