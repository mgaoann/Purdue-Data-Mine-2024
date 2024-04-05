"""
Script that splits a large mismatch file into separate files.

Note: the upload limit for the Mismatch Finder API is 10 MB.

Please see the Mismatch Finder User Guide for more information:
    https://github.com/wmde/wikidata-mismatch-finder/blob/development/docs/UserGuide.md

Usage:
    python3 split_mismatch_file.py \
        --mismatch-file MISMATCH_FILE \
        --mismatch-files-dir MISMATCH_FILE_DIR \
        --delete-mismatch-file \
        --verbose

Abbreviated argument usage:
    python3 split_mismatch_file.py \
        -mf MISMATCH_FILE \
        -mfd MISMATCH_FILE_DIR \
        -del \
        -v
"""

import argparse
import os

import pandas as pd
import numpy as np


# Section: functions for the script.
def lower(s: str):
    """
    Returns a string with the first letter lowercased.
    """
    return s[:1].lower() + s[1:] if s else ""


# Section: Set arguments for the script.
parser = argparse.ArgumentParser()
parser._actions[0].help = "Show this help message and exit."
parser.add_argument(
    "-v", "--verbose", help="Increase output verbosity.", action="store_true"
)
parser.add_argument(
    "-mf",
    "--mismatch-file",
    help="Path to the CSV file containing mismatches that should be split into smaller files (<10 MB).",
)
parser.add_argument(
    "-mfd",
    "--mismatch-files-dir",
    help="(Optional) Path to a directory where split mismatches should be saved. The directory will be made if it doesn't already exist.",
)
parser.add_argument(
    "-del",
    "--delete-mismatch-file",
    help="(Optional) Delete the original mismatch file passed via the --mismatch-file (-mf) argument.",
    action="store_true",
)

args = parser.parse_args()

VERBOSE = args.verbose
MISMATCH_FILE = args.mismatch_file
MISMATCH_FILES_DIR = args.mismatch_files_dir
DELETE_MISMATCH_FILE = args.delete_mismatch_file

# Section: Assertions for passed arguments.
assert MISMATCH_FILE, f"""Please provide a path via the --mismatch-file (-mf) argument:
--mismatch-file (-mf): a {lower(parser._actions[2].help)}"""

# Assert that the file exists and that it is a CSV that is greater than 10 MB.
if MISMATCH_FILE:
    assert os.path.isfile(
        MISMATCH_FILE
    ), f"Please provide a {lower(parser._actions[2].help)}"

    assert (
        MISMATCH_FILE[-4:] == ".csv"
    ), f"Please provide a {lower(parser._actions[2].help)}"

    mf_size = os.path.getsize(MISMATCH_FILE) >> 20

    assert (
        mf_size > 10
    ), "The size of the mismatch file passed via the --mismatch-file (-mf) argument is less than the import file size limit of 10 MB. You do not need to run this script, and are ready to upload your CSV to Mismatch Finder via the upload API! Please use the `upload_mismatches.py` file or see other instructions in the user guide at https://github.com/wmde/wikidata-mismatch-finder/blob/development/docs/UserGuide.md."

# Section: Create the needed directory for the output CSVs.
if os.name == "nt":  # Windows
    dir_path_separator = "\\"
else:
    dir_path_separator = "/"

if dir_path_separator in MISMATCH_FILE:
    path_to_mismatch_file = f"{dir_path_separator}".join(
        MISMATCH_FILE.split(dir_path_separator)[:-1]
    )
    mismatch_file_name = MISMATCH_FILE.split(dir_path_separator)[-1]
    mismatch_dir_name = os.path.splitext(mismatch_file_name)[0]
    mismatch_files_dir_path = (
        path_to_mismatch_file + dir_path_separator + mismatch_dir_name
    )

else:
    mismatch_file_name = MISMATCH_FILE
    mismatch_dir_name = os.path.splitext(mismatch_file_name)[0]
    mismatch_files_dir_path = mismatch_dir_name

if not MISMATCH_FILES_DIR:
    if VERBOSE:
        print(
            "No output directory has been provided. Creating one based on the mismatch file name."
        )

    assert not os.path.exists(
        mismatch_files_dir_path
    ), "No output directory has been provided, but a directory that matches the mismatch file name passed to the --mismatch-file (-mf) argument exists. Please pass a desired directory name."

    os.makedirs(mismatch_files_dir_path, exist_ok=True)

else:
    if not os.path.exists(MISMATCH_FILES_DIR):
        if VERBOSE:
            print(
                "The output mismatch files directory does not exist and will be created."
            )

        os.makedirs(MISMATCH_FILES_DIR)

    else:
        assert (
            len(os.listdir(MISMATCH_FILES_DIR)) == 0
        ), "The mismatch directory passed to the --mismatch-files-directory (-mfd) argument is not empty. This directory should be empty to assure that directory based uploads to Mismatch Finder with the resulting split CSV files will not send invalid files."

        if VERBOSE:
            print(
                "The output mismatch files directory exists and is empty. Splitting and saving mismatches."
            )

    mismatch_files_dir_path = MISMATCH_FILES_DIR

# Section: Calculate how many CSVs should be made.
# In the following, the second quantity is True (= 1) if there is a remainder of the division.
number_of_split_mismatch_files = int(mf_size / 10) + (mf_size % 10 > 0)

both_or_all = "both" if number_of_split_mismatch_files == 2 else "all"
if VERBOSE:
    print(
        f"The mismatch file {mismatch_file_name} will be split into {number_of_split_mismatch_files} different files that will {both_or_all} be 10 MB or less."
    )

# Section: Split and save the resulting CSVs.
df_mismatch_file = pd.read_csv(MISMATCH_FILE)

mismatch_file_dfs = np.array_split(df_mismatch_file, number_of_split_mismatch_files)
mismatch_file_df_names = [
    f"{os.path.splitext(mismatch_file_name)[0]}_{i+1}.csv"
    for i in range(len(mismatch_file_dfs))
]

for i, df in enumerate(mismatch_file_dfs):
    df.to_csv(
        f"{mismatch_files_dir_path}{dir_path_separator}{mismatch_file_df_names[i]}",
        encoding="utf-8",
        index=False,
    )

created_mismatch_files_print_str = "\n".join(mismatch_file_df_names)
print(
    f"The following mismatch files were created in the {mismatch_files_dir_path} directory:\n\n{created_mismatch_files_print_str}"
)
print(
    "\nYou're now ready to upload your mismatch files to Mismatch Finder via the upload API! Please use the `upload_mismatches.py` file or see other instructions in the user guide at https://github.com/wmde/wikidata-mismatch-finder/blob/development/docs/UserGuide.md."
)
