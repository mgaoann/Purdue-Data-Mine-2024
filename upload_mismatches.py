"""
Script to send post requests to the Mismatch Finder API given a filepath or directory of mismatch files.

Please see the Mismatch Finder User Guide for more information:
    https://github.com/wmde/wikidata-mismatch-finder/blob/development/docs/UserGuide.md

The example cURL request for Mismatch Finder is:
    curl -X POST "https://mismatch-finder.toolforge.org/api/imports" \
        -H "Accept: application/json" \
        -H "Authorization: Bearer {ACCESS_TOKEN}" \
        -F "mismatch_file=@PATH_TO_CSV_FILE" \
        -F "description=DESCRIPTION" \
        -F "external_source=SOURCE" \
        -F "external_source_url=URL" \
        -F "expires=YYYY-MM-DD"

Usage:

Note: Please only pass arguments to EITHER --mismatch-file OR --mismatch-files-dir.
Note: --description, --external-source-url and --expires are optional.

python3 upload_mismatches.py \
    --access-token ACCESS_TOKEN \
    --mismatch-file MISMATCH_FILE \
    --mismatch-files-dir MISMATCH_FILE_DIR \
    --description DESCRIPTION \
    --external-source EXTERNAL_SOURCE \
    --external-source-url EXTERNAL_SOURCE_URL \
    --expires EXPIRES \
    --verbose

Example:

python3 upload_mismatches.py \
    --access-token YOUR_ACCESS_TOKEN \
    --mismatch-file mismatches_test.csv \
    --description "Test mismatches upload" \
    --external-source "Test Source" \
    --external-source-url "https://www.wikidata.org" \
    --verbose

Abbreviated arguments usage:

Note: Please only pass arguments to EITHER -mf OR -mfd.
Note: --des, --src and --exp are optional.

python3 upload_mismatches.py \
    -pat ACCESS_TOKEN \
    -mf MISMATCH_FILE \
    -mfd MISMATCH_FILE_DIR \
    -des DESCRIPTION \
    -src EXTERNAL_SOURCE \
    -url EXTERNAL_SOURCE_URL \
    -exp EXPIRES \
    -v

Abbreviated arguments example:

python3 upload_mismatches.py \
    -pat YOUR_ACCESS_TOKEN \
    -mf mismatches_test.csv \
    -des "Test mismatches upload" \
    -src "Test Source" \
    -url "https://www.wikidata.org" \
    -v
"""

import argparse
import os
import requests

from tqdm import tqdm


# Section: Helper classes functions for the script.
class terminal_colors:
    """
    Class for easily applying the Wikidata brand colors in the terminal and resetting.
    """

    WD_RED = "\033[38;2;153;0;0m"
    WD_GREEN = "\033[38;2;51;153;102m"
    WD_BLUE = "\033[38;2;0;102;153m"
    RESET = "\033[0m"


def print_thank_you_message():
    """
    Prints a multicolored thank you message to the command line.
    """
    heart_char = "\u2665"
    print(
        "Thank you for helping to improve Wikidata's data!"
        + " "
        + f"{terminal_colors.WD_RED}{heart_char}"
        + f"{terminal_colors.WD_GREEN}{heart_char}"
        + f"{terminal_colors.WD_BLUE}{heart_char}"
        + f"{terminal_colors.WD_GREEN}{heart_char}"
        + f"{terminal_colors.RESET}"
    )


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
    "-pat", "--access-token", help="Your Mismatch Finder API access token."
)
parser.add_argument(
    "-mf",
    "--mismatch-file",
    help="(Optional) Path to the CSV file containing mismatches to import to Mismatch Finder.",
)
parser.add_argument(
    "-mfd",
    "--mismatch-files-dir",
    help="(Optional) Path to a directory containing only CSV files with mismatches to import to Mismatch Finder.",
)
parser.add_argument(
    "-des",
    "--description",
    help="(Optional) A short text (up to 350 characters) to describe this import.",
)
parser.add_argument(
    "-src",
    "--external-source",
    help="The name of the external source that mismatches are coming from (up to 100 characters).",
)
parser.add_argument(
    "-url",
    "--external-source-url",
    help="(Optional) A URL to the external source that mismatches are coming from.",
)
parser.add_argument(
    "-exp",
    "--expires",
    help="(Optional) An ISO formatted date to describe the date where the mismatches imported will be no longer relevant. If omitted, mismatches from the import will expire after 6 months by default. A timeframe of a few weeks or months is recommended.",
)

args = parser.parse_args()

VERBOSE = args.verbose
ACCESS_TOKEN = args.access_token
MISMATCH_FILE = args.mismatch_file
MISMATCH_FILES_DIR = args.mismatch_files_dir
DESCRIPTION = args.description
EXTERNAL_SOURCE = args.external_source
EXTERNAL_SOURCE_URL = args.external_source_url
EXPIRES = args.expires

# Section: Assertions for passed arguments.
assert ACCESS_TOKEN, f"Please provide {lower(parser._actions[2].help)}"

assert (
    MISMATCH_FILE or MISMATCH_FILES_DIR and not (MISMATCH_FILE and MISMATCH_FILES_DIR)
), f"""Please provide a path via EITHER the --mismatch-file (-mf) OR --mismatch-files-dir (-mfd) arguments:
--mismatch-file (-mf): a {lower(parser._actions[3].help)}
--mismatch-files-dir (-mfd): a {lower(parser._actions[4].help)}"""

assert EXTERNAL_SOURCE, f"Please provide {lower(parser._actions[6].help)}"

# Assert that the file exists and that it is a CSV that is less than 10 MB.
if MISMATCH_FILE:
    assert os.path.isfile(
        MISMATCH_FILE
    ), f"Mismatch file not found. Please provide a {lower(parser._actions[3].help.split('(Optional) ')[1])}"

    assert (
        MISMATCH_FILE[-4:] == ".csv"
    ), f"Mismatch file not a CSV. Please provide a {lower(parser._actions[3].help.split('(Optional) ')[1])}"

    mf_size = os.path.getsize(MISMATCH_FILE) >> 20

    assert (
        mf_size < 10
    ), "The size of the passed mismatch file via the --mismatch-file (-mf) argument is greater than the Mismatch Finder import file size limit of 10 MB. Please break it down into smaller CSV files using `split_mismatch_file.py` and pass a directory containing only these CSVs to the --mismatch-files-dir (-mdf) argument."

# Assert that the directory exists and that the contents of the directory are all CSVs that are less than 10 MB.
if MISMATCH_FILES_DIR:
    assert os.path.isdir(
        MISMATCH_FILES_DIR
    ), f"Mismatch directory not found. Please provide a {lower(parser._actions[4].help.split('(Optional) ')[1])}"

    mfd_files = [
        f
        for f in os.listdir(MISMATCH_FILES_DIR)
        if os.path.isfile(os.path.join(MISMATCH_FILES_DIR, f))
    ]
    mfd_mf_files = [f for f in mfd_files if f[-4:] == ".csv"]
    mfd_remaining_files = set(mfd_files) - set(mfd_mf_files)

    assert not mfd_remaining_files, f"Mismatch directory is not empty. Please provide a {lower(parser._actions[4].help.split('(Optional) ')[1])}"

    mfd_mf_paths = []
    for mf in mfd_mf_files:
        if os.name == "nt":  # Windows
            dir_path_separator = "\\"
        else:
            dir_path_separator = "/"

        # Remove potential trailing slash or backlash from the end of the directory path.
        if MISMATCH_FILES_DIR.endswith(dir_path_separator):
            mfd_path = MISMATCH_FILES_DIR[:-1]
        else:
            mfd_path = MISMATCH_FILES_DIR

        mfd_mf_paths.append(mfd_path + dir_path_separator + mf)

    too_large_mismatch_files = []
    for mf_path in mfd_mf_paths:
        mfd_mf_size = os.path.getsize(mf_path) >> 20

        if mfd_mf_size > 10:
            too_large_mismatch_files.append(mf_path)

        too_large_mismatch_files_print_st = "\n".join(too_large_mismatch_files)

        assert not too_large_mismatch_files, f"The size of one of the passed mismatch files via the --mismatch-files-dir (-mdf) argument is greater than the import file size limit of 10 MB. Please break it down into smaller CSV files and pass a directory containing only these CSVs to the --mismatch-files-dir (-mdf) argument. Mismatch files that are too large are:\n\n{too_large_mismatch_files_print_st}"

# Section: Prepare components of the request.
MF_API_IMPORT_URL = "https://mismatch-finder.toolforge.org/api/imports"
headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}

params = {"external_source": EXTERNAL_SOURCE}

if DESCRIPTION:
    params["description"] = DESCRIPTION

if EXTERNAL_SOURCE_URL:
    params["external_source_url"] = EXTERNAL_SOURCE_URL

if EXPIRES:
    params["expires"] = EXPIRES

# Section: Make upload request(s).
if MISMATCH_FILE:
    if VERBOSE:
        print(
            f"Uploading the mismatch file {MISMATCH_FILE} to the Wikidata Mismatch Finder..."
        )

    try:
        with open(MISMATCH_FILE, "rb") as mismatch_file_binary:
            r = requests.post(
                MF_API_IMPORT_URL,
                files={"mismatch_file": mismatch_file_binary},
                headers=headers,
                params=params,
            )
            r.raise_for_status()

        print(
            f"Mismatch file {MISMATCH_FILE} was successfully uploaded to the Wikidata Mismatch Finder."
        )
        print_thank_you_message()

    except requests.exceptions.HTTPError as e:
        print(
            f"There was an error in trying to upload the mismatch file {MISMATCH_FILE}."
        )
        print("Response: " + e.response.text)

elif MISMATCH_FILES_DIR:
    sorted_mfd_mf_paths = sorted(mfd_mf_paths)

    if VERBOSE:
        mismatch_files_to_upload_print_str = "\n".join(sorted_mfd_mf_paths)
        print(
            f"The following mismatch files will be uploaded to the Wikidata Mismatch Finder:\n\n{mismatch_files_to_upload_print_str}\n"
        )

    for mf in tqdm(
        sorted_mfd_mf_paths,
        desc="Mismatch files uploaded",
        unit="file",
        disable=not VERBOSE,
    ):
        try:
            with open(mf, "rb") as mismatch_file_binary:
                r = requests.post(
                    MF_API_IMPORT_URL,
                    files={"mismatch_file": mismatch_file_binary},
                    headers=headers,
                    params=params,
                )
                r.raise_for_status()

            # To assure some level of logging for if there is an error with one of the uploads.
            if not VERBOSE:
                print(
                    f"Mismatch file {mf} was successfully uploaded to the Wikidata Mismatch Finder."
                )

        except requests.exceptions.HTTPError as e:
            print(f"There was an error in trying to upload the mismatch file {mf}.")
            print("Response: " + e.response.text)

    print(
        "All mismatch files were successfully uploaded to the Wikidata Mismatch Finder."
    )
    print_thank_you_message()
