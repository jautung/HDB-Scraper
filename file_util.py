# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import os

OUTPUT_FOLDER = "output"
PRECOMPUTE_FILENAME = "mrt_lat_lon.csv"
LISTINGS_FILENAME = "listing_urls.csv"
BASE_INFO_FILENAME = "listing_info.csv"
FULL_RESULTS_FILENAME = "listings.csv"

def maybe_create_output_folder():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
