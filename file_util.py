# pylint: disable=import-error,missing-module-docstring,missing-function-docstring
import os

OUTPUT_FOLDER = "output"
PRECOMPUTE_FILENAME = "mrt_lat_lon.csv"


def maybe_create_output_folder():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
