# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import csv
import dataclasses
import logging
import os
import math
import typing
import file_util
import gmaps_util

logger = logging.getLogger("HDB Scraper: HDB Full Results")


def _full_results_with_mrt_info():
    logger.info(
        f"Starting to compile full outputs from {file_util.BASE_INFO_FILENAME} and {file_util.PRECOMPUTE_FILENAME}"
    )

    output_file_exists, already_processed_urls = _get_already_processed_urls()
    gmaps = gmaps_util.get_gmaps_client()
    mrt_station_map = _get_mrt_station_map_from_file()

    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.BASE_INFO_FILENAME),
        newline="",
        encoding="utf-8",
    ) as base_info_file, open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.FULL_RESULTS_FILENAME),
        "a",
        newline="",
        encoding="utf-8",
    ) as full_results_file:
        base_info_dict_reader = csv.DictReader(base_info_file)
        full_results_writer = csv.writer(full_results_file)

        if not output_file_exists:
            _write_full_results_headers(full_results_writer=full_results_writer)

        listing_dicts = sorted(
            list(base_info_dict_reader), key=lambda row: row["Postal code"]
        )
        num_listings = len(listing_dicts)
        num_written = 0
        for listing_index, listing_dict in enumerate(listing_dicts):
            assert "Link" in listing_dict and listing_dict["Link"]
            listing_url = listing_dict["Link"]
            debug_logging_name = (
                f"{listing_url} (listing #{listing_index+1} of {num_listings})"
            )

            if listing_url in already_processed_urls:
                logger.info(
                    f"Skipping {debug_logging_name} because it is already processed"
                )
                continue

            nearest_mrt_info = _get_nearest_mrt_info_for_listing(
                listing_dict=listing_dict,
                debug_logging_name=debug_logging_name,
                mrt_station_map=mrt_station_map,
                gmaps=gmaps,
            )
            if nearest_mrt_info is None:
                logger.warning(
                    f"Unable to scrape {debug_logging_name}, skipping writing to {file_util.FULL_RESULTS_FILENAME}"
                )
                continue

            _write_full_results_row(
                full_results_writer=full_results_writer,
                listing_dict=listing_dict,
                nearest_mrt_info=nearest_mrt_info,
            )
            num_written += 1
            base_info_file.flush()
            os.fsync(base_info_file.fileno())

        logger.info(
            f"Successfully exported {num_written} scraped results to {file_util.BASE_INFO_FILENAME}"
        )


def _get_already_processed_urls():
    logger.debug(
        f"Getting already-processed listings from {file_util.FULL_RESULTS_FILENAME}"
    )
    if not os.path.exists(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.FULL_RESULTS_FILENAME)
    ):
        logger.debug(f"{file_util.FULL_RESULTS_FILENAME} does not exist yet!")
        return False, {}
    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.FULL_RESULTS_FILENAME),
        newline="",
        encoding="utf-8",
    ) as full_results_file:
        dict_reader = csv.DictReader(full_results_file)
        already_processed_urls = set(row["Link"] for row in dict_reader)
        logger.info(f"{len(already_processed_urls)} already-processed listings found!")
        return True, already_processed_urls


def _get_mrt_station_map_from_file():
    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.PRECOMPUTE_FILENAME),
        newline="",
        encoding="utf-8",
    ) as mrt_precompute_file:
        reader = csv.reader(mrt_precompute_file)
        return {row[0]: (float(row[1]), float(row[2])) for row in reader}


@dataclasses.dataclass
class NearestMRTInfo:
    nearest_mrt_station: typing.Any
    straight_line_distance_km: typing.Any
    walking_distance_km: typing.Any
    walking_duration_mins: typing.Any


def _get_nearest_mrt_info_for_listing(
    listing_dict, debug_logging_name, gmaps, mrt_station_map
):
    postal_code = listing_dict["Postal code"]
    nearest_mrt_info = _nearest_mrt_info_for_postal_code(
        postal_code=listing_dict["Postal code"],
        gmaps=gmaps,
        mrt_station_map=mrt_station_map,
    )
    return nearest_mrt_info


def _nearest_mrt_info_for_postal_code(postal_code, gmaps, mrt_station_map):
    logger.debug(f"Finding nearest MRT info for 'S{postal_code}'")
    postal_code_address = f"{postal_code}, Singapore"
    postal_code_lat, postal_code_lon = gmaps_util.get_lat_lon_from_address(
        gmaps=gmaps, address=postal_code_address
    )
    mrt_station_distances_km = [
        (
            mrt_station,
            _haversine_distance_km(
                lat1=postal_code_lat,
                lon1=postal_code_lon,
                lat2=mrt_station_lat,
                lon2=mrt_station_lon,
            ),
        )
        for mrt_station, (mrt_station_lat, mrt_station_lon) in mrt_station_map.items()
    ]
    nearest_mrt_station, nearest_mrt_station_distance_km = min(
        mrt_station_distances_km, key=lambda x: x[1]
    )
    logger.debug(
        f"Computed that closest MRT to 'S{postal_code}' is {nearest_mrt_station}"
    )
    gmaps_result = gmaps.distance_matrix(
        origins=[postal_code_address],
        destinations=[nearest_mrt_station],
        mode="walking",
    )
    gmaps_result_inner = gmaps_result["rows"][0]["elements"][0]
    distance_metres = gmaps_result_inner["distance"]["value"]
    duration_seconds = gmaps_result_inner["duration"]["value"]
    logger.debug(
        f"Google Maps says that 'S{postal_code}' to {nearest_mrt_station} takes {(duration_seconds / 60):.2f}mins"
    )
    return NearestMRTInfo(
        nearest_mrt_station=nearest_mrt_station,
        straight_line_distance_km=nearest_mrt_station_distance_km,
        walking_distance_km=distance_metres / 1000,
        walking_duration_mins=duration_seconds / 60,
    )


# Based on great-circle distance
def _haversine_distance_km(lat1, lon1, lat2, lon2):
    R = 6371.0  # Radius of the Earth in kilometers
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _write_full_results_headers(full_results_writer):
    full_results_writer.writerow(
        [
            # Key info
            "Link",
            "Address",
            "Postal code",
            "HDB type",
            "Ethnic eligibility",
            "Area (sqm)",
            "Price ($)",
            "Storey range",
            "Remaining lease (years)",
            "Nearest MRT station",
            "Walking duration to MRT (mins)",
            "Last updated date",
            "Free-form description (provided by seller)",
            # Useful info
            "Number of bedrooms",
            "Number of bathrooms",
            "Balcony",
            "Upcoming upgrading plans?",
            # Fallback scraped info
            "Sub-address [fallback if postal code is 'None']",
            "Town [fallback if nearest MRT station is 'None']",
            "Remaining lease [fallback if parsed remaining lease is 'None']",
            "Last updated [fallback if last updated date is 'None']",
            "Straight line distance to MRT (km) [fallback if MRT duration is 'None']",
            "Walking distance to MRT (km) [fallback if MRT duration is 'None']",
            # Mostly irrelevant info (for us)
            "Will seller want to extend their stay (up to 3 months)? [less relevant for us]",
            "Enhanced Contra Facility (ECF) Allowed? [irrelevant for us]",
            "SPR eligibility [irrelevant for us]",
        ]
    )


def _write_full_results_row(full_results_writer, listing_dict, nearest_mrt_info):
    pass


def main():
    parser = argparse.ArgumentParser(description="HDB Scraper")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logger.setLevel(args.log_level)

    _full_results_with_mrt_info()


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()
