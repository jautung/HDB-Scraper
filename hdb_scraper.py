# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import bs4
import csv
import dataclasses
import datetime
import logging
import math
import re
import typing
import browser_util
import file_util


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


@dataclasses.dataclass
class NearestMRTInfo:
    nearest_mrt_station: typing.Any
    straight_line_distance_km: typing.Any
    walking_distance_km: typing.Any
    walking_duration_mins: typing.Any


def _nearest_mrt_info(postal_code, gmaps, mrt_station_map):
    logger.debug(f"Finding nearest MRT info for 'S{postal_code}'")
    postal_code_address = f"{postal_code}, Singapore"
    postal_code_lat, postal_code_lon = file_util.get_lat_lon_from_address(
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


CACHED_NEAREST_MRT_INFO = dict()


def _get_nearest_mrt_info_for_listing(listing, gmaps, mrt_station_map):
    postal_code = listing.header_info.postal_code
    if postal_code in CACHED_NEAREST_MRT_INFO:
        logger.debug(f"Hit cache for nearest MRT info for 'S{postal_code}'!")
        return CACHED_NEAREST_MRT_INFO[postal_code]
    logger.debug(f"Missed cache for nearest MRT info for 'S{postal_code}'")
    nearest_mrt_info = _nearest_mrt_info(
        postal_code=listing.header_info.postal_code,
        gmaps=gmaps,
        mrt_station_map=mrt_station_map,
    )
    CACHED_NEAREST_MRT_INFO[postal_code] = nearest_mrt_info
    return nearest_mrt_info


def _augment_listings_with_mrt_info(listings, gmaps, mrt_station_map):
    logger.info(f"Augmenting {len(listings)} listings with MRT info")
    for listing in listings:
        if listing is None:
            continue
        listing.nearest_mrt_info = _get_nearest_mrt_info_for_listing(
            listing=listing, gmaps=gmaps, mrt_station_map=mrt_station_map
        )


def main():
    parser = argparse.ArgumentParser(description="HDB Scraper")
    # parser.add_argument(
    #     "--output_filename",
    #     type=_validate_csv_filename,
    #     default="listings.csv",
    #     help="Output file name (must end with .csv)",
    # )
    parser.add_argument(
        "--max_attempts_for_network_error",
        type=int,
        default=5,
        help="Maximum number of attempts to retry on network errors (default: 5)",
    )
    parser.add_argument(
        "--max_attempts_for_other_error",
        type=int,
        default=3,
        help="Maximum number of attempts to retry on other errors (default: 3)",
    )
    parser.add_argument(
        "--single_browser_run_timeout_seconds",
        type=int,
        default=5 * 60,
        help="Overall timeout for loading each listing, in seconds (default: 5 * 60)",
    )
    parser.add_argument(
        "--delay_per_listing_load",
        type=int,
        default=0,
        help="Delay before loading each listing page, in seconds (default: 0)",
    )
    parser.add_argument(
        "--delay_per_listing_first_second_retry_load",
        type=int,
        default=30,
        help="Delay before loading each listing page for the first and second retries, in seconds (default: 30)",
    )
    parser.add_argument(
        "--delay_per_listing_subsequent_retries_load",
        type=int,
        default=60,
        help="Delay before loading each listing page for all subsequent retries, in seconds (default: 60)",
    )
    parser.add_argument(
        "--max_number_of_pages",
        type=int,
        help="Maximum number of pages of listings to scrape, with each page containing 20 listings (default: unlimited)",
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()

    global OUTPUT_FILENAME
    OUTPUT_FILENAME = args.output_filename

    global MAX_ATTEMPTS_FOR_NETWORK_ERROR
    MAX_ATTEMPTS_FOR_NETWORK_ERROR = args.max_attempts_for_network_error

    global MAX_ATTEMPTS_FOR_OTHER_ERROR
    MAX_ATTEMPTS_FOR_OTHER_ERROR = args.max_attempts_for_other_error

    global SINGLE_BROWSER_RUN_TIMEOUT_SECONDS
    SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = args.single_browser_run_timeout_seconds

    global DELAY_PER_LISTING_LOAD_SECONDS
    DELAY_PER_LISTING_LOAD_SECONDS = args.delay_per_listing_load

    global DELAY_PER_LISTING_FIRST_SECOND_RETRY_LOAD_SECONDS
    DELAY_PER_LISTING_FIRST_SECOND_RETRY_LOAD_SECONDS = (
        args.delay_per_listing_first_second_retry_load
    )

    global DELAY_PER_LISTING_SUBSEQUENT_RETRIES_LOAD_SECONDS
    DELAY_PER_LISTING_SUBSEQUENT_RETRIES_LOAD_SECONDS = (
        args.delay_per_listing_subsequent_retries_load
    )

    global MAX_NUMBER_OF_PAGES
    MAX_NUMBER_OF_PAGES = args.max_number_of_pages

    logger.setLevel(args.log_level)

    gmaps = file_util.get_gmaps_client()
    # asyncio.run(_main_scrape_all(gmaps=gmaps, mrt_station_map=mrt_station_map))


if __name__ == "__main__":
    logger = logging.getLogger("HDB Scraper")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()
