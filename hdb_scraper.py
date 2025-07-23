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

OUTPUT_FILENAME = "listings.csv"  # Default
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5  # Default
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3  # Default
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60  # Default
DELAY_PER_LISTING_LOAD_SECONDS = 0  # Default
DELAY_PER_LISTING_FIRST_SECOND_RETRY_LOAD_SECONDS = 30  # Default
DELAY_PER_LISTING_SUBSEQUENT_RETRIES_LOAD_SECONDS = 60  # Default
MAX_NUMBER_OF_PAGES = None  # Default

################################################################
# GOOGLE MAPS START
################################################################


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


################################################################
# GOOGLE MAPS END
################################################################

################################################################
# EXPORT START
################################################################


def _export_to_csv(listings):
    num_written = 0
    with open(OUTPUT_FILENAME, "w+", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
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
        for listing in listings:
            if listing is None or isinstance(listing, Exception):
                logger.warning(f"Skipping a scraped listing {listing}")
                continue
            writer.writerow(
                [
                    # Key info
                    listing.listing_url,
                    listing.header_info.address,
                    listing.header_info.postal_code,
                    listing.header_info.hdb_type,
                    listing.details_info.ethnic_eligibility,
                    listing.header_info.area,
                    listing.header_info.price,
                    listing.details_info.storey_range,
                    listing.details_info.remaining_lease_num_years,
                    listing.nearest_mrt_info.nearest_mrt_station
                    if listing.nearest_mrt_info is not None
                    else None,
                    listing.nearest_mrt_info.walking_duration_mins
                    if listing.nearest_mrt_info is not None
                    else None,
                    listing.details_info.last_updated_date,
                    listing.details_info.description,
                    # Useful info
                    listing.details_info.num_bedrooms,
                    listing.details_info.num_bathrooms,
                    listing.details_info.balcony,
                    listing.details_info.upgrading,
                    # Fallback scraped info
                    listing.header_info.sub_address,
                    listing.details_info.town,
                    listing.details_info.remaining_lease,
                    listing.details_info.last_updated,
                    listing.nearest_mrt_info.straight_line_distance_km
                    if listing.nearest_mrt_info is not None
                    else None,
                    listing.nearest_mrt_info.walking_distance_km
                    if listing.nearest_mrt_info is not None
                    else None,
                    # Mostly irrelevant info (for us)
                    listing.details_info.extension_of_stay,
                    listing.details_info.contra,
                    listing.details_info.spr_eligibility,
                ]
            )
            num_written += 1

    logger.info(
        f"Successfully exported {num_written} scraped results to {OUTPUT_FILENAME}"
    )


################################################################
# EXPORT END
################################################################

################################################################
# MAIN START
################################################################


# _augment_listings_with_mrt_info(
#     listings=scraped_listings, gmaps=gmaps, mrt_station_map=mrt_station_map
# )
# _export_to_csv(listings=scraped_listings)


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

################################################################
# MAIN END
################################################################
