# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import csv
import dataclasses
import logging
import os
import typing
import file_util
import gmaps_util

logger = logging.getLogger(__name__)


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

        # Sorting allows us to 'cache' the results of postal code lookups,
        # without the overhead of keeping an entire dictionary
        # of postal code -> nearest MRT info in memory
        listing_dicts = sorted(
            list(base_info_dict_reader), key=lambda row: row["Postal code"]
        )
        num_listings = len(listing_dicts)
        num_written = 0
        prev_postal_code = None
        prev_postal_code_nearest_mrt_info = None
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

            assert "Postal code" in listing_dict and listing_dict["Postal code"]
            postal_code = listing_dict["Postal code"]
            if prev_postal_code is not None and postal_code == prev_postal_code:
                logger.info(
                    f"Cache hit for nearest MRT info for 'S{postal_code}' for {debug_logging_name}"
                )
                nearest_mrt_info = prev_postal_code_nearest_mrt_info
            else:
                nearest_mrt_info = _get_nearest_mrt_info(
                    postal_code=postal_code,
                    debug_logging_name=debug_logging_name,
                    gmaps=gmaps,
                    mrt_station_map=mrt_station_map,
                )

            if nearest_mrt_info is None:
                logger.warning(
                    f"Skipping {debug_logging_name} because we could not obtain nearest MRT info"
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

            prev_postal_code = postal_code
            prev_postal_code_nearest_mrt_info = nearest_mrt_info

        logger.info(
            f"Successfully exported {num_written} scraped results to {file_util.FULL_RESULTS_FILENAME}"
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


def _get_nearest_mrt_info(postal_code, debug_logging_name, gmaps, mrt_station_map):
    logger.info(
        f"Finding nearest MRT info for 'S{postal_code}' for {debug_logging_name}"
    )

    postal_code_address = f"{postal_code}, Singapore"
    postal_code_lat, postal_code_lon = gmaps_util.get_lat_lon_from_address(
        gmaps=gmaps, address=postal_code_address
    )

    mrt_station_distances_km = [
        (
            mrt_station,
            gmaps_util.haversine_distance_km(
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

    distance_metres, duration_seconds = gmaps_util.get_walking_distance_and_duration(
        gmaps=gmaps,
        start=postal_code_address,
        end=nearest_mrt_station,
    )
    if distance_metres is not None and duration_seconds is not None:
        logger.debug(
            f"Google Maps says that 'S{postal_code}' to {nearest_mrt_station} takes {(duration_seconds / 60):.2f}mins"
        )
    else:
        logger.warning(
            f"Google Maps unable to find walking distance from 'S{postal_code}' to {nearest_mrt_station}"
        )
        return None

    return NearestMRTInfo(
        nearest_mrt_station=nearest_mrt_station,
        straight_line_distance_km=nearest_mrt_station_distance_km,
        walking_distance_km=distance_metres / 1000,
        walking_duration_mins=duration_seconds / 60,
    )


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
    full_results_writer.writerow(
        [
            # Key info
            listing_dict["Link"],
            listing_dict["Address"],
            listing_dict["Postal code"],
            listing_dict["HDB type"],
            listing_dict["Ethnic eligibility"],
            listing_dict["Area (sqm)"],
            listing_dict["Price ($)"],
            listing_dict["Storey range"],
            listing_dict["Remaining lease (years)"],
            nearest_mrt_info.nearest_mrt_station,
            nearest_mrt_info.walking_duration_mins,
            listing_dict["Last updated date"],
            listing_dict["Free-form description (provided by seller)"],
            # Useful info
            listing_dict["Number of bedrooms"],
            listing_dict["Number of bathrooms"],
            listing_dict["Balcony"],
            listing_dict["Upcoming upgrading plans?"],
            # Fallback scraped info
            listing_dict["Sub-address [fallback if postal code is 'None']"],
            listing_dict["Town [fallback if nearest MRT station is 'None']"],
            listing_dict[
                "Remaining lease [fallback if parsed remaining lease is 'None']"
            ],
            listing_dict["Last updated [fallback if last updated date is 'None']"],
            nearest_mrt_info.straight_line_distance_km,
            nearest_mrt_info.walking_distance_km,
            # Mostly irrelevant info (for us)
            listing_dict[
                "Will seller want to extend their stay (up to 3 months)? [less relevant for us]"
            ],
            listing_dict["Enhanced Contra Facility (ECF) Allowed? [irrelevant for us]"],
            listing_dict["SPR eligibility [irrelevant for us]"],
        ]
    )


def main():
    parser = argparse.ArgumentParser(description="HDB Scraper")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s (%(name)s) [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    _full_results_with_mrt_info()


if __name__ == "__main__":
    main()
