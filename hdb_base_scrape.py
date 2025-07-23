# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import logging
import os
import bs4
import browser_util
import file_util

SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
logger = logging.getLogger("HDB Scraper: HDB Base Scrape")


async def _scrape_listings():
    logger.info(f"Starting to scrape all listings from {file_util.LISTINGS_FILENAME}")

    output_file_exists, already_processed_urls = _get_already_processed_urls()
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
    )
    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.LISTINGS_FILENAME),
        newline="",
        encoding="utf-8",
    ) as listings_file, open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.BASE_INFO_FILENAME),
        "a",
        newline="",
        encoding="utf-8",
    ) as base_info_file:
        listings_reader = csv.reader(listings_file)
        base_info_writer = csv.writer(base_info_file)

        if not output_file_exists:
            # Write header only for a fresh file
            base_info_writer.writerow(["Link", "Address"])

        listing_urls = [row for row in listings_reader]
        for listing_index, listing_row in enumerate(listing_urls):
            assert len(listing_row) == 1
            listing_url = listing_row[0]
            logger.debug(
                f"Maybe scraping information from {listing_url} ({listing_index+1} of {len(listing_urls)})"
            )

            if listing_url in already_processed_urls:
                logger.debug(f"Skipping {listing_url} because it is already processed")
                continue

            result = await _scrape_single_listing(
                listing_url=listing_url, browser=browser
            )
            if result is None:
                logger.warning(
                    f"Unable to scrape {listing_url}, skipping writing to {file_util.BASE_INFO_FILENAME}"
                )
                continue

            base_info_writer.writerow([listing_url, result])
            base_info_file.flush()
            os.fsync(base_info_file.fileno())

    await browser.maybe_close_browser()


async def _scrape_single_listing(listing_url, browser):
    logger.debug(f"Scraping {listing_url}")
    return "hello"


def _get_already_processed_urls():
    logger.debug(
        f"Getting already-processed listings from {file_util.BASE_INFO_FILENAME}"
    )
    if not os.path.exists(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.BASE_INFO_FILENAME)
    ):
        logger.debug(f"{file_util.BASE_INFO_FILENAME} does not exist yet!")
        return False, {}
    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.BASE_INFO_FILENAME),
        newline="",
        encoding="utf-8",
    ) as base_info_file:
        dict_reader = csv.DictReader(base_info_file)
        already_processed_urls = set(row["Link"] for row in dict_reader)
        logger.info(f"{len(already_processed_urls)} already-processed listings found!")
        return True, already_processed_urls


def main():
    parser = argparse.ArgumentParser(description="MRT Precompute")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logger.setLevel(args.log_level)

    file_util.maybe_create_output_folder()
    asyncio.run(_scrape_listings())


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()
