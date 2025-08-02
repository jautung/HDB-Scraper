# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import dataclasses
import logging
import os
import typing
import bs4
import browser_util
import file_util
import hdb_parsing_util

SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
DELAY_PER_LISTING_LOAD_SECONDS = 1
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
logger = logging.getLogger(__name__)


async def _scrape_listings():
    logger.info(f"Starting to scrape all listings from {file_util.LISTINGS_FILENAME}")

    output_file_exists, already_processed_urls = _get_already_processed_urls()
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
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
            _write_base_info_headers(base_info_writer=base_info_writer)

        listing_urls = list(listings_reader)
        num_listings = len(listing_urls)
        num_written = 0
        for listing_index, listing_row in enumerate(listing_urls):
            assert len(listing_row) == 1
            listing_url = listing_row[0]
            debug_logging_name = (
                f"{listing_url} (listing #{listing_index+1} of {num_listings})"
            )

            if listing_url in already_processed_urls:
                logger.info(
                    f"Skipping {debug_logging_name} because it is already processed"
                )
                continue

            listing_info = await _scrape_single_listing(
                listing_url=listing_url,
                debug_logging_name=debug_logging_name,
                browser=browser,
            )
            if listing_info is None:
                logger.warning(
                    f"Unable to scrape {debug_logging_name}, skipping writing to {file_util.BASE_INFO_FILENAME}"
                )
                continue

            _write_base_info_row(
                base_info_writer=base_info_writer, listing_info=listing_info
            )
            num_written += 1
            base_info_file.flush()
            os.fsync(base_info_file.fileno())

            # Artificially make this slower so HDB doesn't block us... :)
            await asyncio.sleep(DELAY_PER_LISTING_LOAD_SECONDS)

        logger.info(
            f"Successfully exported {num_written} scraped results to {file_util.BASE_INFO_FILENAME}"
        )

    await browser.maybe_close_browser()


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


@dataclasses.dataclass
class ListingInfo:
    listing_url: typing.Any
    header_info: typing.Any
    details_info: typing.Any


async def _scrape_single_listing(listing_url, debug_logging_name, browser):
    logger.info(f"Starting to scrape {debug_logging_name}")

    logger.debug(f"Getting rendered HTML of {debug_logging_name}")
    html = await browser.run_with_browser_page_for_url(
        url=listing_url,
        callback_on_page=_get_single_rendered_html_browser_page_callback(
            # N/B: any 'h3' tag is a simple heuristic to determine that the Angular-rendered web page has loaded
            selector_to_wait_for="h3",
            additional_action=_click_expand_all_button,
        ),
        debug_logging_name=debug_logging_name,
    )
    if html is None:
        return None

    logger.debug(f"Parsing HTML of {debug_logging_name}")
    html_soup = bs4.BeautifulSoup(html, "html.parser")

    header_info = hdb_parsing_util.parse_header_info(html_soup=html_soup)
    details_info = hdb_parsing_util.parse_details_info(html_soup=html_soup)

    logger.info(f"Finished scraping {debug_logging_name}")
    return ListingInfo(
        listing_url=listing_url,
        header_info=header_info,
        details_info=details_info,
    )


def _get_single_rendered_html_browser_page_callback(
    selector_to_wait_for=None, additional_action=None
):
    async def _callback(page, debug_logging_name):
        if selector_to_wait_for is not None:
            logger.debug(
                f"Waiting for selector {selector_to_wait_for} of {debug_logging_name}"
            )
            await page.waitForSelector(selector_to_wait_for)

        if additional_action is not None:
            await additional_action(page=page, debug_logging_name=debug_logging_name)

        logger.debug(f"Extracting rendered HTML from {debug_logging_name}")
        html = await page.content()
        logger.debug(f"Successfully extracted rendered HTML from {debug_logging_name}")
        return html

    return _callback


async def _click_expand_all_button(page, debug_logging_name):
    logger.debug(f"Waiting for 'Expand/Collapse all' button of {debug_logging_name}")
    await page.waitForSelector(".btn-secondary")
    logger.debug(f"Clicking on 'Expand/Collapse all' button of {debug_logging_name}")
    await page.click(".btn-secondary")


def _write_base_info_headers(base_info_writer):
    base_info_writer.writerow(
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
            # Mostly irrelevant info (for us)
            "Will seller want to extend their stay (up to 3 months)? [less relevant for us]",
            "Enhanced Contra Facility (ECF) Allowed? [irrelevant for us]",
            "SPR eligibility [irrelevant for us]",
        ]
    )


def _write_base_info_row(base_info_writer, listing_info):
    base_info_writer.writerow(
        [
            # Key info
            listing_info.listing_url,
            listing_info.header_info.address,
            listing_info.header_info.postal_code,
            listing_info.header_info.hdb_type,
            listing_info.details_info.ethnic_eligibility,
            listing_info.header_info.area,
            listing_info.header_info.price,
            listing_info.details_info.storey_range,
            listing_info.details_info.remaining_lease_num_years,
            listing_info.details_info.last_updated_date,
            listing_info.details_info.description,
            # Useful info
            listing_info.details_info.num_bedrooms,
            listing_info.details_info.num_bathrooms,
            listing_info.details_info.balcony,
            listing_info.details_info.upgrading,
            # Fallback scraped info
            listing_info.header_info.sub_address,
            listing_info.details_info.town,
            listing_info.details_info.remaining_lease,
            listing_info.details_info.last_updated,
            # Mostly irrelevant info (for us)
            listing_info.details_info.extension_of_stay,
            listing_info.details_info.contra,
            listing_info.details_info.spr_eligibility,
        ]
    )


def main():
    parser = argparse.ArgumentParser(description="HDB Base Scraper")
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

    file_util.maybe_create_output_folder()
    asyncio.run(_scrape_listings())


if __name__ == "__main__":
    main()
