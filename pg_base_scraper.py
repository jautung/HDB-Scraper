# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import datetime
import logging
import os
import re
import bs4
import browser_util
import file_util
import pg_parsing_util

SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
NAVIGATION_TIMEOUT_SECONDS = 1
DELAY_PER_LISTING_LOAD_SECONDS = 1
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
MAX_ATTEMPTS_FOR_CLOUDFLARE_WAIT = 5
MRT_DISTANCE_PATTERN = r"^([\d.]+) (m|km) \((\d+) mins\) from ([A-Z]+\d+) .+$"
LISTING_PATTERN = r"^https://www\.propertyguru\.com\.sg/listing/(?:.*-)?(\d+)$"
logger = logging.getLogger(__name__)


async def _scrape_listings():
    logger.info(
        f"Starting to scrape all listings from {file_util.PG_LISTINGS_FILENAME}"
    )

    output_file_exists, already_processed_urls = _get_already_processed_urls()
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
        user_agent=browser_util.FAKE_USER_AGENT,
    )

    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.PG_LISTINGS_FILENAME),
        newline="",
        encoding="utf-8",
    ) as listings_file, open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.PG_FULL_RESULTS_FILENAME),
        "a",
        newline="",
        encoding="utf-8",
    ) as full_results_file:
        listings_reader = csv.reader(listings_file)
        full_results_writer = csv.writer(full_results_file)

        if not output_file_exists:
            _write_full_results_headers(full_results_writer=full_results_writer)

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
                    f"Unable to scrape {debug_logging_name}, skipping writing to {file_util.PG_FULL_RESULTS_FILENAME}"
                )
                continue

            _write_full_results_row(
                full_results_writer=full_results_writer, listing_info=listing_info
            )
            num_written += 1
            full_results_file.flush()
            os.fsync(full_results_file.fileno())

            # Artificially make this slower so HDB doesn't block us... :)
            await asyncio.sleep(DELAY_PER_LISTING_LOAD_SECONDS)

        logger.info(
            f"Successfully exported {num_written} scraped results to {file_util.PG_FULL_RESULTS_FILENAME}"
        )

    await browser.maybe_close_browser()


def _get_already_processed_urls():
    logger.debug(
        f"Getting already-processed listings from {file_util.PG_FULL_RESULTS_FILENAME}"
    )
    if not os.path.exists(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.PG_FULL_RESULTS_FILENAME)
    ):
        logger.debug(f"{file_util.PG_FULL_RESULTS_FILENAME} does not exist yet!")
        return False, {}
    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.PG_FULL_RESULTS_FILENAME),
        newline="",
        encoding="utf-8",
    ) as full_results_file:
        dict_reader = csv.DictReader(full_results_file)
        already_processed_urls = set(row["Link"] for row in dict_reader)
        logger.info(f"{len(already_processed_urls)} already-processed listings found!")
        return True, already_processed_urls


async def _scrape_single_listing(
    listing_url, debug_logging_name, browser, current_attempt=1
):
    logger.info(f"Starting to scrape {debug_logging_name} (attempt {current_attempt})")

    logger.debug(f"Getting rendered HTML of {debug_logging_name}")
    html = await browser.run_with_browser_page_for_url(
        url=listing_url,
        callback_on_page=browser_util.get_single_rendered_html_browser_page_callback(),
        debug_logging_name=debug_logging_name,
        wait_until="domcontentloaded",
        wait_for_selector='h1[da-id="property-title"]',
        timeout=NAVIGATION_TIMEOUT_SECONDS * 1000,
        validate_after_navigate=_get_validate_after_navigate(
            listing_url=listing_url, debug_logging_name=debug_logging_name
        ),
    )
    if html is None:
        return None

    logger.debug(f"Parsing HTML of {debug_logging_name}")
    html_soup = bs4.BeautifulSoup(html, "html.parser")

    # Somewhat helpfully, this element already contains all the semantic data that is used
    # to populate the UI of the website, in a huge JSON blob.
    # Unclear if this is intended/secure, but I'll take it! :)
    script_data_element = html_soup.find(
        "script", {"id": "__NEXT_DATA__", "type": "application/json"}
    )
    if script_data_element is None:
        logger.info(f"Script data tag not found for {debug_logging_name}")
        if "Just a moment..." in html:
            if current_attempt < MAX_ATTEMPTS_FOR_CLOUDFLARE_WAIT:
                return await _scrape_single_listing(
                    listing_url=listing_url,
                    debug_logging_name=debug_logging_name,
                    browser=browser,
                    current_attempt=current_attempt + 1,
                )
            logger.error(
                f"Hit 'Just a moment...' Cloudflare page after {MAX_ATTEMPTS_FOR_CLOUDFLARE_WAIT} attempts; giving up on {listing_url}!"
            )
            return None
        logger.error(f"Giving up on {debug_logging_name}!")
        return None

    listing_info = pg_parsing_util.parse_script_data_element(
        script_data_element=script_data_element, listing_url=listing_url
    )
    logger.info(f"Finished scraping {debug_logging_name}")
    return listing_info


def _get_validate_after_navigate(listing_url, debug_logging_name):
    def validate_after_navigate(new_page_url, new_page_html):
        if new_page_url == "about:blank":
            logger.info(
                f"Skipping {debug_logging_name} because it redirected to about:blank"
            )
            return False

        normalized_listing_url = _parse_and_normalize_listing(listing_url)
        normalized_new_page_url = _parse_and_normalize_listing(new_page_url)
        if normalized_listing_url is None or normalized_new_page_url is None:
            logger.info(
                f"Skipping {debug_logging_name} because redirected URL {new_page_url} could not be parsed"
            )
            return False
        if normalized_listing_url != normalized_new_page_url:
            logger.info(
                f"Skipping {debug_logging_name} because redirected URL {new_page_url} is different from original"
            )
            return False

        if "Oops! Page not found" in new_page_html:
            logger.info(
                f"Skipping {debug_logging_name} because hit 'page not found' page"
            )
            return False

        return True

    return validate_after_navigate


def _parse_and_normalize_listing(link):
    match = re.search(LISTING_PATTERN, link)
    if match is None:
        return None
    return f"https://www.propertyguru.com.sg/listing/{match.group(1)}"


def _write_full_results_headers(full_results_writer):
    full_results_writer.writerow(
        [
            # Key info
            "Link",
            "Address",
            "Postal code",
            "HDB type",
            "Area (sqm)",
            "Price ($)",
            "Storey range",
            "Remaining lease (years)",
            "Nearest MRT station",
            "Walking duration to MRT (mins)",
            "Last updated date",
            "Free-form description title (provided by seller)",
            "Free-form description body (provided by seller)",
            # Useful info
            "Is verified listing?",
            "Is price negotiable?",
            "Number of bedrooms",
            "Number of bathrooms",
            "Furnished status",
            "Tenanted status",
            "Walking distance to MRT (km)",
            "Main image",
            # Mostly irrelevant info
            "Listing title",
            "Location latitude",
            "Location longitude",
            "Region",
            "District",
            "Estate",
            "Street",
            "Agent name",
            "Agent agency",
            "Agent profile link",
            "Amenities",
            "Images",
            "Floor plans",
            "FAQ info",
        ]
    )


def _write_full_results_row(full_results_writer, listing_info):
    full_results_writer.writerow(
        [
            # Key info
            listing_info.listing_url,
            listing_info.header_info.address,
            listing_info.header_info.postal_code,
            listing_info.header_info.hdb_type,
            listing_info.header_info.area_sqft / 10.764,
            listing_info.header_info.price,
            listing_info.details_info.floor_level,
            99 - (datetime.datetime.now().year - listing_info.details_info.top_year)
            if listing_info.details_info.top_year is not None
            else None,
            listing_info.details_info.nearest_mrt_name,
            listing_info.details_info.nearest_mrt_duration_seconds / 60
            if listing_info.details_info.nearest_mrt_duration_seconds is not None
            else None,
            listing_info.details_info.listed_date,
            listing_info.details_info.description_subtitle,
            listing_info.details_info.description_details,
            # Useful info
            listing_info.header_info.is_verified,
            listing_info.header_info.price_is_negotiable,
            listing_info.header_info.num_bedrooms,
            listing_info.header_info.num_bathrooms,
            listing_info.details_info.furnished_status,
            listing_info.details_info.tenanted_status,
            listing_info.details_info.nearest_mrt_distance_metres / 1000
            if listing_info.details_info.nearest_mrt_distance_metres is not None
            else None,
            listing_info.extra_info.main_image,
            # Mostly irrelevant info
            listing_info.header_info.title,
            listing_info.details_info.location_lat,
            listing_info.details_info.location_lon,
            listing_info.details_info.region,
            listing_info.details_info.district,
            listing_info.details_info.estate,
            listing_info.details_info.street,
            listing_info.extra_info.agent_name,
            listing_info.extra_info.agent_agency,
            listing_info.extra_info.agent_profile_url,
            ",".join(listing_info.extra_info.amenities),
            "|".join(listing_info.extra_info.all_images),
            "|".join(listing_info.extra_info.floor_plans),
            listing_info.extra_info.faq_info,
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
