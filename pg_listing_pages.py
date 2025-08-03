# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import logging
import os
import re
import sys
import bs4
import browser_util
import file_util

PROPERTY_GURU_5_ROOM_URL = "https://www.propertyguru.com.sg/hdb-5-room-flat-for-sale"
PROPERTY_GURU_4_ROOM_URL = "https://www.propertyguru.com.sg/hdb-4-room-flat-for-sale"
PROPERTY_GURU_3_ROOM_URL = "https://www.propertyguru.com.sg/hdb-3-room-flat-for-sale"
PROPERTY_GURU_URLS = [
    PROPERTY_GURU_5_ROOM_URL,
    PROPERTY_GURU_4_ROOM_URL,
    PROPERTY_GURU_3_ROOM_URL,
]
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
LISTING_PATTERN = r"^https://www\.propertyguru\.com\.sg/listing/(?:.*-)?(\d+)$"
logger = logging.getLogger(__name__)


async def _get_listing_urls():
    for base_page in PROPERTY_GURU_URLS:
        await _get_listing_urls_from_base_page(base_page=base_page)


async def _get_listing_urls_from_base_page(base_page):
    logger.info(f"Starting to get all listing URLs from {base_page}")
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
        user_agent=browser_util.FAKE_USER_AGENT,
    )

    num_pages = await _get_num_pages(browser=browser, base_page=base_page)
    logger.info(f"Number of pages is {num_pages}")

    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.PG_LISTINGS_FILENAME),
        "w",
        newline="",
        encoding="utf-8",
    ) as csvfile:
        writer = csv.writer(csvfile)
        for page_num in range(1, num_pages + 1):
            logger.info(f"Getting listing URLs from page {page_num}/{num_pages}")
            listing_urls = await _get_listing_urls_from_page(
                browser=browser, page_url=f"{base_page}/{page_num}"
            )
            writer.writerows([[listing_url] for listing_url in listing_urls])

    await browser.maybe_close_browser()


async def _get_num_pages(browser, base_page):
    logger.info(
        f"Reading the first main page {base_page} to determine the number of pages"
    )
    html = await browser.run_with_browser_page_for_url(
        url=base_page,
        callback_on_page=browser_util.get_single_rendered_html_browser_page_callback(),
        debug_logging_name=base_page,
        wait_until="domcontentloaded",
    )

    logger.debug(f"Parsing HTML of the first main page of {base_page}")
    html_soup = bs4.BeautifulSoup(html, "html.parser")
    pagination_bar = html_soup.find("ul", {"class": "hui-pagination"})
    pagination_links = pagination_bar.find_all("a", class_="page-link")

    logger.debug(f"Found {len(pagination_links)} pagination links")
    page_nums = [
        _parse_and_get_page_num(pagination_link=pagination_link)
        for pagination_link in pagination_links
    ]
    return max(n for n in page_nums if n is not None)


def _parse_and_get_page_num(pagination_link):
    try:
        return int(pagination_link.get_text(strip=True))
    except ValueError:
        return None


async def _get_listing_urls_from_page(browser, page_url):
    logger.debug(f"Getting HTML from {page_url}")
    html = await browser.run_with_browser_page_for_url(
        url=page_url,
        callback_on_page=browser_util.get_single_rendered_html_browser_page_callback(),
        debug_logging_name=page_url,
        wait_until="domcontentloaded",
    )

    logger.debug(f"Parsing HTML of {page_url}")
    html_soup = bs4.BeautifulSoup(html, "html.parser")
    listing_urls = [
        _parse_and_normalize_listing(link=listing_link["href"])
        for listing_link in html_soup.find_all("a", class_="listing-card-link")
    ]

    logger.info(f"Found {len(listing_urls)} listing URLs from {page_url}")
    return listing_urls


def _parse_and_normalize_listing(link):
    match = re.search(LISTING_PATTERN, link)
    if match is None:
        logger.error(
            f"Link {link} did not match the listing pattern; this should not be possible; exiting!"
        )
        sys.exit(1)
    assert match is not None
    return f"https://www.propertyguru.com.sg/listing/{match.group(1)}"


def main():
    parser = argparse.ArgumentParser(description="HDB Listing Pages")
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
    asyncio.run(_get_listing_urls())


if __name__ == "__main__":
    main()
