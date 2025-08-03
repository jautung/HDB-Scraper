# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import logging
import re
import sys
import bs4
import browser_util
import file_util

PROPERTY_GURU_URL = "https://www.propertyguru.com.sg/property-for-sale"
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
LISTING_PATTERN = r"^https://www\.propertyguru\.com\.sg/listing/(?:.*-)?(\d+)$"
logger = logging.getLogger(__name__)


async def _get_listing_urls():
    logger.info(f"Starting to get all listing URLs from {PROPERTY_GURU_URL}")
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
        user_agent=browser_util.FAKE_USER_AGENT,
    )

    num_pages = await _get_num_pages(browser=browser)
    logger.info(f"Number of pages is {num_pages}")

    listing_urls = []
    for page_num in range(1, min(3, num_pages + 1)):  # TODO
        logger.info(f"Getting listing URLs from page {page_num}/{num_pages}")
        listing_urls.append(
            await _get_listing_urls_from_page(
                browser=browser, page_url=f"{PROPERTY_GURU_URL}/{page_num}"
            )
        )

    await browser.maybe_close_browser()
    return listing_urls


async def _get_num_pages(browser):
    logger.info(
        f"Reading the first main page {PROPERTY_GURU_URL} to determine the number of pages"
    )
    html = await browser.run_with_browser_page_for_url(
        url=PROPERTY_GURU_URL,
        callback_on_page=browser_util.get_single_rendered_html_browser_page_callback(),
        debug_logging_name=PROPERTY_GURU_URL,
        wait_until="domcontentloaded",
    )

    logger.debug(f"Parsing HTML of the first main page of {PROPERTY_GURU_URL}")
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
