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
logger = logging.getLogger(__name__)


async def _get_listing_urls():
    logger.info(f"Starting to get all listing URLs from {PROPERTY_GURU_URL}")

    logger.debug(f"Getting paged HTMLs from {PROPERTY_GURU_URL}")
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
        user_agent=browser_util.FAKE_USER_AGENT,
    )
    htmls = await browser.run_with_browser_page_for_url(
        url=PROPERTY_GURU_URL,
        callback_on_page=browser_util.get_paged_rendered_html_browser_page_callback(
            initial_action=None,
            pagination_action=_click_next_page_button,
        ),
        debug_logging_name=PROPERTY_GURU_URL,
        wait_until="domcontentloaded",
    )
    await browser.maybe_close_browser()
    htmls = [] if htmls is None else htmls
    logger.debug(f"Got {len(htmls)} paged HTMLs from {PROPERTY_GURU_URL}")

    for page_index, html in enumerate(htmls):
        logger.debug(
            f"Parsing HTML page {page_index+1} of {len(htmls)} from {PROPERTY_GURU_URL}"
        )
        html_soup = bs4.BeautifulSoup(html, "html.parser")

        listing_urls = [
            _parse_and_normalize_listing(listing_link["href"])
            for listing_link in html_soup.find_all("a", class_="listing-card-link")
        ]
        # TODO
        print(listing_urls)

        logger.info(
            f"Found {len(listing_urls)} listing URLs from page {page_index+1} of {PROPERTY_GURU_URL}"
        )


async def _click_next_page_button(page, debug_logging_name):
    # TODO
    # Actually probably change this to check for largest page number and just iterate instead
    return False


LISTING_PATTERN = r"^https://www\.propertyguru\.com\.sg/listing/(?:.*-)?(\d+)$"


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
