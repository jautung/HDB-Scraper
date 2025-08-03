# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import logging
import os
import bs4
import browser_util
import file_util

HDB_URL_MAIN = "https://homes.hdb.gov.sg/home/finding-a-flat"
HDB_URL_PREFIX = "https://homes.hdb.gov.sg"
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
NEXT_PAGE_WAIT_TIME_SECONDS = 3
logger = logging.getLogger(__name__)


async def _get_listing_urls():
    logger.info(f"Starting to get all listing URLs from {HDB_URL_MAIN}")

    logger.debug(f"Getting paged HTMLs from {HDB_URL_MAIN}")
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
    )
    htmls = await browser.run_with_browser_page_for_url(
        url=HDB_URL_MAIN,
        callback_on_page=browser_util.get_paged_rendered_html_browser_page_callback(
            initial_action=_click_resale_listings_button,
            pagination_action=_click_next_page_button,
        ),
        debug_logging_name=HDB_URL_MAIN,
    )
    await browser.maybe_close_browser()
    htmls = [] if htmls is None else htmls
    logger.debug(f"Got {len(htmls)} paged HTMLs from {HDB_URL_MAIN}")

    with open(
        os.path.join(file_util.OUTPUT_FOLDER, file_util.LISTINGS_FILENAME),
        "w",
        newline="",
        encoding="utf-8",
    ) as csvfile:
        writer = csv.writer(csvfile)
        for page_index, html in enumerate(htmls):
            logger.debug(
                f"Parsing HTML page {page_index+1} of {len(htmls)} from {HDB_URL_MAIN}"
            )
            html_soup = bs4.BeautifulSoup(html, "html.parser")

            listing_urls = [
                listing_link["href"]
                for listing_link in html_soup.find_all("a", class_="flat-link")
            ]
            listing_urls = [
                # Many URLs are just encoded as '/home/resale/xxx'
                HDB_URL_PREFIX + listing_url
                if listing_url.startswith("/")
                else listing_url
                for listing_url in listing_urls
            ]

            logger.info(
                f"Found {len(listing_urls)} listing URLs from page {page_index+1} of {HDB_URL_MAIN}"
            )
            writer.writerows([[listing_url] for listing_url in listing_urls])


async def _click_resale_listings_button(page, debug_logging_name):
    # N/B: any 'h1' tag is a simple heuristic to determine that the Angular-rendered web page has loaded
    logger.debug(f"Waiting page to load of {debug_logging_name}")
    await page.waitForSelector("h1")

    logger.debug(f"Finding 'resale listings' button of {debug_logging_name}")
    links = await page.querySelectorAll("a.flat-link")
    links_for_resale_listings = [
        link
        for link in links
        # Check for nested child element of the link to indicate 'resale'
        if await link.querySelector(".tag-resale")
    ]
    assert len(links_for_resale_listings) == 1
    link_for_resale_listings = links_for_resale_listings[0]

    logger.debug(f"Clicking 'resale listings' button of {debug_logging_name}")
    await link_for_resale_listings.click()
    logger.debug(
        f"Waiting for page to reload with resale listings of {debug_logging_name}"
    )
    await page.waitForSelector(".listing-card")


async def _click_next_page_button(page, debug_logging_name):
    logger.debug(f"Waiting for 'next page' button of {debug_logging_name}")
    await page.waitForSelector('[aria-label="Next"]')
    next_page_button = await page.querySelector('[aria-label="Next"]')
    assert next_page_button is not None

    is_next_page_button_disabled = (
        await page.evaluate(
            'button => button.getAttribute("aria-disabled")', next_page_button
        )
        == "true"
    )
    if is_next_page_button_disabled:
        logger.debug(f"Found disabled 'next page' button of {debug_logging_name}")
        return False

    logger.debug(f"Clicking on 'next page' button of {debug_logging_name}")
    await next_page_button.click()
    logger.debug(f"Waiting for 'next page' to reload of {debug_logging_name}")
    await page.waitFor(NEXT_PAGE_WAIT_TIME_SECONDS * 1000)
    await page.waitForSelector(".listing-card")

    return True


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
