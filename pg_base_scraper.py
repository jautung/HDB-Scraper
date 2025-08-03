# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import logging
import bs4
import browser_util
import file_util
import pg_parsing_util

SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
DELAY_PER_LISTING_LOAD_SECONDS = 1
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
MAX_ATTEMPTS_FOR_CLOUDFLARE_WAIT = 5
MRT_DISTANCE_PATTERN = r"^([\d.]+) (m|km) \((\d+) mins\) from ([A-Z]+\d+) .+$"
logger = logging.getLogger(__name__)


async def _scrape_listings():
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
        user_agent=browser_util.FAKE_USER_AGENT,
    )
    # TODO Actually write this properly (read/write from/to files)
    print(
        await _scrape_single_listing(
            listing_url="https://www.propertyguru.com.sg/listing/60034673",
            browser=browser,
        )
    )
    print(
        await _scrape_single_listing(
            listing_url="https://www.propertyguru.com.sg/listing/25539219",
            browser=browser,
        )
    )
    print(
        await _scrape_single_listing(
            listing_url="https://www.propertyguru.com.sg/listing/25559652",
            browser=browser,
        )
    )
    await browser.maybe_close_browser()


async def _scrape_single_listing(listing_url, browser, current_attempt=1):
    logger.info(f"Starting to scrape {listing_url} (attempt {current_attempt})")

    logger.debug(f"Getting rendered HTML of {listing_url}")
    html = await browser.run_with_browser_page_for_url(
        url=listing_url,
        callback_on_page=browser_util.get_single_rendered_html_browser_page_callback(),
        debug_logging_name=listing_url,
        wait_until="domcontentloaded",
    )
    if html is None:
        return None

    logger.debug(f"Parsing HTML of {listing_url}")
    html_soup = bs4.BeautifulSoup(html, "html.parser")

    # Somewhat helpfully, this element already contains all the semantic data that is used
    # to populate the UI of the website, in a huge JSON blob.
    # Unclear if this is intended/secure, but I'll take it! :)
    script_data_element = html_soup.find(
        "script", {"id": "__NEXT_DATA__", "type": "application/json"}
    )
    if script_data_element is None:
        logger.info(f"Script data tag not found for {listing_url}")
        if "Just a moment..." in html:
            if current_attempt < MAX_ATTEMPTS_FOR_CLOUDFLARE_WAIT:
                return await _scrape_single_listing(
                    listing_url=listing_url,
                    browser=browser,
                    current_attempt=current_attempt + 1,
                )
            logger.error(
                f"Hit 'Just a moment...' Cloudflare page after {MAX_ATTEMPTS_FOR_CLOUDFLARE_WAIT} attempts; giving up on {listing_url}!"
            )
            return None
        logger.error(f"Giving up on {listing_url}!")
        return None

    listing_info = pg_parsing_util.parse_script_data_element(
        script_data_element=script_data_element, listing_url=listing_url
    )
    logger.info(f"Finished scraping {listing_url}")
    return listing_info


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
