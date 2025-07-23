# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import logging
import bs4
import browser_util
import file_util

HDB_URL_MAIN = "https://homes.hdb.gov.sg/home/finding-a-flat"
HDB_URL_PREFIX = "https://homes.hdb.gov.sg"
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
logger = logging.getLogger("HDB Scraper: HDB Listing Pages")


async def _get_listing_urls(browser):
    logger.info(f"Starting to get all listing URLs from {HDB_URL_MAIN}")

    logger.debug(f"Getting paged HTMLs from {HDB_URL_MAIN}")
    htmls = await browser.run_with_browser_page_for_url(
        url=HDB_URL_MAIN,
        callback_on_page=_get_paged_rendered_html_browser_page_callback(
            initial_action=_click_resale_listings_button,
            pagination_action=_click_next_page_button,
        ),
        debug_logging_name=HDB_URL_MAIN,
    )
    htmls = [] if htmls is None else htmls
    logger.debug(f"Got {len(htmls)} paged HTMLs from {HDB_URL_MAIN}")

    all_listing_urls = set()
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
            HDB_URL_PREFIX + listing_url if listing_url.startswith("/") else listing_url
            for listing_url in listing_urls
        ]
        logger.info(
            f"Found {len(listing_urls)} listing URLs from page {page_index+1} of {HDB_URL_MAIN}"
        )
        all_listing_urls = all_listing_urls | set(listing_urls)

    return all_listing_urls


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
    await page.waitFor(3000)
    await page.waitForSelector(".listing-card")

    return True


def _get_paged_rendered_html_browser_page_callback(
    initial_action=None, pagination_action=None
):
    async def _callback(page, debug_logging_name):
        htmls = []

        if initial_action is not None:
            await initial_action(page=page, debug_logging_name=debug_logging_name)

        logger.info(f"Extracting rendered HTML from {debug_logging_name} (page 1)")
        html = await page.content()
        logger.debug(
            f"Successfully extracted rendered HTML from {debug_logging_name} (page 1)"
        )
        htmls.append(html)

        if pagination_action is not None:
            page_num = 1

            while True:
                was_pagination_successful = await pagination_action(
                    page=page, debug_logging_name=debug_logging_name
                )
                if not was_pagination_successful:
                    logger.info(
                        f"No more pages from {debug_logging_name} ({page_num} pages total)"
                    )
                    break
                page_num += 1

                logger.info(
                    f"Extracting rendered HTML from {debug_logging_name} (page {page_num})"
                )
                html = await page.content()
                logger.debug(
                    f"Successfully extracted rendered HTML from {debug_logging_name} (page {page_num})"
                )
                htmls.append(html)

        return htmls

    return _callback


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
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
    )
    asyncio.run(_get_listing_urls(browser=browser))


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()
