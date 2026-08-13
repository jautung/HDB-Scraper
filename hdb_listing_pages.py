# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import logging
import os
import bs4
import browser_util
import file_util
import time
import os

HDB_URL_MAIN = "https://homes.hdb.gov.sg/home/finding-a-flat"
HDB_URL_PREFIX = "https://homes.hdb.gov.sg"
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
NEXT_PAGE_WAIT_TIME_SECONDS = 3
LISTING_DELAY_SECONDS = 0.5
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
        wait_until="networkidle2",
        wait_for_selector=".listing-card",
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

            # Newer pages use PCard anchors with obfuscated class names; match by
            # anchor tags that contain a PCard__content-container child and a
            # 'Resale' badge span.
            listing_urls = []
            for a in html_soup.find_all("a"):
                # must have an href
                href = a.get("href")
                if not href:
                    continue
                # look for inner PCard__content-container or a resale badge
                if a.find(class_="PCard__content-container") or a.find(
                    lambda tag: tag.name == "span"
                    and "resale" in (tag.get_text() or "").lower()
                ):
                    listing_urls.append(href)
            listing_urls = [
                # Many URLs are just encoded as '/home/resale/xxx'
                (
                    HDB_URL_PREFIX + listing_url
                    if listing_url.startswith("/")
                    else listing_url
                )
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

    # Dump initial page HTML for debugging
    try:
        os.makedirs("output", exist_ok=True)
        initial_html = await page.content()
        ts = int(time.time())
        with open(f"output/debug_initial_{ts}.html", "w", encoding="utf-8") as fh:
            fh.write(initial_html)
        logger.debug("Wrote initial page debug HTML: output/debug_initial_%s.html", ts)
    except Exception:
        logger.exception("Failed to write initial page debug HTML")
    logger.debug(f"Finding 'resale listings' button of {debug_logging_name}")

    # Try multiple strategies to find the 'Resale' control: older markup had
    # '.flat-link .tag-resale' inside an anchor; newer markup may use a
    # 'Resale' span inside a PCard__content-container. We attempt both.
    resale_handle = None

    # Strategy 1: legacy selector
    try:
        await page.waitForSelector(".flat-link .tag-resale", timeout=3000)
        resale_tag = await page.querySelector(".flat-link .tag-resale")
        if resale_tag is not None:
            resale_handle = await page.evaluateHandle(
                'element => element.closest("a.flat-link")', resale_tag
            )
    except Exception:
        logger.debug("Legacy '.flat-link .tag-resale' not found or timed out")

    # Strategy 2: find a span with text containing 'Resale' and click its closest anchor/card
    if resale_handle is None:
        logger.debug("Trying text-match strategy for 'Resale' badge")
        try:
            # Quick strategy: match obfuscated 'resaleBadge' classes that appear in the DOM
            try:
                await page.waitForSelector("span[class*='resaleBadge']", timeout=2000)
                badge = await page.querySelector("span[class*='resaleBadge']")
                if badge is not None:
                    resale_handle = await page.evaluateHandle(
                        'element => element.closest("a") || element.closest("[role="button"]") || element.closest(".PCard__content-container")',
                        badge,
                    )
            except Exception:
                logger.debug("No 'resaleBadge' class match")

            script = """
            () => {
                const spans = Array.from(document.querySelectorAll('span'));
                for (const s of spans) {
                    if (s.textContent && s.textContent.trim().toLowerCase().startsWith('resale')) {
                        // Prefer an ancestor anchor if present
                        const a = s.closest('a');
                        if (a) return a;
                        // Otherwise return the nearest clickable card
                        const card = s.closest('[role="button"]') || s.closest('.PCard__content-container') || s.closest('.card');
                        if (card) return card;
                    }
                }
                return null;
            }
            """
            resale_handle = await page.evaluateHandle(script)
        except Exception:
            logger.exception("Text-match strategy for 'Resale' failed")

    if resale_handle is None:
        logger.error("Could not find 'Resale' control on %s", debug_logging_name)
        raise AssertionError("Resale control not found")

    # Normalize JSHandles that reference `null` — evaluateHandle may return
    # a JSHandle object whose underlying value is `null`. Treat those as None
    # so the subsequent click logic doesn't attempt to call methods on null.
    try:
        if resale_handle is not None:
            try:
                is_null = await page.evaluate("(el) => el === null", resale_handle)
                if is_null:
                    resale_handle = None
            except Exception:
                # If evaluation fails, continue and let the existing checks handle it
                logger.debug("Could not determine if resale_handle is null")
    except Exception:
        # Defensive: ensure we don't break on unexpected failures here
        logger.debug("Error normalizing resale_handle")

    logger.debug(f"Clicking 'resale listings' button of {debug_logging_name}")
    # Robust click: `resale_handle` may be an ElementHandle or a JSHandle.
    try:
        element = None
        try:
            element = await resale_handle.asElement()
        except Exception:
            element = None

        if element is not None:
            await element.click()
        else:
            # Fallback: ask the page to click the referenced element via evaluate,
            # which works with JSHandle references returned from evaluateHandle.
            await page.evaluate("(el) => el.click()", resale_handle)
    except Exception:
        logger.exception("Failed to click resale control")
        raise

    logger.debug(
        f"Waiting for page to reload with resale listings of {debug_logging_name}"
    )
    # Wait for any known listing-card indicators; be tolerant of variants
    try:
        await page.waitForSelector(".listing-card", timeout=10000)
    except Exception:
        logger.debug(
            "'.listing-card' not found after clicking resale; trying PCard__title"
        )
        await page.waitForSelector(".PCard__title", timeout=10000)

    # Dump HTML after clicking resale so we can inspect the loaded listings
    try:
        os.makedirs("output", exist_ok=True)
        after_html = await page.content()
        ts2 = int(time.time())
        with open(f"output/debug_after_click_{ts2}.html", "w", encoding="utf-8") as fh:
            fh.write(after_html)
        logger.debug(
            "Wrote post-click page debug HTML: output/debug_after_click_%s.html", ts2
        )
    except Exception:
        logger.exception("Failed to write post-click page debug HTML")


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
