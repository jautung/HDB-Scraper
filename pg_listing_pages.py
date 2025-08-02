# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import logging
import pyppeteer
import random
import requests
import browser_util
import file_util
from playwright.async_api import async_playwright

PROPERTY_GURU_URL = "https://www.propertyguru.com.sg/property-for-sale"
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
logger = logging.getLogger(__name__)


async def get_listing_page_html_playwright():
    print("Launching browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            slow_mo=100,
            args=[
                # '--no-sandbox',
                # '--disable-blink-features=AutomationControlled',
                # '--disable-infobars',
                # '--window-size=1920,1080',
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            ),
            # locale='en-US',
            # viewport={'width': 1920, 'height': 1080},
        )

        # Override navigator.webdriver to undefined in all pages:
        # await context.add_init_script("""
        #     Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        #     window.navigator.chrome = { runtime: {} };
        #     Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        #     Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        # """)

        page = await context.new_page()

        # Debugging event listeners
        # page.on("response", lambda response: print(f"Response: {response.status} {response.url}"))
        # page.on("framenavigated", lambda frame: print(f"Navigated to {frame.url}"))

        print("Going to PropertyGuru page...")
        await page.goto(PROPERTY_GURU_URL, wait_until="domcontentloaded", timeout=60000)

        # Give it some extra time after domcontentloaded to make sure JS loads
        await asyncio.sleep(1)

        html = await page.content()
        print(f"Page loaded, length of content: {len(html)}")

        if "Verifying you are human." in html:
            print("Hit bot detection page!")
        else:
            print("Page loaded successfully without bot detection.")
            print("Success?", "Contact Agent" in html)

        await browser.close()


async def get_listing_page_html_manual():
    logger.info(f"Starting to get all listing URLs from {PROPERTY_GURU_URL}")

    logger.debug(f"Getting paged HTMLs from {PROPERTY_GURU_URL}")
    browser = await pyppeteer.launch(
        headless=True, dumpio=False, logLevel=logger.level, autoClose=False
    )
    page = await browser.newPage()
    # print('new page!')

    # await page.setViewport({'width': 1920, 'height': 1080})
    # print('set viewport')

    await page.setUserAgent(browser_util.FAKE_USER_AGENT)
    # print('set user agent')

    # await page.evaluateOnNewDocument("""
    # () => {
    #     // Pass the webdriver test
    #     Object.defineProperty(navigator, 'webdriver', {get: () => undefined});

    #     // Pass the chrome test
    #     window.navigator.chrome = { runtime: {} };

    #     // Pass the languages test
    #     Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});

    #     // Pass the plugins test
    #     Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});

    #     // Pass the permissions test
    #     const originalQuery = window.navigator.permissions.query;
    #     window.navigator.permissions.query = (parameters) => (
    #         parameters.name === 'notifications' ?
    #             Promise.resolve({ state: Notification.permission }) :
    #             originalQuery(parameters)
    #     );

    #     // Spoof userAgentData (newer Chrome API)
    #     Object.defineProperty(navigator, 'userAgentData', {
    #         get: () => ({
    #             brands: [
    #                 { brand: "Chromium", version: "114" },
    #                 { brand: "Google Chrome", version: "114" }
    #             ],
    #             mobile: false,
    #             platform: "Windows"
    #         })
    #     });

    #     // Mock plugins length
    #     Object.defineProperty(navigator.plugins, 'length', { get: () => 5 });

    #     // Mock permissions for notifications explicitly
    #     const originalQueryPermissions = navigator.permissions.query;
    #     navigator.permissions.query = (parameters) => {
    #         if (parameters.name === 'notifications') {
    #             return Promise.resolve({ state: Notification.permission });
    #         }
    #         return originalQueryPermissions(parameters);
    #     };
    # }
    # """)
    print("set eval on new document")

    await page.goto(
        "https://www.propertyguru.com.sg/property-for-sale",
        {"waitUntil": "domcontentloaded", "timeout": 60000},
    )
    print("GOTO")
    # await asyncio.sleep(3 + random.random() * 3)
    html = await page.content()
    # print(html)
    print()
    print("Bot page?", "Verifying you are human." in html)
    print("Success?", "Contact Agent" in html)
    await page.close()
    # await asyncio.sleep(10)
    await browser.close()


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
            initial_action=None,  # _xcxc_initial_action,
            pagination_action=None,  # _xcxc_pagination_action,
        ),
        debug_logging_name=PROPERTY_GURU_URL,
        wait_until="domcontentloaded",
    )
    await browser.maybe_close_browser()
    htmls = [] if htmls is None else htmls
    print(len(htmls))
    print("Bot page?", "Verifying you are human." in htmls[0])
    print("Success?", "Contact Agent" in htmls[0])


async def _xcxc_initial_action(page, debug_logging_name):
    return


async def _xcxc_pagination_action(page, debug_logging_name):
    return False


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
    # asyncio.run(get_listing_page_html_playwright())
    # print("manual")
    # asyncio.run(get_listing_page_html_manual())
    # print("util")
    asyncio.run(_get_listing_urls())


if __name__ == "__main__":
    main()
