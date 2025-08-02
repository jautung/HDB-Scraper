# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import asyncio
import logging
import pyppeteer

FAKE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
logger = logging.getLogger(__name__)


# "homes.hdb.gov.sg" has dynamically loaded page content rendered by JavaScript (specifically Angular),
# so we can't simply GET request the static HTML (doing this yields minimal visible content of <app-root>...</app-root>).
# We instead use 'pyppeteer' to launch a headless browser and retrieve the rendered HTML instead.
class BrowserUtil:
    def __init__(
        self,
        single_browser_run_timeout_seconds,
        retry_delay_seconds,
        max_attempts_for_network_error,
        max_attempts_for_other_error,
        user_agent=None,
    ):
        self.browser = None
        self.page = None
        self.single_browser_run_timeout_seconds = single_browser_run_timeout_seconds
        self.retry_delay_seconds = retry_delay_seconds
        self.max_attempts_for_network_error = max_attempts_for_network_error
        self.max_attempts_for_other_error = max_attempts_for_other_error
        self.user_agent = user_agent

    async def run_with_browser_page_for_url(
        self, url, callback_on_page, debug_logging_name, current_attempt=1
    ):
        await self._maybe_close_page()

        try:
            return await asyncio.wait_for(
                self._inner_run_with_browser_page_for_url(
                    url=url,
                    callback_on_page=callback_on_page,
                    debug_logging_name=debug_logging_name,
                ),
                timeout=self.single_browser_run_timeout_seconds,
            )

        except (asyncio.TimeoutError, pyppeteer.errors.NetworkError) as e:
            if current_attempt >= self.max_attempts_for_network_error:
                logger.error(
                    f"Timeout or network error for {debug_logging_name} (attempt {current_attempt}), giving up!"
                )
                logger.error(e)
                return None

            logger.warning(
                f"Timeout or network error for {debug_logging_name} (attempt {current_attempt}), retrying!"
            )
            logger.debug(e)
            await asyncio.sleep(self.retry_delay_seconds)
            return await self.run_with_browser_page_for_url(
                url=url,
                callback_on_page=callback_on_page,
                debug_logging_name=debug_logging_name,
                current_attempt=current_attempt + 1,
            )

        except Exception as e:
            if current_attempt >= self.max_attempts_for_other_error:
                logger.error(
                    f"Unexpected error for {debug_logging_name} (attempt {current_attempt}), giving up!"
                )
                logger.error(e)
                return None

            logger.warning(
                f"Unexpected error for {debug_logging_name} (attempt {current_attempt}), retrying!"
            )
            logger.warning(e)
            await asyncio.sleep(self.retry_delay_seconds)
            return await self.run_with_browser_page_for_url(
                url=url,
                callback_on_page=callback_on_page,
                debug_logging_name=debug_logging_name,
                current_attempt=current_attempt + 1,
            )

        finally:
            await self._maybe_close_page()

    async def _inner_run_with_browser_page_for_url(
        self, url, callback_on_page, debug_logging_name
    ):
        self.page = await (await self._get_browser()).newPage()
        if self.user_agent is not None:
            await self.page.setUserAgent(self.user_agent)

        logger.debug(f"Navigating to {debug_logging_name}")
        await self.page.goto(url, waitUntil="networkidle0")

        return await callback_on_page(
            page=self.page,
            debug_logging_name=debug_logging_name,
        )

    async def _get_browser(self):
        if self.browser is None:
            logger.debug("Launching browser")
            self.browser = await pyppeteer.launch(
                headless=True, dumpio=False, logLevel=logger.level, autoClose=False
            )
        else:
            logger.debug("Browser already exists, using existing browser")

        return self.browser

    async def _maybe_close_page(self):
        if self.page is not None:
            if self.page.isClosed():
                logger.debug("Page is already closed")
                self.page = None
            else:
                logger.debug("Closing page")
                try:
                    await self.page.close()
                except pyppeteer.errors.NetworkError as e:
                    if "Target closed" in str(e):
                        logger.warning(
                            "Page was already closed, ignoring additional close"
                        )
                    else:
                        raise e
                self.page = None
        else:
            logger.debug("No page to close")

    async def maybe_close_browser(self):
        if self.browser is not None:
            logger.debug("Closing browser")
            await self.browser.close()
            self.browser = None
        else:
            logger.debug("No browser to close")


def get_paged_rendered_html_browser_page_callback(
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
