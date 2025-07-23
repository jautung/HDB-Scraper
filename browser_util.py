# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import asyncio
import logging
import pyppeteer

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
    ):
        self.browser = None
        self.page = None
        self.single_browser_run_timeout_seconds = single_browser_run_timeout_seconds
        self.retry_delay_seconds = retry_delay_seconds
        self.max_attempts_for_network_error = max_attempts_for_network_error
        self.max_attempts_for_other_error = max_attempts_for_other_error

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
