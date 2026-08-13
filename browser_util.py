# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import asyncio
import os
import shutil
import platform
import logging
import pyppeteer

FAKE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
logger = logging.getLogger(__name__)
SAFER_BROWSER_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
]


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
        self,
        url,
        callback_on_page,
        debug_logging_name,
        wait_until="networkidle0",
        wait_for_selector=None,
        timeout=None,
        validate_after_navigate=None,
        current_attempt=1,
    ):
        await self._maybe_close_page()

        try:
            return await asyncio.wait_for(
                self._inner_run_with_browser_page_for_url(
                    url=url,
                    callback_on_page=callback_on_page,
                    debug_logging_name=debug_logging_name,
                    wait_until=wait_until,
                    wait_for_selector=wait_for_selector,
                    timeout=timeout,
                    validate_after_navigate=validate_after_navigate,
                ),
                timeout=self.single_browser_run_timeout_seconds,
            )

        except (asyncio.TimeoutError, pyppeteer.errors.NetworkError) as e:
            await self.maybe_close_browser()
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
                wait_until=wait_until,
                wait_for_selector=wait_for_selector,
                timeout=timeout,
                validate_after_navigate=validate_after_navigate,
                current_attempt=current_attempt + 1,
            )

        except Exception as e:
            await self.maybe_close_browser()
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
                wait_until=wait_until,
                wait_for_selector=wait_for_selector,
                timeout=timeout,
                validate_after_navigate=validate_after_navigate,
                current_attempt=current_attempt + 1,
            )

        finally:
            await self._maybe_close_page()

    async def _inner_run_with_browser_page_for_url(
        self,
        url,
        callback_on_page,
        debug_logging_name,
        wait_until,
        wait_for_selector,
        timeout,
        validate_after_navigate,
    ):
        logger.debug("Creating new page for %s", debug_logging_name)
        try:
            browser = await self._get_browser()
            self.page = await browser.newPage()
        except Exception as e:
            logger.exception("Failed to create new page")
            raise

        if self.user_agent is not None:
            logger.debug("Setting user agent to %s", self.user_agent)
            try:
                await self.page.setUserAgent(self.user_agent)
            except Exception:
                logger.exception("Failed to set user agent")

        if timeout is None:
            logger.debug("Navigating to %s without timeout", debug_logging_name)
            try:
                await self.page.goto(url, waitUntil=wait_until)
            except Exception:
                logger.exception("Error during page.goto without timeout")
                raise
        else:
            logger.debug(
                "Navigating to %s with timeout %s", debug_logging_name, timeout
            )
            try:
                await self.page.goto(url, waitUntil=wait_until, timeout=timeout)
            except pyppeteer.errors.TimeoutError:
                logger.debug("Continuing after timeout for 'goto' navigation")
            except Exception:
                logger.exception("Error during page.goto with timeout")
                raise

        if validate_after_navigate is not None:
            logger.debug("Validating page after 'goto' navigation")
            if not (await validate_after_navigate(new_page=self.page)):
                return None

        if wait_for_selector is not None:
            logger.debug(
                "Waiting for selector %s on %s", wait_for_selector, debug_logging_name
            )
            try:
                await self.page.waitForSelector(wait_for_selector)
                # Count matching elements for debugging
                try:
                    count = await self.page.evaluate(
                        f"() => document.querySelectorAll('{wait_for_selector}').length"
                    )
                except Exception:
                    count = None
                logger.debug(
                    "Selector %s present; match count=%s", wait_for_selector, count
                )
            except Exception:
                logger.exception("Waiting for selector %s failed", wait_for_selector)

        return await callback_on_page(
            page=self.page,
            debug_logging_name=debug_logging_name,
        )

    async def _get_browser(self):
        if self.browser is not None:
            try:
                browser_process = getattr(self.browser, "process", None)
                if browser_process is not None and browser_process.poll() is not None:
                    logger.warning(
                        "Detected browser process has exited; restarting browser"
                    )
                    await self.maybe_close_browser()
                elif browser_process is None:
                    logger.warning("Browser process missing; restarting browser")
                    await self.maybe_close_browser()
            except Exception as e:
                logger.warning(
                    "Error checking browser process; resetting browser instance"
                )
                logger.warning(e)
                await self.maybe_close_browser()

        if self.browser is None:
            logger.debug(
                "Launching browser (pyppeteer.launch) with args=%s",
                SAFER_BROWSER_LAUNCH_ARGS,
            )
            try:
                # Enable dumpio to surface Chromium stdout/stderr into the process logs
                self.browser = await pyppeteer.launch(
                    headless=True,
                    dumpio=True,
                    logLevel=logger.level,
                    autoClose=False,
                    args=SAFER_BROWSER_LAUNCH_ARGS,
                )
                try:
                    ws = getattr(self.browser, "wsEndpoint", None)
                except Exception:
                    ws = None
                logger.debug("Launched browser; wsEndpoint=%s", ws)
            except Exception as e:
                logger.exception("pyppeteer.launch() raised an exception")

                # If the browser closed unexpectedly, try launching using a locally
                # installed Chrome/Chromium binary (common on macOS/Linux).
                try:
                    logger.debug(
                        "Attempting to locate local Chrome/Chromium binary as fallback"
                    )
                    candidates = []
                    system = platform.system().lower()
                    if system == "darwin":
                        candidates.extend(
                            [
                                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                                "/Applications/Chromium.app/Contents/MacOS/Chromium",
                            ]
                        )
                    else:
                        candidates.extend(
                            [
                                shutil.which("google-chrome-stable") or "",
                                shutil.which("google-chrome") or "",
                                shutil.which("chromium") or "",
                                shutil.which("chromium-browser") or "",
                            ]
                        )

                    candidates = [c for c in candidates if c]
                    if candidates:
                        for exec_path in candidates:
                            try:
                                logger.debug("Trying executablePath=%s", exec_path)
                                self.browser = await pyppeteer.launch(
                                    headless=True,
                                    dumpio=True,
                                    logLevel=logger.level,
                                    autoClose=False,
                                    args=SAFER_BROWSER_LAUNCH_ARGS,
                                    executablePath=exec_path,
                                )
                                ws = getattr(self.browser, "wsEndpoint", None)
                                logger.debug(
                                    "Launched browser via executablePath; wsEndpoint=%s",
                                    ws,
                                )
                                break
                            except Exception:
                                logger.exception(
                                    "Retry with executablePath %s failed", exec_path
                                )
                                self.browser = None
                        if self.browser is None:
                            logger.error("All fallback executablePath attempts failed")
                            raise
                    else:
                        logger.error(
                            "No local Chrome/Chromium binary found for fallback"
                        )
                        raise
                except Exception:
                    raise
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
            try:
                await self.browser.close()
            except Exception as e:
                logger.warning("Error closing browser; resetting browser instance")
                logger.warning(e)
            finally:
                self.browser = None
        else:
            logger.debug("No browser to close")


def get_single_rendered_html_browser_page_callback(
    wait_for_selector=None, additional_action=None
):
    async def _callback(page, debug_logging_name):
        if wait_for_selector is not None:
            logger.debug(
                f"Waiting for selector {wait_for_selector} of {debug_logging_name}"
            )
            await page.waitForSelector(wait_for_selector)

        if additional_action is not None:
            await additional_action(page=page, debug_logging_name=debug_logging_name)

        logger.debug(f"Extracting rendered HTML from {debug_logging_name}")
        try:
            html = await page.content()
            logger.debug(
                "Successfully extracted rendered HTML from %s (len=%d)",
                debug_logging_name,
                len(html),
            )
            # Save a debug dump of the rendered page for inspection
            try:
                safe_name = "debug_" + "_".join(
                    [c if c.isalnum() else "_" for c in debug_logging_name]
                )
                os.makedirs("output", exist_ok=True)
                with open(f"output/{safe_name}.html", "w", encoding="utf-8") as fh:
                    fh.write(html)
                logger.debug("Wrote debug HTML to output/%s.html", safe_name)
            except Exception:
                logger.exception("Unable to write debug HTML to output folder")
            return html
        except Exception:
            logger.exception(
                "Failed to extract page.content() for %s", debug_logging_name
            )
            raise

    return _callback


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
