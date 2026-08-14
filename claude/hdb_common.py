#!/usr/bin/env python3
"""
Shared helpers for talking to the HDB Flat Portal's public API
(api.homes.hdb.gov.sg/flatback/public/v1/...).

The API sits behind a double-submit CSRF check: the server hands out a
fresh `XSRF-TOKEN` cookie on every response (via Set-Cookie -- even on a
403), and expects that same value echoed back as the `x-xsrf-token`
request header on the next call. `post_json` below handles that handshake
automatically: it primes the session on first use and re-attaches whatever
token it currently holds on every call.
"""

import json
import sys
import time

import requests

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "origin": "https://homes.hdb.gov.sg",
    "referer": "https://homes.hdb.gov.sg/",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

PHOTO_BASE_URL = "https://resource.homes.hdb.gov.sg/"
LISTING_URL_TEMPLATE = "https://homes.hdb.gov.sg/home/resale/{id}"


def new_session():
    return requests.Session()


def post_json(session, url, payload, retries=4, backoff=2.0, timeout=30, debug=False):
    """POST `payload` as JSON to `url`, handling the XSRF handshake and retries.

    Returns the parsed JSON response body. Raises RuntimeError if all
    retries are exhausted.
    """

    def _debug(msg):
        if debug:
            print(f"[debug] {msg}", file=sys.stderr)

    last_err = None
    for attempt in range(1, retries + 1):
        headers = dict(HEADERS)
        token = session.cookies.get("XSRF-TOKEN")
        if token:
            headers["x-xsrf-token"] = token

        try:
            resp = session.post(url, headers=headers, json=payload, timeout=timeout)
            _debug(f"{url} attempt {attempt}: POST -> {resp.status_code}")

            if resp.status_code == 403:
                new_token = session.cookies.get("XSRF-TOKEN")
                if new_token and new_token != token:
                    _debug(
                        f"{url} attempt {attempt}: got fresh XSRF-TOKEN from 403, retrying immediately"
                    )
                    continue
                resp.raise_for_status()

            if resp.status_code == 404:
                # Not found is not transient -- e.g. getPastTransaction 404s
                # for addresses with no resale history. Retrying won't help,
                # so fail immediately instead of burning the retry budget.
                raise requests.HTTPError(
                    f"404 Client Error: Not Found for url: {url}", response=resp
                )

            resp.raise_for_status()
            if not resp.content:
                return None
            return resp.json()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise RuntimeError(f"404 Not Found: {url}") from exc
            last_err = exc
            print(
                f"[warn] {url} attempt {attempt}/{retries} failed: {exc}",
                file=sys.stderr,
            )
            if attempt < retries:
                time.sleep(backoff * attempt)
        except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
            last_err = exc
            print(
                f"[warn] {url} attempt {attempt}/{retries} failed: {exc}",
                file=sys.stderr,
            )
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"Failed POST to {url} after {retries} attempts: {last_err}")


def extract_lat_lon(coords_value):
    """Parse a coords value that may be '[a, b]' as a string or a list/tuple,
    and disambiguate lat vs lon by value range (Singapore: lat ~1-2, lon ~103-104)
    since different endpoints on this API order the pair differently.
    """
    if coords_value is None:
        return None, None
    try:
        parsed = (
            json.loads(coords_value) if isinstance(coords_value, str) else coords_value
        )
    except (json.JSONDecodeError, TypeError):
        return None, None
    if not isinstance(parsed, (list, tuple)) or len(parsed) != 2:
        return None, None
    a, b = parsed
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return None, None
    # Singapore latitude ~1.1-1.5, longitude ~103.5-104.1
    if 0 < a < 10 and 100 < b < 110:
        return a, b  # (lat, lon)
    if 0 < b < 10 and 100 < a < 110:
        return b, a  # was (lon, lat)
    return a, b  # fallback: return as-is, uncertain order
