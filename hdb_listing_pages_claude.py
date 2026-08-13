#!/usr/bin/env python3
"""
Scrape the full HDB resale flat listing index from the (unauthenticated,
public) HDB Flat Portal map API.

Endpoint discovered via browser DevTools:
    POST https://api.homes.hdb.gov.sg/flatback/public/v1/map/getCoordinatesByFilters

This endpoint returns ALL matching listings in a single response (no
server-side pagination observed) as a list of "map pin" objects, each of
which can bundle multiple listings (`desc` is a list) at the same
coordinate/address.

Output: one row per listing (i.e. per `desc` entry), flattened to CSV/JSON.

Listing detail pages live at https://homes.hdb.gov.sg/home/resale/{id}
(confirmed). Step 2 (scrape_details.py, not yet written) will still need the
underlying detail JSON endpoint -- grab its cURL from DevTools on one of
these pages the same way the map endpoint was found.
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone

import requests

API_URL = "https://api.homes.hdb.gov.sg/flatback/public/v1/map/getCoordinatesByFilters"
BOOTSTRAP_URL = "https://homes.hdb.gov.sg/home/finding-a-flat"

# Confirmed against a real listing.
LISTING_URL_TEMPLATE = "https://homes.hdb.gov.sg/home/resale/{id}"

# Confirmed image CDN. Note: the live site appends a `?t=<cache-busting
# timestamp>` query param (e.g. ?t=1786635460262) tied to page-load time --
# not required for the image to load, so we omit it here.
PHOTO_BASE_URL = "https://resource.homes.hdb.gov.sg/"

DEFAULT_PAYLOAD = {
    "town": "",
    "location": "",
    "range": "2",
    "classification": "",
    "priceRangeLower": "0",
    "priceRangeUpper": "0",
    "flatType": "",
    "waitingTime": "",
    "modeOfSale": "Resale",
    "remainingLeaseRangeLower": 1,
    "remainingLeaseRangeUpper": 99,
    "salesPerson": False,
    "floorRange": "",
    "ethnicGroup": "",
    "citizenship": "",
    "extension": "",
    "contra": "",
    "rank": "Location, Price Range, Flat Type, Remaining Lease",
    "coordinates": [["", ""]],
    "fullResult": True,
}

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
    # Browser-like extras -- the API sits behind a WAF/bot-check that appears
    # to care about these being present, not just the CSRF token.
    "sec-ch-ua": '"Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

FIELDNAMES = [
    "listing_id",
    "url",
    "title",
    "address",
    "region",
    "flat_type",
    "area_sqm",
    "price",
    "max_price",
    "max_lease_years",
    "created_at",
    "latitude",
    "longitude",
    "photo_path",
    "photo_url",
    "raw_desc_json",
]


def fetch_all_listings(payload=None, session=None, retries=4, backoff=2.0, timeout=30, debug=False):
    """Call the public map API and return the raw JSON list of pin objects.

    The API sits behind a double-submit CSRF check: the server hands out a
    fresh `XSRF-TOKEN` cookie on every response (Set-Cookie -- even on a 403),
    and expects that same value echoed back as the `x-xsrf-token` request
    header on the next call. So we can't just hardcode a token/cookie string
    (it expires in ~1hr per Max-Age=3600 and rotates anyway) -- instead we
    prime the session with a first request, read whatever cookie the server
    just gave us, and retry with it attached.
    """
    payload = payload or DEFAULT_PAYLOAD
    sess = session or requests.Session()

    def _debug(msg):
        if debug:
            print(f"[debug] {msg}", file=sys.stderr)

    last_err = None
    for attempt in range(1, retries + 1):
        headers = dict(HEADERS)
        token = sess.cookies.get("XSRF-TOKEN")
        if token:
            headers["x-xsrf-token"] = token
            _debug(f"attempt {attempt}: sending with x-xsrf-token={token}")
        else:
            _debug(f"attempt {attempt}: no XSRF-TOKEN cookie yet, sending without header (priming request)")

        try:
            resp = sess.post(API_URL, headers=headers, json=payload, timeout=timeout)
            _debug(f"attempt {attempt}: POST -> {resp.status_code}")
            _debug(f"attempt {attempt}: cookies now held: {dict(sess.cookies)}")

            if resp.status_code == 403:
                new_token = sess.cookies.get("XSRF-TOKEN")
                if new_token and new_token != token:
                    # Server just handed us a usable token via Set-Cookie on
                    # this very 403 -- retry immediately with it, no backoff
                    # wait needed since this isn't rate limiting.
                    _debug(f"attempt {attempt}: got fresh XSRF-TOKEN from 403 response, retrying immediately")
                    continue
                resp.raise_for_status()

            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected response shape: {type(data)}")
            return data
        except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
            last_err = exc
            print(f"[warn] attempt {attempt}/{retries} failed: {exc}", file=sys.stderr)
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"Failed to fetch listings after {retries} attempts: {last_err}")


def flatten_pins(pins):
    """Turn the nested pin/desc structure into one flat dict per listing."""
    rows = []
    for pin in pins:
        addr = pin.get("props", {}).get("addr", "")
        region = pin.get("props", {}).get("region", "")

        coords_raw = pin.get("coords", "")
        lat, lon = None, None
        try:
            parsed = json.loads(coords_raw) if isinstance(coords_raw, str) else coords_raw
            if isinstance(parsed, (list, tuple)) and len(parsed) == 2:
                lat, lon = parsed[0], parsed[1]
        except (json.JSONDecodeError, TypeError):
            pass

        for listing in pin.get("props", {}).get("desc", []):
            listing_id = listing.get("id", "")
            photo_path = listing.get("photo", "")
            rows.append(
                {
                    "listing_id": listing_id,
                    "url": LISTING_URL_TEMPLATE.format(id=listing_id) if listing_id else "",
                    "title": f"{listing.get('type', '')} at {addr}".strip(),
                    "address": addr,
                    "region": region,
                    "flat_type": listing.get("type", ""),
                    "area_sqm": listing.get("area", ""),
                    "price": listing.get("price", ""),
                    "max_price": listing.get("maxPrice", ""),
                    "max_lease_years": listing.get("maxLease", ""),
                    "created_at": listing.get("createDt", ""),
                    "latitude": lat,
                    "longitude": lon,
                    "photo_path": photo_path,
                    "photo_url": (PHOTO_BASE_URL + photo_path) if photo_path else "",
                    "raw_desc_json": json.dumps(listing, ensure_ascii=False),
                }
            )
    return rows


def write_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_json(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Scrape HDB resale flat listing index.")
    parser.add_argument(
        "--out",
        default="hdb_resale_listings",
        help="Output file base name (without extension). Default: hdb_resale_listings",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json", "both"],
        default="both",
        help="Output format. Default: both",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Also dump the raw, unflattened API response to <out>_raw.json",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print verbose request/response/cookie info to stderr (for diagnosing 403s etc).",
    )
    args = parser.parse_args()

    print("Fetching listings from HDB public map API...")
    pins = fetch_all_listings(debug=args.debug)
    print(f"Received {len(pins)} map pin(s).")

    if args.raw:
        raw_path = f"{args.out}_raw.json"
        write_json(pins, raw_path)
        print(f"Wrote raw response -> {raw_path}")

    rows = flatten_pins(pins)
    print(f"Flattened to {len(rows)} individual listing(s).")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Scrape timestamp: {timestamp}")

    if args.format in ("csv", "both"):
        csv_path = f"{args.out}.csv"
        write_csv(rows, csv_path)
        print(f"Wrote CSV -> {csv_path}")

    if args.format in ("json", "both"):
        json_path = f"{args.out}.json"
        write_json(rows, json_path)
        print(f"Wrote JSON -> {json_path}")


if __name__ == "__main__":
    main()