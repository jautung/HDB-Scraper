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

Output: one row per listing (i.e. per `desc` entry), flattened to CSV.

Listing detail pages live at https://homes.hdb.gov.sg/home/resale/{id}
(confirmed). Step 2 (scrape_details.py, not yet written) will still need the
underlying detail JSON endpoint -- grab its cURL from DevTools on one of
these pages the same way the map endpoint was found.
"""

import argparse
import csv
import json
from datetime import datetime, timezone

from hdb_common import (
    HEADERS,
    PHOTO_BASE_URL,
    LISTING_URL_TEMPLATE,
    new_session,
    post_json,
)

API_URL = "https://api.homes.hdb.gov.sg/flatback/public/v1/map/getCoordinatesByFilters"

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


def fetch_all_listings(
    payload=None, session=None, retries=4, backoff=2.0, timeout=30, debug=False
):
    """Call the public map API and return the raw JSON list of pin objects.

    Delegates the CSRF handshake (fresh XSRF-TOKEN cookie <-> x-xsrf-token
    header) to hdb_common.post_json -- see that module for details.
    """
    payload = payload or DEFAULT_PAYLOAD
    sess = session or new_session()
    data = post_json(
        sess,
        API_URL,
        payload,
        retries=retries,
        backoff=backoff,
        timeout=timeout,
        debug=debug,
    )
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response shape from {API_URL}: {type(data)}")
    return data


def flatten_pins(pins):
    """Turn the nested pin/desc structure into one flat dict per listing."""
    rows = []
    for pin in pins:
        addr = pin.get("props", {}).get("addr", "")
        region = pin.get("props", {}).get("region", "")

        coords_raw = pin.get("coords", "")
        lat, lon = None, None
        try:
            parsed = (
                json.loads(coords_raw) if isinstance(coords_raw, str) else coords_raw
            )
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
                    "url": (
                        LISTING_URL_TEMPLATE.format(id=listing_id) if listing_id else ""
                    ),
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


def main():
    parser = argparse.ArgumentParser(
        description="Scrape HDB resale flat listing index."
    )
    parser.add_argument(
        "--out",
        default="hdb_resale_listings",
        help="Output CSV base name (without extension). Default: hdb_resale_listings",
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

    rows = flatten_pins(pins)
    print(f"Flattened to {len(rows)} individual listing(s).")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"Scrape timestamp: {timestamp}")

    csv_path = f"{args.out}.csv"
    write_csv(rows, csv_path)
    print(f"Wrote CSV -> {csv_path}")


if __name__ == "__main__":
    main()
