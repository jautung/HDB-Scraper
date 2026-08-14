#!/usr/bin/env python3
"""
Step 2: for each listing_id produced by scrape_listings.py, fetch the full
detail record by chaining API calls:

    1. POST .../v1/listing/resale/detailsJdbc      {"listingId": id}
    2. POST .../v1/resale/getAllImagesByListing     {"listingId": id}
    3. POST .../v1/map/getListingCoordinates        {"listingId": id}
    4. POST .../v1/listing/getFlatUpgradingDescription  {"postalCode": <from #1>}

Output (under `output/` by default):
    - <out>.csv              one row per listing (listings index columns + detail fields)

Columns shared with the listings CSV (listing_id, url, flat_type, price,
latitude, longitude) come from the detail API. Listings-only columns
(address, region, max_price, max_lease_years, created_at) are copied from
the step-1 CSV. area_sqm is omitted (floor_area_sqm from the detail API
is used instead). photo_url from step 1 is omitted (photo_main / photo_urls
from the detail API are used instead).

Resumable: if the output CSV already exists, listing_ids already present in
it are skipped on a re-run (use --overwrite to start fresh). Progress is
flushed to disk after every listing, so a crash partway through a ~2,000+
listing run doesn't lose completed work.
"""

import argparse
import csv
import os
import sys
import time
import logging

from hdb_common import (
    DEFAULT_DETAILS_BASE,
    DEFAULT_LISTINGS_CSV,
    DETAIL_FIELDNAMES,
    LISTINGS_INDEX_ONLY_FIELDNAMES,
    PHOTO_BASE_URL,
    LISTING_URL_TEMPLATE,
    ensure_parent_dir,
    extract_lat_lon,
    new_session,
    post_json,
)

DETAILS_URL = (
    "https://api.homes.hdb.gov.sg/flatback/public/v1/listing/resale/detailsJdbc"
)
IMAGES_URL = (
    "https://api.homes.hdb.gov.sg/flatback/public/v1/resale/getAllImagesByListing"
)
COORDS_URL = "https://api.homes.hdb.gov.sg/flatback/public/v1/map/getListingCoordinates"
UPGRADING_URL = "https://api.homes.hdb.gov.sg/flatback/public/v1/listing/getFlatUpgradingDescription"


def load_listings_index(input_path):
    """Read the step-1 CSV into an ordered id list and id -> row lookup."""
    ids = []
    index = {}
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "listing_id" not in (reader.fieldnames or []):
            raise ValueError(
                f"'listing_id' column not found in {input_path}. Columns seen: {reader.fieldnames}"
            )
        for row in reader:
            lid = (row.get("listing_id") or "").strip()
            if lid:
                ids.append(lid)
                index[lid] = row
    return ids, index


def apply_listings_index(row, index_row):
    """Copy listings-only columns from the step-1 CSV into a detail row."""
    for fn in LISTINGS_INDEX_ONLY_FIELDNAMES:
        row[fn] = (index_row.get(fn) or "") if index_row else ""
    return row


def normalize_detail_row(row):
    return {fn: row.get(fn, "") for fn in DETAIL_FIELDNAMES}


def load_existing_earliest_map(output_path):
    """Read existing detail rows (if any) and return a mapping from listing_id
    to the earliest_scraped value seen for that listing. Used to preserve
    the original earliest timestamp when appending new rows.
    """
    if not os.path.exists(output_path):
        return {}
    earliest = {}
    with open(output_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = (row.get("listing_id") or "").strip()
            if not lid:
                continue
            es = (row.get("earliest_scraped") or "").strip()
            if not es:
                es = (row.get("latest_scraped") or "").strip()
            if not es:
                continue
            prev = earliest.get(lid)
            # Prefer the lexicographically smaller ISO timestamp if available.
            earliest[lid] = es if prev is None or es < prev else prev
    return earliest


def load_existing_details(output_path):
    """Load existing detail rows into a mapping of listing_id -> row.

    If multiple rows for the same listing_id exist, the last occurrence
    in the file is used.
    """
    if not os.path.exists(output_path):
        return {}
    out = {}
    with open(output_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = (row.get("listing_id") or "").strip()
            if not lid:
                continue
            out[lid] = row
    return out


def fetch_listing_detail(
    session, listing_id, index_row=None, retries=4, backoff=2.0, timeout=30, debug=False
):
    """Fetch full detail for one listing.

    detailsJdbc is treated as a hard requirement -- if it fails, this
    listing is skipped for this run (the error is logged) and can be
    retried later. The other four calls (images, coordinates,
    upgrading description, past transactions) fail *soft* and are
    logged to stderr but do not prevent emitting a detail row.
    """
    row = {fn: "" for fn in DETAIL_FIELDNAMES}
    row["listing_id"] = listing_id
    row["url"] = LISTING_URL_TEMPLATE.format(id=listing_id)
    apply_listings_index(row, index_row)
    warnings = []

    def call(url, payload):
        return post_json(
            session,
            url,
            payload,
            retries=retries,
            backoff=backoff,
            timeout=timeout,
            debug=debug,
        )

    try:
        details = call(DETAILS_URL, {"listingId": listing_id}) or {}
    except Exception as exc:  # noqa: BLE001 -- hard failure, skip this listing for now
        logger = logging.getLogger(__name__)
        logger.error("listing %s failed (core details call): %s", listing_id, exc)
        return None

    row["flat_type"] = details.get("flatType", "")
    row["town"] = details.get("town", "")
    row["street"] = details.get("street", "")
    row["block"] = details.get("block", "")
    row["postal"] = details.get("postal", "")
    row["storey_range"] = details.get("storeyRange", "")
    row["floor_area_sqm"] = details.get("floorArea", "")
    row["bedroom"] = details.get("bedroom", "")
    row["bathroom"] = details.get("bathroom", "")
    row["balcony"] = details.get("balcony", "")
    row["extension"] = details.get("extension", "")
    row["contra"] = details.get("contra", "")
    row["price"] = details.get("price", "")
    row["remaining_lease"] = details.get("remainingLease", "")
    row["ethnic_eligibility"] = details.get("ethnicEligibility", "")
    row["spr_eligibility"] = details.get("sprEligibility", "")
    row["ethnic_eligibility_date"] = details.get("ethnicEligibilityDate", "")
    row["managed_by_agent"] = details.get("managedByAgent", "")

    agents = details.get("description") or []
    if agents:
        agent = agents[0]
        row["agent_name"] = agent.get("name", "")
        row["agent_number"] = agent.get("number", "")
        row["agent_email"] = agent.get("email", "")
        row["agent_agency_name"] = agent.get("agencyName", "")
        row["agent_cea_number"] = agent.get("ceaNumber", "")
        row["agent_license_no"] = agent.get("licenseNo", "")
        row["agent_last_updated"] = agent.get("lastUpdated", "")
        row["listing_description"] = " | ".join(
            (a.get("description") or "").replace("\n", " ")
            for a in agents
            if a.get("description")
        )

    main_photo = details.get("photo", "")
    row["photo_main"] = (PHOTO_BASE_URL + main_photo) if main_photo else ""

    # --- images (soft-fail) ---
    try:
        images = call(IMAGES_URL, {"listingId": listing_id}) or {}
        scanned = images.get("scannedList") or []
        row["photo_count"] = len(scanned)
        row["photo_urls"] = " | ".join(PHOTO_BASE_URL + p for p in scanned)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"images: {exc}")

    # --- coordinates (soft-fail) ---
    try:
        coords_resp = call(COORDS_URL, {"listingId": listing_id}) or {}
        coords_raw = (coords_resp.get("geometry") or {}).get("coords")
        lat, lon = extract_lat_lon(coords_raw)
        row["latitude"] = lat if lat is not None else ""
        row["longitude"] = lon if lon is not None else ""
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"coordinates: {exc}")

    # --- upgrading description (soft-fail, needs postal) ---
    postal = details.get("postal")
    if postal:
        try:
            upgrading = call(UPGRADING_URL, {"postalCode": postal}) or {}
            row["upgrading_tooltip"] = upgrading.get("tooltipDesc", "")
            row["upgrading_short_desc"] = "; ".join(upgrading.get("shortDesc") or [])
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"upgrading_description: {exc}")

    if warnings:
        logger = logging.getLogger(__name__)
        logger.warning("listing %s warnings: %s", listing_id, "; ".join(warnings))
    return row


def main():
    parser = argparse.ArgumentParser(
        description="Scrape full details for each HDB resale listing_id."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_LISTINGS_CSV,
        help=f"CSV from scrape_listings.py containing a listing_id column. Default: {DEFAULT_LISTINGS_CSV}",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_DETAILS_BASE,
        help=f"Output file base name (without extension). Default: {DEFAULT_DETAILS_BASE}",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.4,
        help="Seconds to sleep between listings, to be polite to the API. Default: 0.4",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N listings (useful for a test run).",
    )
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Skip listings that already exist in the output details CSV (do not refetch or update timestamps).",
    )
    # Always append; no overwrite mode
    parser.add_argument(
        "--debug", action="store_true", help="Verbose per-request logging to stderr."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s (%(name)s) [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    detail_path = f"{args.out}.csv"
    ensure_parent_dir(detail_path)

    ids, listings_index = load_listings_index(args.input)
    logger.info("Loaded %d listing_id(s) from %s", len(ids), args.input)

    if args.limit:
        ids = ids[: args.limit]
        logger.info("--limit set: processing only first %d", len(ids))

    # Load existing details and earliest timestamps to preserve earliest_scraped
    existing = load_existing_details(detail_path)
    existing_earliest = load_existing_earliest_map(detail_path)

    session = new_session()

    touched = set()
    ok_count = 0
    err_count = 0

    # Process each listing and update in-memory `existing` mapping; do not append rows.
    for i, listing_id in enumerate(ids, 1):
        # If --only-new is set and this listing already exists, skip refetching.
        if args.only_new and listing_id in existing:
            touched.add(listing_id)
            if i % 25 == 0 or i == len(ids):
                logger.info(
                    "[%d/%d] skipping existing (only-new) %s", i, len(ids), listing_id
                )
            if args.delay and i < len(ids):
                time.sleep(args.delay)
            continue
        row = fetch_listing_detail(
            session, listing_id, listings_index.get(listing_id), debug=args.debug
        )
        if row is None:
            err_count += 1
            if i % 25 == 0 or i == len(ids):
                logger.info(
                    "[%d/%d] ok=%d err=%d (last: %s)",
                    i,
                    len(ids),
                    ok_count,
                    err_count,
                    listing_id,
                )
            if args.delay and i < len(ids):
                time.sleep(args.delay)
            continue

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        prev = existing.get(listing_id)
        # preserve earliest from existing row, or from earliest map, or set to now
        if prev and (prev.get("earliest_scraped") or ""):
            row["earliest_scraped"] = prev.get("earliest_scraped")
        else:
            row["earliest_scraped"] = existing_earliest.get(listing_id) or now
        row["latest_scraped"] = now
        row["is_still_listed"] = "true"

        apply_listings_index(row, listings_index.get(listing_id))

        # Update in-memory record (single current row per listing)
        existing[listing_id] = normalize_detail_row(row)
        touched.add(listing_id)

        ok_count += 1
        if i % 25 == 0 or i == len(ids):
            logger.info(
                "[%d/%d] ok=%d err=%d (last: %s)",
                i,
                len(ids),
                ok_count,
                err_count,
                listing_id,
            )
        if args.delay and i < len(ids):
            time.sleep(args.delay)
    # Any existing listing not touched this run should be marked as not listed.
    # Do NOT update `latest_scraped` for untouched rows — only change
    # `latest_scraped` when we actually fetched/updated the row.
    for lid, r in list(existing.items()):
        if lid not in touched:
            r["is_still_listed"] = "false"

    # Overwrite details CSV with one current row per listing
    with open(detail_path, "w", newline="", encoding="utf-8") as df:
        writer = csv.DictWriter(df, fieldnames=DETAIL_FIELDNAMES)
        writer.writeheader()
        for lid in sorted(existing.keys()):
            writer.writerow(existing[lid])

    logger.info("Done. Wrote current details -> %s", detail_path)
    if err_count:
        logger.error("%d listing(s) failed during fetch.", err_count)


if __name__ == "__main__":
    main()
