#!/usr/bin/env python3
"""
Step 2: for each listing_id produced by scrape_listings.py, fetch the full
detail record by chaining five HDB Flat Portal API calls:

  1. POST .../v1/listing/resale/detailsJdbc      {"listingId": id}
  2. POST .../v1/resale/getAllImagesByListing     {"listingId": id}
  3. POST .../v1/map/getListingCoordinates        {"listingId": id}
  4. POST .../v1/listing/getFlatUpgradingDescription  {"postalCode": <from #1>}
  5. POST .../v1/transaction/getPastTransaction   {"postal": <from #1>, "flatType": <from #1>}

(4) and (5) depend on fields returned by (1), so they must run after it.

Output:
  - <out>.csv              one row per listing (flattened detail fields)
  - <out>_transactions.csv one row per past transaction, linked by listing_id

Resumable: if the output CSV already exists, listing_ids already present in
it are skipped on a re-run (use --overwrite to start fresh). Progress is
flushed to disk after every listing, so a crash partway through a ~2,000+
listing run doesn't lose completed work.
"""

import argparse
import csv
import json
import os
import sys
import time

from hdb_common import (
    PHOTO_BASE_URL,
    LISTING_URL_TEMPLATE,
    new_session,
    post_json,
    extract_lat_lon,
)

DETAILS_URL = (
    "https://api.homes.hdb.gov.sg/flatback/public/v1/listing/resale/detailsJdbc"
)
IMAGES_URL = (
    "https://api.homes.hdb.gov.sg/flatback/public/v1/resale/getAllImagesByListing"
)
COORDS_URL = "https://api.homes.hdb.gov.sg/flatback/public/v1/map/getListingCoordinates"
UPGRADING_URL = "https://api.homes.hdb.gov.sg/flatback/public/v1/listing/getFlatUpgradingDescription"
PAST_TXN_URL = (
    "https://api.homes.hdb.gov.sg/flatback/public/v1/transaction/getPastTransaction"
)

DETAIL_FIELDNAMES = [
    "listing_id",
    "url",
    "flat_type",
    "town",
    "street",
    "block",
    "postal",
    "storey_range",
    "floor_area_sqm",
    "bedroom",
    "bathroom",
    "balcony",
    "extension",
    "contra",
    "price",
    "remaining_lease",
    "ethnic_eligibility",
    "spr_eligibility",
    "ethnic_eligibility_date",
    "managed_by_agent",
    "agent_name",
    "agent_number",
    "agent_email",
    "agent_agency_name",
    "agent_cea_number",
    "agent_license_no",
    "listing_description",
    "agent_last_updated",
    "latitude",
    "longitude",
    "photo_main",
    "photo_count",
    "photo_urls",
    "upgrading_tooltip",
    "upgrading_short_desc",
    "past_transaction_count",
    "scraped_ok",
    "warnings",
    "error",
]

TXN_FIELDNAMES = [
    "listing_id",
    "street",
    "block",
    "range",
    "floor_area_sqm",
    "model",
    "lease_comm",
    "lease_tenure_years",
    "lease_tenure_months",
    "resale_price",
    "registration_date",
]


def load_listing_ids(input_path):
    """Read listing_id values out of the CSV produced by scrape_listings.py."""
    ids = []
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
    return ids


def load_successful_rows(output_path):
    """Read existing detail rows, keeping only ones that succeeded (scraped_ok == '1').
    Failed rows are dropped so they get retried (and re-appended fresh) on this run.
    Returns (kept_rows, done_ids_set).
    """
    if not os.path.exists(output_path):
        return [], set()
    kept = []
    done = set()
    with open(output_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = (row.get("listing_id") or "").strip()
            if lid and row.get("scraped_ok") == "1":
                kept.append(row)
                done.add(lid)
    return kept, done


def load_kept_transactions(txn_path, keep_ids):
    """Read existing transaction rows, keeping only ones whose listing_id is in keep_ids
    (i.e. belongs to a listing that succeeded and is being preserved across the resume).
    """
    if not os.path.exists(txn_path):
        return []
    kept = []
    with open(txn_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("listing_id") or "").strip() in keep_ids:
                kept.append(row)
    return kept


def fetch_listing_detail(
    session, listing_id, retries=4, backoff=2.0, timeout=30, debug=False
):
    """Fetch full detail for one listing.

    detailsJdbc is treated as a hard requirement -- if it fails, the whole
    row is marked scraped_ok=0 and gets retried on the next run.

    The other four calls (images, coordinates, upgrading description, past
    transactions) fail *soft*: e.g. getPastTransaction legitimately 404s for
    flats with no resale history at that address, and a missing coordinate
    or image lookup shouldn't throw away an otherwise-good row. Each soft
    failure is noted in `warnings` and that field is just left blank.
    """
    row = {fn: "" for fn in DETAIL_FIELDNAMES}
    row["listing_id"] = listing_id
    row["url"] = LISTING_URL_TEMPLATE.format(id=listing_id)
    txn_rows = []
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
    except (
        Exception
    ) as exc:  # noqa: BLE001 -- hard failure, whole row is bad, will be retried
        row["scraped_ok"] = "0"
        row["error"] = str(exc)
        print(
            f"[error] listing {listing_id} failed (core details call): {exc}",
            file=sys.stderr,
        )
        return row, txn_rows

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

    # --- past transactions (soft-fail, needs postal + flatType) ---
    # NB: legitimately 404s for flats with no resale history at that address --
    # that's not a scrape error, just "no data", so we don't want it to look
    # like one in the warnings.
    flat_type = details.get("flatType")
    if postal and flat_type:
        try:
            past = call(PAST_TXN_URL, {"postal": postal, "flatType": flat_type}) or {}
            trans_list = past.get("listTrans") or []
            row["past_transaction_count"] = len(trans_list)
            for t in trans_list:
                txn_rows.append(
                    {
                        "listing_id": listing_id,
                        "street": t.get("street", "").strip(),
                        "block": t.get("block", ""),
                        "range": t.get("range", ""),
                        "floor_area_sqm": t.get("floorArea", ""),
                        "model": t.get("model", ""),
                        "lease_comm": t.get("leaseComm", ""),
                        "lease_tenure_years": t.get("leaseTenure", ""),
                        "lease_tenure_months": t.get("leaseTenureMonth", ""),
                        "resale_price": t.get("resalePrice", ""),
                        "registration_date": t.get("registrationDate", ""),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            if "404" in str(exc):
                row["past_transaction_count"] = 0
            else:
                warnings.append(f"past_transactions: {exc}")

    row["scraped_ok"] = "1"
    row["warnings"] = "; ".join(warnings)
    return row, txn_rows


def main():
    parser = argparse.ArgumentParser(
        description="Scrape full details for each HDB resale listing_id."
    )
    parser.add_argument(
        "--input",
        default="hdb_resale_listings.csv",
        help="CSV from scrape_listings.py containing a listing_id column. Default: hdb_resale_listings.csv",
    )
    parser.add_argument(
        "--out",
        default="hdb_resale_details",
        help="Output file base name (without extension). Writes <out>.csv and <out>_transactions.csv",
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
        "--overwrite",
        action="store_true",
        help="Ignore any existing output CSV and start fresh instead of resuming.",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Verbose per-request logging to stderr."
    )
    args = parser.parse_args()

    detail_path = f"{args.out}.csv"
    txn_path = f"{args.out}_transactions.csv"

    ids = load_listing_ids(args.input)
    print(f"Loaded {len(ids)} listing_id(s) from {args.input}")

    if args.limit:
        ids = ids[: args.limit]
        print(f"--limit set: processing only first {len(ids)}")

    done_ids = set() if args.overwrite else None
    kept_detail_rows, kept_txn_rows = [], []
    if not args.overwrite:
        kept_detail_rows, done_ids = load_successful_rows(detail_path)
        kept_txn_rows = load_kept_transactions(txn_path, done_ids)
    if done_ids:
        print(
            f"Resuming: {len(done_ids)} listing(s) already succeeded in {detail_path}, will skip those."
        )

    todo = [i for i in ids if i not in done_ids]
    print(
        f"{len(todo)} listing(s) to fetch (includes retries of any previously-failed ones)."
    )

    session = new_session()

    with open(detail_path, "w", newline="", encoding="utf-8") as df, open(
        txn_path, "w", newline="", encoding="utf-8"
    ) as tf:
        detail_writer = csv.DictWriter(df, fieldnames=DETAIL_FIELDNAMES)
        txn_writer = csv.DictWriter(tf, fieldnames=TXN_FIELDNAMES)
        detail_writer.writeheader()
        txn_writer.writeheader()

        # Re-seed previously successful rows first (resume case).
        for r in kept_detail_rows:
            detail_writer.writerow(r)
        for r in kept_txn_rows:
            txn_writer.writerow(r)
        df.flush()
        tf.flush()

        ok_count = 0
        err_count = 0
        for i, listing_id in enumerate(todo, 1):
            row, txn_rows = fetch_listing_detail(session, listing_id, debug=args.debug)
            detail_writer.writerow(row)
            for t in txn_rows:
                txn_writer.writerow(t)
            df.flush()
            tf.flush()

            if row["scraped_ok"] == "1":
                ok_count += 1
            else:
                err_count += 1

            if i % 25 == 0 or i == len(todo):
                print(
                    f"[{i}/{len(todo)}] ok={ok_count} err={err_count} (last: {listing_id})"
                )

            if args.delay and i < len(todo):
                time.sleep(args.delay)

    print(f"Done. Wrote details -> {detail_path}, transactions -> {txn_path}")
    if err_count:
        print(
            f"{err_count} listing(s) failed -- rerun the same command (without --overwrite) "
            f"to retry just those, since already-succeeded listings are skipped.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
