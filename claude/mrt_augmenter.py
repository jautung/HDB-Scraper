#!/usr/bin/env python3
"""
Read a details CSV (from scrape_details.py) and compute/augment the MRT-related
columns only:
  - `nearest_mrt_station`
  - `walking_duration_mins`
  - `straight_line_distance_km`
  - `walking_distance_km`

Behaviour:
- Default input: `claude/hdb_common.DEFAULT_DETAILS_BASE + '.csv'`.
- Only process up to `--limit` rows if provided.
- Only fills these four columns when they are blank; leaves other columns untouched.
- Writes updates in-place (writes to a temp file then replaces original).

This copies the logic from the deprecated pipeline: uses a precomputed
`output/mrt_lat_lon.csv` for MRT station lat/lon, uses Google Maps for
geocoding the postal code and the walking distance matrix. Requires
`GOOGLE_MAPS_API_KEY` in env.
"""

import argparse
import csv
import logging
import os
import sys
import tempfile

from hdb_common import DEFAULT_DETAILS_BASE
import gmaps_util

# Precompute CSV location (in this directory's output/)
PRECOMPUTE_PATH = os.path.join(os.path.dirname(__file__), "output", "mrt_lat_lon.csv")

MRT_FIELDS = [
    "nearest_mrt_station",
    "walking_duration_mins",
    "straight_line_distance_km",
    "walking_distance_km",
]


def load_mrt_map(path):
    mmap = {}
    if not os.path.exists(path):
        raise FileNotFoundError(f"MRT precompute file not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            name = row[0]
            try:
                lat = float(row[1])
                lon = float(row[2])
            except Exception:
                continue
            mmap[name] = (lat, lon)
    return mmap


def compute_nearest_mrt_info(postal, gmaps, mrt_map):
    # Return tuple: (nearest_name, walking_duration_mins_or_None, straight_km, walking_km_or_None)
    try:
        postal_address = f"{postal}, Singapore"
        postal_lat, postal_lon = gmaps_util.get_lat_lon_from_address(
            gmaps=gmaps, address=postal_address
        )
    except Exception:
        return None, None, None, None

    # compute straight-line distances
    distances = []
    for mrt_name, (m_lat, m_lon) in mrt_map.items():
        d = gmaps_util.haversine_distance_km(postal_lat, postal_lon, m_lat, m_lon)
        distances.append((mrt_name, d))
    if not distances:
        return None, None, None, None
    nearest_name, nearest_dist_km = min(distances, key=lambda x: x[1])

    # attempt walking distance/duration via Google Distance Matrix
    try:
        distance_metres, duration_seconds = (
            gmaps_util.get_walking_distance_and_duration(
                gmaps=gmaps, start=postal_address, end=nearest_name
            )
        )
        if distance_metres is not None and duration_seconds is not None:
            walking_km = distance_metres / 1000.0
            walking_mins = duration_seconds / 60.0
        else:
            walking_km = None
            walking_mins = None
    except Exception:
        walking_km = None
        walking_mins = None

    return nearest_name, walking_mins, nearest_dist_km, walking_km


def main():
    parser = argparse.ArgumentParser(description="Augment details CSV with MRT info")
    parser.add_argument(
        "--input",
        default=f"{DEFAULT_DETAILS_BASE}.csv",
        help="Input details CSV (default: claude/hdb_resale_details.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process first N rows (useful for testing)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Verbose debugging prints",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s (%(name)s) [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    input_path = args.input
    if not os.path.exists(input_path):
        logger.error("Input file not found: %s", input_path)
        sys.exit(2)

    mrt_map = load_mrt_map(PRECOMPUTE_PATH)
    gmaps = gmaps_util.get_gmaps_client()

    # Read input rows while preserving field order
    with open(input_path, newline="", encoding="utf-8") as inf:
        reader = csv.DictReader(inf)
        fieldnames = reader.fieldnames or []
        rows = []
        for i, row in enumerate(reader):
            if args.limit is not None and i >= args.limit:
                break
            rows.append(row)

    logger.debug("Loaded %d rows to process from %s", len(rows), input_path)

    # Process and update only MRT_FIELDS
    updated = 0
    for idx, row in enumerate(rows):
        # Only fill when blank (empty string or missing)
        needs = any((not (row.get(f) or "")) for f in MRT_FIELDS)
        if not needs:
            continue
        postal = (row.get("postal") or "").strip()
        if not postal:
            # cannot compute
            continue
        nearest, walk_mins, straight_km, walk_km = compute_nearest_mrt_info(
            postal, gmaps, mrt_map
        )
        if nearest is not None:
            row["nearest_mrt_station"] = nearest
        if walk_mins is not None:
            row["walking_duration_mins"] = f"{walk_mins:.2f}"
        if straight_km is not None:
            row["straight_line_distance_km"] = f"{straight_km:.6f}"
        if walk_km is not None:
            row["walking_distance_km"] = f"{walk_km:.3f}"
        updated += 1
        if args.debug and (idx % 25 == 0):
            logger.debug("Processed %d/%d rows", idx + 1, len(rows))

    logger.info("Updated %d rows. Writing back to %s", updated, input_path)

    # Write to temp file and replace original
    dirn = os.path.dirname(input_path) or "."
    fd, tmp_path = tempfile.mkstemp(prefix="mrt_augment_", dir=dirn)
    os.close(fd)
    try:
        with open(tmp_path, "w", newline="", encoding="utf-8") as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()
            # We only processed first N rows; need to re-read full input and stream through
            with open(input_path, newline="", encoding="utf-8") as inf:
                reader = csv.DictReader(inf)
                for i, orig in enumerate(reader):
                    if i < len(rows):
                        # use possibly-updated row
                        writer.writerow(rows[i])
                    else:
                        writer.writerow(orig)
        os.replace(tmp_path, input_path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    logger.info("MRT augmentation complete. Updated %d rows in %s", updated, input_path)


if __name__ == "__main__":
    main()
