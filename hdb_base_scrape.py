# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import logging
import os
import bs4
import browser_util
import file_util

logger = logging.getLogger("HDB Scraper: HDB Listing Pages")


async def _scrape_listings():
    already_processed_urls = _get_already_processed_urls()
    with open(
        file_util.LISTINGS_FILENAME, newline="", encoding="utf-8"
    ) as listings_file, open(
        file_util.BASE_INFO_FILENAME, "w", newline="", encoding="utf-8"
    ) as base_info_file:
        reader = csv.DictReader(listings_file)
        writer = csv.DictWriter(base_info_file, fieldnames=["a"])
        writer.writeheader()

        for row in reader:
            row_id = row["Link"]

            if (
                row_id in already_processed_urls
                and already_processed_urls[row_id].get("result1")
                and already_processed_urls[row_id].get("result2")
            ):
                writer.writerow(already_processed_urls[row_id])
            else:
                try:
                    result = await _scrape_single_listing(row_id)
                    full_row = {"Link": row_id, **result}
                    writer.writerow(full_row)
                    print(f"Wrote: {full_row}")
                except Exception as e:
                    print(f"Error processing {row_id}: {e}")
                    writer.writerow({"Link": row_id, "result1": "", "result2": ""})

            base_info_file.flush()
            os.fsync(base_info_file.fileno())


async def _scrape_single_listing(row_id: str):
    await asyncio.sleep(0.1)
    return {
        "result1": row_id.upper(),
        "result2": len(row_id),
    }


def _get_already_processed_urls():
    if not os.path.exists(file_util.BASE_INFO_FILENAME):
        return set()
    with open(
        file_util.BASE_INFO_FILENAME, newline="", encoding="utf-8"
    ) as base_info_file:
        reader = csv.DictReader(base_info_file)
        return {row["Link"]: row for row in reader}


def main():
    parser = argparse.ArgumentParser(description="MRT Precompute")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logger.setLevel(args.log_level)

    file_util.maybe_create_output_folder()
    asyncio.run(_scrape_listings())


if __name__ == "__main__":
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()
