import argparse
import bs4
import csv
import googlemaps
import logging
import os
import re
import requests
import util

WIKIPEDIA_LIST_OF_MRT_STATIONS_URL = (
    "https://en.wikipedia.org/wiki/List_of_Singapore_MRT_stations"
)


# N/B: Wikipedia stores the list of MRT station names in 'tables',
# each with a link to the dedicated page for the MRT station,
# with italicized names being 'future' stations (and are thus excluded)
def _get_all_mrt_station_names():
    logger.debug(f"Getting all MRT station names from Wikipedia")
    response = requests.get(WIKIPEDIA_LIST_OF_MRT_STATIONS_URL)
    assert response.status_code == 200
    html_soup = bs4.BeautifulSoup(response.text, "html.parser")
    all_mrt_station_names = set()
    all_tables = html_soup.find_all("table", class_="wikitable sortable")
    for table in all_tables:
        links_in_table = table.find_all("a", href=True)
        for link in links_in_table:
            link_href = link["href"]
            if not re.search(
                pattern=r"_MRT_station$", string=link_href, flags=re.IGNORECASE
            ) and not re.search(
                pattern=r"_MRT/LRT_station$", string=link_href, flags=re.IGNORECASE
            ):
                continue
            if link.find("i") is not None:
                continue
            assert link_href.startswith("/wiki/")
            mrt_station_name = link_href[len("/wiki/") :].replace("_", " ")
            all_mrt_station_names.add(mrt_station_name)
    logger.info(f"Obtained {len(all_mrt_station_names)} MRT station names!")
    return all_mrt_station_names


def _precompute_mrt_station_map(all_mrt_station_names):
    logger.debug(f"Precomputing MRT station map with latitudes and longitudes")
    gmaps = util.get_gmaps_client()
    with open(
        os.path.join(util.OUTPUT_FOLDER, util.PRECOMPUTE_FILENAME),
        "w",
        encoding="utf-8",
    ) as csvfile:
        writer = csv.writer(csvfile)
        for mrt_station_name in all_mrt_station_names:
            lat_lon = util.get_lat_lon_from_address_throwing(
                gmaps=gmaps, address=f"{mrt_station_name}, Singapore"
            )
            writer.writerow([mrt_station_name, *lat_lon])
    logger.info(
        f"Precomputed MRT station map with {len(all_mrt_station_names)} latitudes and longitudes"
    )
    return


def main():
    parser = argparse.ArgumentParser(description="HDB Scraper")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logger.setLevel(args.log_level)

    all_mrt_station_names = _get_all_mrt_station_names()
    util.maybe_create_output_folder()
    mrt_station_map = _precompute_mrt_station_map(
        all_mrt_station_names=all_mrt_station_names
    )


if __name__ == "__main__":
    logger = logging.getLogger("HDB Scraper")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()
