# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import bs4
import csv
import dataclasses
import datetime
import logging
import math
import re
import typing
import browser_util
import file_util

OUTPUT_FILENAME = "listings.csv"  # Default
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5  # Default
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3  # Default
SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60  # Default
DELAY_PER_LISTING_LOAD_SECONDS = 0  # Default
DELAY_PER_LISTING_FIRST_SECOND_RETRY_LOAD_SECONDS = 30  # Default
DELAY_PER_LISTING_SUBSEQUENT_RETRIES_LOAD_SECONDS = 60  # Default
MAX_NUMBER_OF_PAGES = None  # Default

################################################################
# BROWSER + RENDER HTML START
################################################################


def _get_single_rendered_html_browser_page_callback(
    selector_to_wait_for=None, additional_action=None
):
    async def _callback(page, debug_logging_name, current_attempt):
        # Artificially make this slower so HDB doesn't block us... :)
        if current_attempt == 1:
            await asyncio.sleep(DELAY_PER_LISTING_LOAD_SECONDS)
        elif current_attempt == 2 or current_attempt == 3:
            await asyncio.sleep(DELAY_PER_LISTING_FIRST_SECOND_RETRY_LOAD_SECONDS)
        else:
            await asyncio.sleep(DELAY_PER_LISTING_SUBSEQUENT_RETRIES_LOAD_SECONDS)

        if selector_to_wait_for is not None:
            logger.debug(
                f"Waiting for selector {selector_to_wait_for} of {debug_logging_name}"
            )
            await page.waitForSelector(selector_to_wait_for)

        if additional_action is not None:
            await additional_action(page=page, debug_logging_name=debug_logging_name)

        logger.debug(f"Extracting rendered HTML from {debug_logging_name}")
        html = await page.content()
        logger.debug(f"Successfully extracted rendered HTML from {debug_logging_name}")
        return html

    return _callback


async def _click_expand_all_button(page, debug_logging_name):
    logger.debug(f"Waiting for 'Expand/Collapse all' button of {debug_logging_name}")
    await page.waitForSelector(".btn-secondary")
    logger.debug(f"Clicking on 'Expand/Collapse all' button of {debug_logging_name}")
    await page.click(".btn-secondary")


################################################################
# BROWSER + RENDER HTML END
################################################################

################################################################
# PARSE HTML START
################################################################


def _direct_text_contents(contents):
    return [content for content in contents if isinstance(content, str)]


def _find_simple_text(html_soup, tag_type, attr_regex, debug_logging_name):
    logger.debug(f"Starting to find simple text for {debug_logging_name}")
    elements = html_soup.find_all(tag_type)
    for element in elements:
        logger.debug(f"Checking element for {debug_logging_name}: {element}")
        if any([attr_regex.match(attr_keys) for attr_keys in element.attrs.keys()]):
            contents = element.contents
            logger.debug(f"Matched element for {debug_logging_name}! {contents}")
            return _direct_text_contents(contents=contents)
    logger.error(f"No matching elements for {debug_logging_name}!")
    return None


@dataclasses.dataclass
class HeaderInfo:
    address: typing.Any
    sub_address: typing.Any
    postal_code: typing.Any
    hdb_type: typing.Any
    area: typing.Any
    price: typing.Any


def _parse_postal_code_from_sub_address(sub_address):
    pattern = r"Singapore\s+(\d{6})"
    match = re.search(pattern=pattern, string=sub_address)
    if match:
        return match.group(1)
    logger.error(f"Could not parse postal code from sub-address {sub_address}")
    return None


def _parse_header_info(html_soup):
    logger.debug(f"*** Parsing header info ***")
    address = _find_simple_text(
        html_soup=html_soup,
        tag_type="h3",
        attr_regex=re.compile(pattern=r"_ngcontent-\w{3}-c7"),
        debug_logging_name="address",
    )
    sub_address = _find_simple_text(
        html_soup=html_soup,
        tag_type="h5",
        attr_regex=re.compile(pattern=r"_ngcontent-\w{3}-c7"),
        debug_logging_name="sub-address",
    )
    subtitle = _find_simple_text(
        html_soup=html_soup,
        tag_type="p",
        attr_regex=re.compile(pattern=r"_ngcontent-\w{3}-c7"),
        debug_logging_name="subtitle",
    )
    price = _find_simple_text(
        html_soup=html_soup,
        tag_type="h2",
        attr_regex=re.compile(pattern=r"_ngcontent-\w{3}-c7"),
        debug_logging_name="price",
    )
    assert len(address) == 1
    assert len(sub_address) == 1
    assert len(subtitle) == 2
    assert subtitle[1].endswith("sqm")
    assert len(price) == 1
    assert price[0].startswith("$")
    return HeaderInfo(
        address=address[0].strip(),
        sub_address=sub_address[0].strip(),
        postal_code=_parse_postal_code_from_sub_address(sub_address[0].strip()),
        hdb_type=subtitle[0].strip(),
        area=float(subtitle[1][: -len("sqm")].strip()),
        price=float(price[0][len("$") :].strip().replace(",", "")),
    )


@dataclasses.dataclass
class SingleDetail:
    key: typing.Any
    val: typing.Any


def _parse_single_detail(element):
    logger.debug(f"Parsing single detail: {element}")

    spans = element.find_all("span")
    assert len(spans) == 1
    assert len(spans[0].contents) == 1
    key = spans[0].contents[0].strip()

    paragraphs = element.find_all("p")
    assert len(paragraphs) > 0
    for paragraph in paragraphs:
        assert len(paragraph.contents) > 0
    val = ", ".join([paragraph.contents[-1].strip() for paragraph in paragraphs])

    logger.debug(f"Parsed single detail to be {key} = {val}")
    return SingleDetail(key=key, val=val)


@dataclasses.dataclass
class DetailsInfo:
    town: typing.Any
    storey_range: typing.Any
    remaining_lease: typing.Any
    remaining_lease_num_years: typing.Any
    num_bathrooms: typing.Any
    num_bedrooms: typing.Any
    balcony: typing.Any
    contra: typing.Any
    extension_of_stay: typing.Any
    upgrading: typing.Any
    ethnic_eligibility: typing.Any
    spr_eligibility: typing.Any
    description: typing.Any
    last_updated: typing.Any
    last_updated_date: typing.Any


def _find_from_details(details, key):
    for detail in details:
        if detail.key.lower() == key.lower():
            return detail.val.strip()
    logger.error(f"Could not find {key} in details")
    return None


def _parse_remaining_lease_num_years(remaining_lease):
    pattern = r"(\d+)\s+years(\s+(\d+)\s+months)?"
    match = re.search(pattern=pattern, string=remaining_lease, flags=re.IGNORECASE)
    if match:
        years = int(match.group(1))
        months = int(match.group(3)) if match.group(3) else 0
        return years + months / 12
    logger.error(f"Could not parse remaining lease {remaining_lease}")
    return None


def _parse_last_updated_date(last_updated):
    pattern = r"Last\s+updated:\s*(\d+)\s+([a-zA-Z]+)\s+(\d+)"
    matches = re.findall(pattern=pattern, string=last_updated, flags=re.IGNORECASE)
    assert len(matches) > 0
    last_updated_date = max(
        (
            datetime.datetime(
                year=int(year),
                month=datetime.datetime.strptime(month, "%B").month,
                day=int(day),
            )
            for day, month, year in matches
        )
    )
    return last_updated_date.strftime("%Y-%m-%d")


def _parse_details_info(html_soup):
    logger.debug(f"*** Parsing details info ***")
    content_element = html_soup.find(id="content")

    detail_elements = content_element.find_all(class_="col-6")
    details = [
        _parse_single_detail(detail_element) for detail_element in detail_elements
    ]

    description_elements = content_element.find_all(class_="col-10")
    assert len(description_elements) > 0
    description_elements = [
        nested_description_element
        for description_element in description_elements
        for nested_description_element in description_element.find_all(
            class_="ng-tns-c8-0 ng-star-inserted"
        )
    ]
    assert len(description_elements) > 0
    logger.debug(f"Parsing description elements: {description_elements}")
    description = "\n".join(
        [
            text_content
            for description_element in description_elements
            for text_content in _direct_text_contents(description_element)
        ]
    )

    last_updated_elements = content_element.find_all(class_="description-last-updated")
    assert len(last_updated_elements) > 0
    logger.debug(f"Parsing last updated elements: {last_updated_elements}")
    last_updated = ", ".join(
        [
            text_content
            for last_updated_element in last_updated_elements
            for text_content in _direct_text_contents(last_updated_element)
        ]
    )

    return DetailsInfo(
        town=_find_from_details(details=details, key="Town"),
        storey_range=_find_from_details(details=details, key="Storey range"),
        remaining_lease=_find_from_details(details=details, key="Remaining lease"),
        remaining_lease_num_years=_parse_remaining_lease_num_years(
            _find_from_details(details=details, key="Remaining lease")
        ),
        num_bathrooms=int(
            _find_from_details(details=details, key="Number of bathrooms")
        ),
        num_bedrooms=int(_find_from_details(details=details, key="Number of bedrooms")),
        balcony=_find_from_details(details=details, key="Balcony"),
        contra=_find_from_details(details=details, key="Contra"),
        extension_of_stay=_find_from_details(details=details, key="Extension of stay"),
        upgrading=_find_from_details(details=details, key="Upgrading"),
        ethnic_eligibility=_find_from_details(
            details=details, key="Ethnic eligibility"
        ),
        spr_eligibility=_find_from_details(details=details, key="SPR eligibility"),
        description=description,
        last_updated=last_updated,
        last_updated_date=_parse_last_updated_date(last_updated=last_updated),
    )


@dataclasses.dataclass
class ListingInfo:
    index_of_listing: typing.Any
    listing_url: typing.Any
    header_info: typing.Any
    details_info: typing.Any
    nearest_mrt_info: typing.Any


async def _scrape_single_listing(listing_url, index_of_listing, num_listings):
    debug_logging_name = (
        f"{listing_url} (listing #{index_of_listing+1} of {num_listings})"
    )
    logger.info(f"Starting to scrape {debug_logging_name}")

    logger.debug(f"Getting rendered HTML of {debug_logging_name}")
    html = await browser_util.run_with_browser_page_for_url(
        url=listing_url,
        callback_on_page=_get_single_rendered_html_browser_page_callback(
            # N/B: any 'h3' tag is a simple heuristic to determine that the Angular-rendered web page has loaded
            selector_to_wait_for="h3",
            additional_action=_click_expand_all_button,
        ),
        debug_logging_name=debug_logging_name,
    )
    if html is None:
        return None

    logger.debug(f"Parsing HTML of {debug_logging_name}")
    html_soup = bs4.BeautifulSoup(html, "html.parser")

    header_info = _parse_header_info(html_soup=html_soup)
    details_info = _parse_details_info(html_soup=html_soup)

    logger.info(f"Finished scraping {debug_logging_name}")
    return ListingInfo(
        index_of_listing=index_of_listing,
        listing_url=listing_url,
        header_info=header_info,
        details_info=details_info,
        nearest_mrt_info=None,
    )


################################################################
# PARSE HTML END
################################################################

################################################################
# GOOGLE MAPS START
################################################################


# Based on great-circle distance
def _haversine_distance_km(lat1, lon1, lat2, lon2):
    R = 6371.0  # Radius of the Earth in kilometers
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@dataclasses.dataclass
class NearestMRTInfo:
    nearest_mrt_station: typing.Any
    straight_line_distance_km: typing.Any
    walking_distance_km: typing.Any
    walking_duration_mins: typing.Any


def _nearest_mrt_info(postal_code, gmaps, mrt_station_map):
    logger.debug(f"Finding nearest MRT info for 'S{postal_code}'")
    postal_code_address = f"{postal_code}, Singapore"
    postal_code_lat, postal_code_lon = file_util.get_lat_lon_from_address(
        gmaps=gmaps, address=postal_code_address
    )
    mrt_station_distances_km = [
        (
            mrt_station,
            _haversine_distance_km(
                lat1=postal_code_lat,
                lon1=postal_code_lon,
                lat2=mrt_station_lat,
                lon2=mrt_station_lon,
            ),
        )
        for mrt_station, (mrt_station_lat, mrt_station_lon) in mrt_station_map.items()
    ]
    nearest_mrt_station, nearest_mrt_station_distance_km = min(
        mrt_station_distances_km, key=lambda x: x[1]
    )
    logger.debug(
        f"Computed that closest MRT to 'S{postal_code}' is {nearest_mrt_station}"
    )
    gmaps_result = gmaps.distance_matrix(
        origins=[postal_code_address],
        destinations=[nearest_mrt_station],
        mode="walking",
    )
    gmaps_result_inner = gmaps_result["rows"][0]["elements"][0]
    distance_metres = gmaps_result_inner["distance"]["value"]
    duration_seconds = gmaps_result_inner["duration"]["value"]
    logger.debug(
        f"Google Maps says that 'S{postal_code}' to {nearest_mrt_station} takes {(duration_seconds / 60):.2f}mins"
    )
    return NearestMRTInfo(
        nearest_mrt_station=nearest_mrt_station,
        straight_line_distance_km=nearest_mrt_station_distance_km,
        walking_distance_km=distance_metres / 1000,
        walking_duration_mins=duration_seconds / 60,
    )


CACHED_NEAREST_MRT_INFO = dict()


def _get_nearest_mrt_info_for_listing(listing, gmaps, mrt_station_map):
    postal_code = listing.header_info.postal_code
    if postal_code in CACHED_NEAREST_MRT_INFO:
        logger.debug(f"Hit cache for nearest MRT info for 'S{postal_code}'!")
        return CACHED_NEAREST_MRT_INFO[postal_code]
    logger.debug(f"Missed cache for nearest MRT info for 'S{postal_code}'")
    nearest_mrt_info = _nearest_mrt_info(
        postal_code=listing.header_info.postal_code,
        gmaps=gmaps,
        mrt_station_map=mrt_station_map,
    )
    CACHED_NEAREST_MRT_INFO[postal_code] = nearest_mrt_info
    return nearest_mrt_info


def _augment_listings_with_mrt_info(listings, gmaps, mrt_station_map):
    logger.info(f"Augmenting {len(listings)} listings with MRT info")
    for listing in listings:
        if listing is None:
            continue
        listing.nearest_mrt_info = _get_nearest_mrt_info_for_listing(
            listing=listing, gmaps=gmaps, mrt_station_map=mrt_station_map
        )


################################################################
# GOOGLE MAPS END
################################################################

################################################################
# EXPORT START
################################################################


def _export_to_csv(listings):
    num_written = 0
    with open(OUTPUT_FILENAME, "w+", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                # Key info
                "Link",
                "Address",
                "Postal code",
                "HDB type",
                "Ethnic eligibility",
                "Area (sqm)",
                "Price ($)",
                "Storey range",
                "Remaining lease (years)",
                "Nearest MRT station",
                "Walking duration to MRT (mins)",
                "Last updated date",
                "Free-form description (provided by seller)",
                # Useful info
                "Number of bedrooms",
                "Number of bathrooms",
                "Balcony",
                "Upcoming upgrading plans?",
                # Fallback scraped info
                "Sub-address [fallback if postal code is 'None']",
                "Town [fallback if nearest MRT station is 'None']",
                "Remaining lease [fallback if parsed remaining lease is 'None']",
                "Last updated [fallback if last updated date is 'None']",
                "Straight line distance to MRT (km) [fallback if MRT duration is 'None']",
                "Walking distance to MRT (km) [fallback if MRT duration is 'None']",
                # Mostly irrelevant info (for us)
                "Will seller want to extend their stay (up to 3 months)? [less relevant for us]",
                "Enhanced Contra Facility (ECF) Allowed? [irrelevant for us]",
                "SPR eligibility [irrelevant for us]",
            ]
        )
        for listing in listings:
            if listing is None or isinstance(listing, Exception):
                logger.warning(f"Skipping a scraped listing {listing}")
                continue
            writer.writerow(
                [
                    # Key info
                    listing.listing_url,
                    listing.header_info.address,
                    listing.header_info.postal_code,
                    listing.header_info.hdb_type,
                    listing.details_info.ethnic_eligibility,
                    listing.header_info.area,
                    listing.header_info.price,
                    listing.details_info.storey_range,
                    listing.details_info.remaining_lease_num_years,
                    listing.nearest_mrt_info.nearest_mrt_station
                    if listing.nearest_mrt_info is not None
                    else None,
                    listing.nearest_mrt_info.walking_duration_mins
                    if listing.nearest_mrt_info is not None
                    else None,
                    listing.details_info.last_updated_date,
                    listing.details_info.description,
                    # Useful info
                    listing.details_info.num_bedrooms,
                    listing.details_info.num_bathrooms,
                    listing.details_info.balcony,
                    listing.details_info.upgrading,
                    # Fallback scraped info
                    listing.header_info.sub_address,
                    listing.details_info.town,
                    listing.details_info.remaining_lease,
                    listing.details_info.last_updated,
                    listing.nearest_mrt_info.straight_line_distance_km
                    if listing.nearest_mrt_info is not None
                    else None,
                    listing.nearest_mrt_info.walking_distance_km
                    if listing.nearest_mrt_info is not None
                    else None,
                    # Mostly irrelevant info (for us)
                    listing.details_info.extension_of_stay,
                    listing.details_info.contra,
                    listing.details_info.spr_eligibility,
                ]
            )
            num_written += 1

    logger.info(
        f"Successfully exported {num_written} scraped results to {OUTPUT_FILENAME}"
    )


################################################################
# EXPORT END
################################################################

################################################################
# MAIN START
################################################################


async def _scrape_listings(listing_urls):
    all_listings = []
    for index, listing_url in enumerate(listing_urls):
        listing = await _scrape_single_listing(
            listing_url=listing_url,
            index_of_listing=index,
            num_listings=len(listing_urls),
        )
        all_listings.append(listing)
    return all_listings


async def _main_scrape_all(gmaps, mrt_station_map):
    listing_urls = await _get_listing_urls()
    scraped_listings = await _scrape_listings(listing_urls=listing_urls)
    # There should be a better place to put this, but leaking the abstraction for now
    if BROWSER is not None:
        logger.debug(f"Closing the global browser")
        await BROWSER.close()
    else:
        logger.debug(f"No global browser to close")
    _augment_listings_with_mrt_info(
        listings=scraped_listings, gmaps=gmaps, mrt_station_map=mrt_station_map
    )
    _export_to_csv(listings=scraped_listings)


def _validate_csv_filename(value):
    if not value.endswith(".csv"):
        raise argparse.ArgumentTypeError(
            f"Filename must end with '.csv'; you provided '{value}'"
        )
    return value


def main():
    parser = argparse.ArgumentParser(description="HDB Scraper")
    parser.add_argument(
        "--output_filename",
        type=_validate_csv_filename,
        default="listings.csv",
        help="Output file name (must end with .csv)",
    )
    parser.add_argument(
        "--max_attempts_for_network_error",
        type=int,
        default=5,
        help="Maximum number of attempts to retry on network errors (default: 5)",
    )
    parser.add_argument(
        "--max_attempts_for_other_error",
        type=int,
        default=3,
        help="Maximum number of attempts to retry on other errors (default: 3)",
    )
    parser.add_argument(
        "--single_browser_run_timeout_seconds",
        type=int,
        default=5 * 60,
        help="Overall timeout for loading each listing, in seconds (default: 5 * 60)",
    )
    parser.add_argument(
        "--delay_per_listing_load",
        type=int,
        default=0,
        help="Delay before loading each listing page, in seconds (default: 0)",
    )
    parser.add_argument(
        "--delay_per_listing_first_second_retry_load",
        type=int,
        default=30,
        help="Delay before loading each listing page for the first and second retries, in seconds (default: 30)",
    )
    parser.add_argument(
        "--delay_per_listing_subsequent_retries_load",
        type=int,
        default=60,
        help="Delay before loading each listing page for all subsequent retries, in seconds (default: 60)",
    )
    parser.add_argument(
        "--max_number_of_pages",
        type=int,
        help="Maximum number of pages of listings to scrape, with each page containing 20 listings (default: unlimited)",
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()

    global OUTPUT_FILENAME
    OUTPUT_FILENAME = args.output_filename

    global MAX_ATTEMPTS_FOR_NETWORK_ERROR
    MAX_ATTEMPTS_FOR_NETWORK_ERROR = args.max_attempts_for_network_error

    global MAX_ATTEMPTS_FOR_OTHER_ERROR
    MAX_ATTEMPTS_FOR_OTHER_ERROR = args.max_attempts_for_other_error

    global SINGLE_BROWSER_RUN_TIMEOUT_SECONDS
    SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = args.single_browser_run_timeout_seconds

    global DELAY_PER_LISTING_LOAD_SECONDS
    DELAY_PER_LISTING_LOAD_SECONDS = args.delay_per_listing_load

    global DELAY_PER_LISTING_FIRST_SECOND_RETRY_LOAD_SECONDS
    DELAY_PER_LISTING_FIRST_SECOND_RETRY_LOAD_SECONDS = (
        args.delay_per_listing_first_second_retry_load
    )

    global DELAY_PER_LISTING_SUBSEQUENT_RETRIES_LOAD_SECONDS
    DELAY_PER_LISTING_SUBSEQUENT_RETRIES_LOAD_SECONDS = (
        args.delay_per_listing_subsequent_retries_load
    )

    global MAX_NUMBER_OF_PAGES
    MAX_NUMBER_OF_PAGES = args.max_number_of_pages

    logger.setLevel(args.log_level)

    gmaps = file_util.get_gmaps_client()
    # asyncio.run(_main_scrape_all(gmaps=gmaps, mrt_station_map=mrt_station_map))


if __name__ == "__main__":
    logger = logging.getLogger("HDB Scraper")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    logger.addHandler(handler)

    main()

################################################################
# MAIN END
################################################################
