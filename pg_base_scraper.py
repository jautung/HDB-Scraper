# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import argparse
import asyncio
import csv
import dataclasses
import json
import logging
import os
import re
import typing
import bs4
import browser_util
import file_util

SINGLE_BROWSER_RUN_TIMEOUT_SECONDS = 5 * 60
DELAY_PER_LISTING_LOAD_SECONDS = 1
RETRY_DELAY_SECONDS = 5
MAX_ATTEMPTS_FOR_NETWORK_ERROR = 5
MAX_ATTEMPTS_FOR_OTHER_ERROR = 3
MRT_DISTANCE_PATTERN = r"^([\d.]+) (m|km) \((\d+) mins\) from ([A-Z]+\d+) .+$"
logger = logging.getLogger(__name__)


async def _xcxc_scrape_all():
    browser = browser_util.BrowserUtil(
        single_browser_run_timeout_seconds=SINGLE_BROWSER_RUN_TIMEOUT_SECONDS,
        retry_delay_seconds=RETRY_DELAY_SECONDS,
        max_attempts_for_network_error=MAX_ATTEMPTS_FOR_NETWORK_ERROR,
        max_attempts_for_other_error=MAX_ATTEMPTS_FOR_OTHER_ERROR,
        user_agent=browser_util.FAKE_USER_AGENT,
    )
    await _scrape_single_listing(
        listing_url="https://www.propertyguru.com.sg/listing/60034673", browser=browser
    )
    await _scrape_single_listing(
        listing_url="https://www.propertyguru.com.sg/listing/25539219", browser=browser
    )
    await _scrape_single_listing(
        listing_url="https://www.propertyguru.com.sg/listing/25559652", browser=browser
    )


async def _scrape_single_listing(listing_url, browser):
    logger.info(f"Starting to scrape {listing_url}")
    html = await browser.run_with_browser_page_for_url(
        url=listing_url,
        callback_on_page=browser_util.get_single_rendered_html_browser_page_callback(),
        debug_logging_name=listing_url,
        wait_until="domcontentloaded",
    )
    html_soup = bs4.BeautifulSoup(html, "html.parser")

    # Somewhat helpfully, this element already contains all the semantic data that is used
    # to populate the UI of the website, in a huge JSON blob. Unclear if this is intended/secure,
    # but I'll take it! :)
    script_data_element = html_soup.find(
        "script", {"id": "__NEXT_DATA__", "type": "application/json"}
    )
    if script_data_element is None:
        print("Script data tag not found")
        print("Contains verifying?", "Verifying" in html)
        print("Contains waiting?", "Just a moment..." in html)
        # Probably just retry if we hit this...
        print(html)
        return None
    json_data = script_data_element.string
    try:
        data = json.loads(json_data)
        main_data = data["props"]["pageProps"]["pageData"]["data"]

        overview_data = main_data["propertyOverviewData"]
        print("verifiedListingBadge", overview_data["verifiedListingBadge"])
        header_info = overview_data["propertyInfo"]
        title = header_info["title"]
        address = header_info["fullAddress"]
        price_info = header_info["price"]
        price_amount = text_to_price(price_info["amount"])
        price_type = price_info["priceType"]
        print(price_type)
        header_info_2 = header_info["amenities"]
        # Probably parse this better
        # print(header_info_2)
        assert len(header_info_2) == 3
        assert header_info_2[0]["iconSrc"] == "bed-o"
        num_bedrooms = header_info_2[0]["value"]
        assert header_info_2[1]["iconSrc"] == "bath-o"
        num_bathrooms = header_info_2[1]["value"]
        assert header_info_2[2]["iconSrc"] == "ruler-o"
        num_sqft = header_info_2[2]["value"]

        location_data = main_data["listingLocationData"]["data"]
        location_lat = location_data["center"]["lat"]
        location_lon = location_data["center"]["lng"]
        # Need to find the first one that is not 'isFuture'
        for nm in location_data["nearestMRTs"]:
            print(
                nm["id"],
                nm["isFutureLine"],
                nm["distance"]["value"],
                "m",
                nm["duration"]["value"],
                "seconds",
            )

        details_data = main_data["detailsData"]["metatable"]["items"]
        # Assume this is always in the right order?
        print([(d["icon"], d["value"]) for d in details_data])

        description_data = main_data["descriptionBlockData"]
        description_subtitle = description_data["subtitle"]
        description_details = description_data["description"]

        # listingData
        listing_data = main_data["listingData"]
        # This also has a lot of fields of the above stuff, maybe we should've been using this instead
        listing_data["price"]
        listing_data["propertyName"]
        listing_data["localizedTitle"]
        listing_data["bedrooms"]
        listing_data["bathrooms"]
        listing_data["floorArea"]
        listing_data["postcode"]
        listing_data["districtCode"]
        listing_data["regionCode"]
        listing_data["tenure"]
        listing_data["hdbTypeCode"]
        listing_data["hdbEstateText"]
        listing_data["streetName"]
        listing_data["lastPosted"]["unix"]

        # Maybe agent can be None? unclear...
        listing_data["agent"]["name"]
        main_data["contactAgentData"]["contactAgentCard"]["agency"]["name"]
        main_data["contactAgentData"]["contactAgentCard"]["agentInfoProps"]["agent"][
            "name"
        ]
        main_data["contactAgentData"]["contactAgentCard"]["agentInfoProps"]["agent"][
            "avatar"
        ]
        # Needs https://www.propertyguru.com.sg/listing/ prefix again
        main_data["contactAgentData"]["contactAgentCard"]["agentInfoProps"]["agent"][
            "profileUrl"
        ]

        # EXTRA STUFF BELOW HERE MIGHT AS WELL SINCE IT IS SO EASY

        amenities_data = main_data["amenitiesData"]["data"]
        # Maybe dedupe this?
        print("amenities", [a["text"] for a in amenities_data])

        main_image = main_data["metadata"]["metaTags"]["openGraph"]["image"]
        print("main_image", main_image)

        media_data = main_data["mediaGalleryData"]["media"]
        image_links = [i["src"] for i in media_data["images"]["items"]]
        floor_plan_links = [i["src"] for i in media_data["floorPlans"]["items"]]
        # Should definitely de-dupe these images
        media_explorer_data = main_data["mediaExplorerData"]["mediaGroups"]
        image_links_2 = [i["src"] for i in media_explorer_data["images"]["items"]]
        floor_plan_links_2 = [
            i["src"] for i in media_explorer_data["floorPlans"]["items"]
        ]

        faq_data = main_data["faqData"]["list"]
        faq_info = "\n\n".join([f["question"] + "\n" + f["answer"] for f in faq_data])
        # print(faq_info)

    except (json.JSONDecodeError, KeyError) as e:
        print("Failed to parse JSON content from script data tag")
        print(e)
        return None

    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="h1",
    #         da_id="property-title",
    #         debug_logging_name="property-title",
    #         transform=get_text_transform,
    #     )
    # )
    # if (
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="h1",
    #         da_id="property-title",
    #         debug_logging_name="property-title",
    #         transform=get_text_transform,
    #     )
    #     is None
    # ):
    #     print(html)
    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="p",
    #         da_id="property-address",
    #         debug_logging_name="property-address",
    #         transform=get_text_transform,
    #     )
    # )
    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="h2",
    #         da_id="price-amount",
    #         debug_logging_name="price-amount",
    #         transform=get_price_transform,
    #     )
    # )
    # # This one is either "Negotiable" or None
    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="span",
    #         da_id="price-type",
    #         debug_logging_name="price-type",
    #         transform=get_text_transform,
    #     )
    # )

    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="div",
    #         da_id="bedroom-amenity",
    #         debug_logging_name="bedroom-amenity",
    #         transform=get_amenity_num_transform,
    #     )
    # )
    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="div",
    #         da_id="bathroom-amenity",
    #         debug_logging_name="bathroom-amenity",
    #         transform=get_amenity_num_transform,
    #     )
    # )
    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="div",
    #         da_id="area-amenity",
    #         debug_logging_name="area-amenity",
    #         transform=get_amenity_num_transform,
    #     )
    # )
    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="div",
    #         da_id="psf-amenity",
    #         debug_logging_name="psf-amenity",
    #         transform=get_amenity_price_transform,
    #     )
    # )

    # print(
    #     _find_element(
    #         html_soup=html_soup,
    #         tag_name="p",
    #         da_id="mrt-distance-text",
    #         debug_logging_name="mrt-distance-text",
    #         transform=get_mrt_distance_text_transform,
    #     )
    # )


def _find_element(html_soup, tag_name, da_id, debug_logging_name, transform=None):
    logger.debug(f"Starting to find simple text for {debug_logging_name}")
    elements = html_soup.find_all(name=tag_name, attrs={"da-id": da_id})
    if len(elements) != 1:
        return None
    if transform is None:
        return elements[0]
    return transform(elements[0])


def get_text_transform(element):
    return element.get_text(strip=True)


def get_price_transform(element):
    return text_to_price(get_text_transform(element))


def get_amenity_num_transform(element):
    # Find the first <p> tag contained within
    first_p_element = element.find(name="p")
    if first_p_element is None:
        return None
    return text_to_num(get_text_transform(first_p_element))


def get_amenity_price_transform(element):
    # Find the first <p> tag contained within
    first_p_element = element.find(name="p")
    if first_p_element is None:
        return None
    return text_to_price(get_text_transform(first_p_element))


@dataclasses.dataclass
class MrtDistanceInfo:
    distance_metres: typing.Any
    time_minutes: typing.Any
    station_code: typing.Any


def get_mrt_distance_text_transform(element):
    text = get_text_transform(element)
    match = re.search(MRT_DISTANCE_PATTERN, text)
    if match is None:
        return None
    return MrtDistanceInfo(
        distance_metres=match.group(1)
        if match.group(2) == "m"
        else match.group(1) * 1000,
        time_minutes=match.group(3),
        station_code=match.group(4),
    )


def text_to_price(text):
    if not text.startswith("S$ "):
        return None
    return text_to_num(text[3:])


def text_to_num(text):
    try:
        return int(text.replace(",", ""))
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(description="HDB Base Scraper")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s (%(name)s) [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    file_util.maybe_create_output_folder()
    asyncio.run(_xcxc_scrape_all())


if __name__ == "__main__":
    main()
