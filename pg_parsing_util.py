# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import dataclasses
import json
import logging
import typing

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ListingInfo:
    listing_url: typing.Any
    header_info: typing.Any
    details_info: typing.Any
    extra_info: typing.Any


@dataclasses.dataclass
class HeaderInfo:
    title: typing.Any
    address: typing.Any
    postal_code: typing.Any
    hdb_type: typing.Any
    area_sqft: typing.Any
    price: typing.Any


@dataclasses.dataclass
class DetailsInfo:
    town: typing.Any


@dataclasses.dataclass
class ExtraInfo:
    town: typing.Any


def parse_script_data_element(script_data_element, listing_url):
    json_data = script_data_element.string
    try:
        data = json.loads(json_data)
        main_data = data["props"]["pageProps"]["pageData"]["data"]
        header_info = _parse_header_info(main_data=main_data, listing_url=listing_url)
        details_info = _parse_details_info(main_data=main_data, listing_url=listing_url)
        extra_info = _parse_extra_info(main_data=main_data, listing_url=listing_url)
        return ListingInfo(
            listing_url=listing_url,
            header_info=header_info,
            details_info=details_info,
            extra_info=extra_info,
        )
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(
            f"Failed to parse JSON content from script data tag in {listing_url}"
        )
        logger.error(e)
        return None


def _parse_header_info(main_data, listing_url):
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


def _parse_details_info(main_data, listing_url):
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


def _parse_extra_info(main_data, listing_url):
    # Maybe agent can be None? unclear...
    listing_data = main_data["listingData"]
    listing_data["agent"]["name"]
    main_data["contactAgentData"]["contactAgentCard"]["agency"]["name"]
    main_data["contactAgentData"]["contactAgentCard"]["agentInfoProps"]["agent"]["name"]
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
    floor_plan_links_2 = [i["src"] for i in media_explorer_data["floorPlans"]["items"]]

    faq_data = main_data["faqData"]["list"]
    faq_info = "\n\n".join([f["question"] + "\n" + f["answer"] for f in faq_data])
    # print(faq_info)


def text_to_price(text):
    if text.startswith("S$ "):
        return text_to_num(text[3:])
    return None


def text_to_num(text):
    try:
        return int(text.replace(",", ""))
    except ValueError:
        return None
