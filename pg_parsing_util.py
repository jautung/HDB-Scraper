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
    price: typing.Any
    price_is_negotiable: typing.Any
    num_bedrooms: typing.Any
    num_bathrooms: typing.Any
    area_sqft: typing.Any
    is_verified: typing.Any


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
        listing_data = main_data["listingData"]
        # Data is somehow duplicated between `main_data` and `listing_data`.
        # Generally, prefer `listing_data` as the source of truth where possible;
        # and warn on mismatches with `main_data`.
        header_info = _parse_header_info(
            main_data=main_data, listing_data=listing_data, listing_url=listing_url
        )
        details_info = _parse_details_info(
            main_data=main_data, listing_data=listing_data, listing_url=listing_url
        )
        extra_info = _parse_extra_info(
            main_data=main_data, listing_data=listing_data, listing_url=listing_url
        )
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


def _parse_header_info(main_data, listing_data, listing_url):
    overview_data = main_data["propertyOverviewData"]
    header_data = overview_data["propertyInfo"]

    title = _compare_data_and_return(
        value_from_main=header_data["title"],
        value_from_listing=listing_data["localizedTitle"],
        listing_url=listing_url,
        debug_logging_name="title",
    )
    address = _compare_data_and_return(
        value_from_main=header_data["fullAddress"],
        value_from_listing=listing_data["propertyName"],
        listing_url=listing_url,
        debug_logging_name="address",
    )

    price_data = header_data["price"]
    price = _compare_data_and_return(
        value_from_main=text_to_price(price_data["amount"]),
        value_from_listing=listing_data["price"],
        listing_url=listing_url,
        debug_logging_name="price",
    )
    price_is_negotiable = _parse_price_is_negotiable(
        price_type=price_data["priceType"], listing_url=listing_url
    )

    room_and_area_data = _parse_room_and_area_data(
        items=header_data["amenities"], listing_url=listing_url
    )
    num_bedrooms = _compare_data_and_return(
        value_from_main=room_and_area_data.num_bedrooms,
        value_from_listing=listing_data["bedrooms"],
        listing_url=listing_url,
        debug_logging_name="num_bedrooms",
    )
    num_bathrooms = _compare_data_and_return(
        value_from_main=room_and_area_data.num_bathrooms,
        value_from_listing=listing_data["bathrooms"],
        listing_url=listing_url,
        debug_logging_name="num_bathrooms",
    )
    area_sqft = _compare_data_and_return(
        value_from_main=room_and_area_data.area_sqft,
        value_from_listing=listing_data["floorArea"],
        listing_url=listing_url,
        debug_logging_name="area_sqft",
    )

    is_verified = _compare_data_and_return(
        value_from_main=overview_data["verifiedListingBadge"] is not None,
        value_from_listing=listing_data["isVerified"],
        listing_url=listing_url,
        debug_logging_name="is_verified",
    )

    return HeaderInfo(
        title=title,
        address=address,
        postal_code=listing_data["postcode"],
        hdb_type=listing_data["hdbTypeCode"],
        price=price,
        price_is_negotiable=price_is_negotiable,
        num_bedrooms=num_bedrooms,
        num_bathrooms=num_bathrooms,
        area_sqft=area_sqft,
        is_verified=is_verified,
    )


def _parse_price_is_negotiable(price_type, listing_url):
    if price_type is not None and price_type != "Negotiable":
        logger.warning(f"Found unexpected price type {price_type} for {listing_url}")
    return price_type == "Negotiable"


@dataclasses.dataclass
class RoomAndAreaData:
    num_bedrooms: typing.Any
    num_bathrooms: typing.Any
    area_sqft: typing.Any


def _parse_room_and_area_data(items, listing_url):
    if len(items) != 3:
        logger.warning(
            f"Found unexpected length of 'amenities' (room and area) items for {listing_url}"
        )
    bedrooms_item = next(item for item in items if item["iconSrc"] == "bed-o")
    if bedrooms_item is None:
        logger.warning(
            f"Did not find a 'bedrooms' items within 'amenities' for {listing_url}"
        )
    bathrooms_item = next(item for item in items if item["iconSrc"] == "bath-o")
    if bathrooms_item is None:
        logger.warning(
            f"Did not find a 'bathrooms' items within 'amenities' for {listing_url}"
        )
    area_item = next(item for item in items if item["iconSrc"] == "ruler-o")
    if area_item is None:
        logger.warning(
            f"Did not find an 'area' items within 'amenities' for {listing_url}"
        )
    return RoomAndAreaData(
        num_bedrooms=text_to_num(bedrooms_item["value"])
        if bedrooms_item is not None
        else None,
        num_bathrooms=text_to_num(bathrooms_item["value"])
        if bathrooms_item is not None
        else None,
        area_sqft=text_to_num(area_item["value"]) if area_item is not None else None,
    )


def _parse_details_info(main_data, listing_data, listing_url):
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
    # TODO: This needs a lot of parsing
    print([(d["icon"], d["value"]) for d in details_data])

    description_data = main_data["descriptionBlockData"]
    description_subtitle = description_data["subtitle"]
    description_details = description_data["description"]

    # listingData
    listing_data = main_data["listingData"]
    # This also has a lot of fields of the above stuff, maybe we should've been using this instead
    listing_data["districtCode"]
    listing_data["regionCode"]
    listing_data["tenure"]
    listing_data["hdbEstateText"]
    listing_data["streetName"]
    listing_data["lastPosted"]["unix"]


def _parse_extra_info(main_data, listing_data, listing_url):
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


def _compare_data_and_return(
    value_from_main, value_from_listing, listing_url, debug_logging_name
):
    if value_from_main is None:
        return value_from_listing
    if value_from_main != value_from_listing:
        logger.warning(
            f"Different info from `main_data` and `listing_data` for {debug_logging_name} for {listing_url}"
        )
    return value_from_listing


def text_to_price(text):
    if text.startswith("S$ "):
        return text_to_num(text[3:])
    return None


def text_to_num(text):
    try:
        return int(text.replace(",", ""))
    except ValueError:
        return None
