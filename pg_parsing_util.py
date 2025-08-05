# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,too-many-arguments,too-many-positional-arguments,too-many-locals,line-too-long,logging-fstring-interpolation,broad-exception-caught
import dataclasses
import datetime
import json
import logging
import re
import typing

PROPERTY_GURU_BASE_URL = "https://www.propertyguru.com.sg"
TOP_YEAR_PATTERN = r"^TOP in( [a-zA-Z]+)? (\d+)$"
LISTING_ID_PATTERN = r"^Listing ID - \d+$"
LISTED_DATE_PATTERN = r"^Listed on\s+(\d+)\s+([a-zA-Z]+)\s+(\d+)$"
SINGAPORE_TIMEZONE = datetime.timezone(datetime.timedelta(hours=8))
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
    location_lat: typing.Any
    location_lon: typing.Any
    region: typing.Any
    district: typing.Any
    estate: typing.Any
    street: typing.Any
    nearest_mrt_name: typing.Any
    nearest_mrt_distance_metres: typing.Any
    nearest_mrt_duration_seconds: typing.Any
    furnished_status: typing.Any
    top_year: typing.Any
    listed_date: typing.Any
    tenanted_status: typing.Any
    floor_level: typing.Any
    description_subtitle: typing.Any
    description_details: typing.Any


@dataclasses.dataclass
class ExtraInfo:
    agent_name: typing.Any
    agent_agency: typing.Any
    agent_profile_url: typing.Any
    amenities: typing.Any
    main_image: typing.Any
    all_images: typing.Any
    floor_plans: typing.Any
    faq_info: typing.Any


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
    address = header_data["fullAddress"]

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
    if (
        price_type is not None
        and price_type != "Negotiable"
        and price_type != "Starting From"
    ):
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
    bedrooms_item = next((item for item in items if item["iconSrc"] == "bed-o"), None)
    if bedrooms_item is None:
        logger.warning(
            f"Did not find a 'bedrooms' items within 'amenities' for {listing_url}"
        )
    bathrooms_item = next((item for item in items if item["iconSrc"] == "bath-o"), None)
    if bathrooms_item is None:
        logger.warning(
            f"Did not find a 'bathrooms' items within 'amenities' for {listing_url}"
        )
    area_item = next((item for item in items if item["iconSrc"] == "ruler-o"), None)
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
    nearest_mrt_info = _parse_nearest_mrt_data(
        items=location_data["nearestMRTs"], listing_url=listing_url
    )
    metatable_details_info = _parse_metatable_details_data(
        items=main_data["detailsData"]["metatable"]["items"],
        listing_data=listing_data,
        listing_url=listing_url,
    )
    description_data = main_data["descriptionBlockData"]
    return DetailsInfo(
        location_lat=location_data["center"]["lat"],
        location_lon=location_data["center"]["lng"],
        region=listing_data["regionText"],
        district=listing_data["districtText"],
        estate=listing_data["hdbEstateText"],
        street=listing_data["streetName"],
        nearest_mrt_name=nearest_mrt_info.name,
        nearest_mrt_distance_metres=nearest_mrt_info.distance_metres,
        nearest_mrt_duration_seconds=nearest_mrt_info.duration_seconds,
        furnished_status=metatable_details_info.furnished_status,
        top_year=metatable_details_info.top_year,
        listed_date=metatable_details_info.listed_date,
        tenanted_status=metatable_details_info.tenanted_status,
        floor_level=metatable_details_info.floor_level,
        description_subtitle=description_data["subtitle"],
        description_details=description_data["description"],
    )


@dataclasses.dataclass
class NearestMrtInfo:
    name: typing.Any
    distance_metres: typing.Any
    duration_seconds: typing.Any


def _parse_nearest_mrt_data(items, listing_url):
    non_future_infos = [
        NearestMrtInfo(
            name=item["name"],
            distance_metres=item["distance"]["value"],
            duration_seconds=item["duration"]["value"],
        )
        for item in items
        if not item["isFutureLine"]
    ]
    if len(non_future_infos) == 0:
        return NearestMrtInfo(
            name=None,
            distance_metres=None,
            duration_seconds=None,
        )
    return min(non_future_infos, key=lambda info: info.duration_seconds)


@dataclasses.dataclass
class MetatableDetailsData:
    furnished_status: typing.Any
    top_year: typing.Any
    listed_date: typing.Any
    tenanted_status: typing.Any
    floor_level: typing.Any


def _parse_metatable_details_data(items, listing_data, listing_url):
    furnished_status_item = next(
        (item for item in items if item["icon"] == "furnished-o"), None
    )
    top_year_item = next(
        (item for item in items if item["icon"] == "document-with-lines-o"), None
    )
    tenure_item = next(
        (item for item in items if item["icon"] == "calendar-days-o"), None
    )
    if tenure_item["value"] != "99-year lease" or listing_data["tenure"] != "L99":
        logger.warning(f"Found a non-99-year lease HDB unit at {listing_url}")
    listed_date_item = next(
        (item for item in items if item["icon"] == "calendar-time-o"), None
    )
    tenanted_status_item = next(
        (item for item in items if item["icon"] == "people-behind-o"), None
    )
    floor_level_item = next(
        (item for item in items if item["icon"] == "layers-2-o"), None
    )
    developer_item = next(
        (item for item in items if item["icon"] == "new-project-o"), None
    )
    if developer_item["value"] != "Developed by Housing & Development Board\xa0(HDB)":
        logger.warning(f"Found a non-HDB unit at {listing_url}")

    return MetatableDetailsData(
        furnished_status=furnished_status_item["value"]
        if furnished_status_item is not None
        else None,
        top_year=_parse_top_year(
            year_text=top_year_item["value"], listing_url=listing_url
        )
        if top_year_item is not None
        else None,
        listed_date=_parse_listed_date(
            listed_date_text=listed_date_item["value"],
            listing_data=listing_data,
            listing_url=listing_url,
        )
        if listed_date_item is not None
        else None,
        tenanted_status=tenanted_status_item["value"]
        if tenanted_status_item is not None
        else None,
        floor_level=floor_level_item["value"] if floor_level_item is not None else None,
    )


def _parse_top_year(year_text, listing_url):
    match = re.search(TOP_YEAR_PATTERN, year_text)
    if match is None:
        # Sometimes there is just no TOP year item, and we use the same icon for Listing ID item;
        # we still return None in this case, but omit the warning since this is an expected outcome
        if re.search(LISTING_ID_PATTERN, year_text) is not None:
            logger.error(
                f"TOP year item {year_text} did not match the known pattern for {listing_url}"
            )
        return None
    return int(match.group(2))


def _parse_listed_date(listed_date_text, listing_data, listing_url):
    unix_time = listing_data["lastPosted"]["unix"]
    unix_date = datetime.datetime.fromtimestamp(unix_time, tz=SINGAPORE_TIMEZONE)

    match = re.search(LISTED_DATE_PATTERN, listed_date_text)
    if match is None:
        logger.error(
            f"Listed date item {listed_date_text} did not match the known pattern for {listing_url}"
        )
        return unix_date.strftime("%Y-%m-%d")

    day, month, year = match.groups()
    text_date = datetime.datetime(
        year=int(year),
        month=datetime.datetime.strptime(month, "%b").month,
        day=int(day),
    )
    if text_date.date() != unix_date.date():
        logger.warning(
            f"Different info from `main_data` and `listing_data` for listed_date for {listing_url}"
        )
    return unix_date.strftime("%Y-%m-%d")


def _parse_extra_info(main_data, listing_data, listing_url):
    agent_card_data = main_data["contactAgentData"]["contactAgentCard"]
    agent_info_data = agent_card_data["agentInfoProps"]["agent"]
    agent_name = _compare_data_and_return(
        value_from_main=agent_info_data["name"],
        value_from_listing=listing_data["agent"]["name"],
        listing_url=listing_url,
        debug_logging_name="agent_name",
    )
    agent_agency = agent_card_data["agency"]["name"]
    agent_profile_url = f"{PROPERTY_GURU_BASE_URL}{agent_info_data['profileUrl']}"

    amenities_data = main_data["amenitiesData"]
    amenities_items = amenities_data["data"] if amenities_data is not None else []
    amenities = sorted(list(set(item["text"] for item in amenities_items)))

    main_image = main_data["metadata"]["metaTags"]["openGraph"]["image"]

    media_gallery_data = main_data["mediaGalleryData"]["media"]
    media_explorer_data = main_data["mediaExplorerData"]["mediaGroups"]
    all_images = sorted(
        list(
            set(
                [item["src"] for item in media_gallery_data["images"]["items"]]
                + [item["src"] for item in media_explorer_data["images"]["items"]]
            )
        )
    )
    floor_plans = sorted(
        list(
            set(
                [item["src"] for item in media_gallery_data["floorPlans"]["items"]]
                + [item["src"] for item in media_explorer_data["floorPlans"]["items"]]
            )
        )
    )

    faq_items = main_data["faqData"]["list"]
    faq_info = "\n\n".join(
        [f"{faq_data['question']}\n{faq_data['answer']}" for faq_data in faq_items]
    )

    return ExtraInfo(
        agent_name=agent_name,
        agent_agency=agent_agency,
        agent_profile_url=agent_profile_url,
        amenities=amenities,
        main_image=main_image,
        all_images=all_images,
        floor_plans=floor_plans,
        faq_info=faq_info,
    )


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
