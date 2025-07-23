# pylint: disable=import-error,missing-module-docstring,missing-class-docstring,missing-function-docstring,too-few-public-methods,too-many-instance-attributes,line-too-long,logging-fstring-interpolation,broad-exception-caught
import dataclasses
import datetime
import logging
import re
import typing

logger = logging.getLogger("HDB Scraper: HDB Parsing Util")


@dataclasses.dataclass
class HeaderInfo:
    address: typing.Any
    sub_address: typing.Any
    postal_code: typing.Any
    hdb_type: typing.Any
    area: typing.Any
    price: typing.Any


def parse_header_info(html_soup):
    logger.debug("*** Parsing header info ***")
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


def _find_simple_text(html_soup, tag_type, attr_regex, debug_logging_name):
    logger.debug(f"Starting to find simple text for {debug_logging_name}")
    elements = html_soup.find_all(tag_type)
    for element in elements:
        logger.debug(f"Checking element for {debug_logging_name}: {element}")
        if any(attr_regex.match(attr_keys) for attr_keys in element.attrs.keys()):
            contents = element.contents
            logger.debug(f"Matched element for {debug_logging_name}! {contents}")
            return _direct_text_contents(contents=contents)
    logger.error(f"No matching elements for {debug_logging_name}!")
    return None


def _direct_text_contents(contents):
    return [content for content in contents if isinstance(content, str)]


def _parse_postal_code_from_sub_address(sub_address):
    pattern = r"Singapore\s+(\d{6})"
    match = re.search(pattern=pattern, string=sub_address)
    if match:
        return match.group(1)
    logger.error(f"Could not parse postal code from sub-address {sub_address}")
    return None


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


def parse_details_info(html_soup):
    logger.debug("*** Parsing details info ***")
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
