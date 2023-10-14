# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import hashlib
import re
from typing import Dict, List
from urllib.parse import urljoin

import dateparser
import scrapy
from itemloaders.processors import TakeFirst


def parse_date(value):
    # Date is always in the last position.
    value = value[-1]
    # Parse the date string and convert to a datetime object
    date_obj = dateparser.parse(value, languages=["pl"])
    if date_obj:
        return date_obj.isoformat(timespec="minutes")
    return None


def extract_location_from_string(location_list):
    """
    Extracts the city name from a list, stopping at ' - '.

    Parameters:
    - location_list (List[str]): A list containing location and date information.

    Returns:
    - str: The extracted city name.
    """
    location = []
    for element in location_list:
        if element.strip() == "-":
            break
        location.append(element.strip())
    return " ".join(location)


def filter_city(value: List[str]) -> Dict[str, str]:
    if isinstance(value, List):
        location = extract_location_from_string(value)
        if "," in location:
            city = location.split(",")[0].strip()
        else:
            city = location
        return city
    return None


def filter_district(value: List[str]) -> Dict[str, str]:
    if isinstance(value, List):
        location = extract_location_from_string(value)
        if "," in location:
            district = location.split(",")[1].strip()
        else:
            district = None
        return district
    return None


def extract_amount(money_string):
    """
    Extracts the numerical amount from a money string.

    Parameters:
    - money_string (str): A string containing a numerical value and a currency symbol.

    Returns:
    - float: The extracted numerical value.
    """
    if isinstance(money_string, List) and money_string:
        money_string = money_string[0]
        if money_string == "Zamienię":
            return 9999
    amount = float(
        money_string.replace("zł", "").replace(" ", "").replace(",", ".").strip()
    )
    return amount


def transform_status(status_string):
    if isinstance(status_string, List) and status_string:
        status_string = status_string[0]
    if status_string == "Nowe":
        return 1
    elif status_string == "Używane":
        return 2
    else:
        return 3


def contains_token(haystack, needle):
    """
    Checks if the needle token is contained within the haystack string,
    ignoring case and punctuation.

    Parameters:
    - haystack (str): The string to search within.
    - needle (str): The token to search for.

    Returns:
    - bool: True if the needle is found within the haystack, otherwise False.
    """
    print(haystack)
    # Remove punctuation and make both strings lowercase
    cleaned_haystack = re.sub(r"[^\w\s]", "", haystack).lower()
    cleaned_needle = re.sub(r"[^\w\s]", "", needle).lower()

    # Check if the cleaned needle is within the cleaned haystack
    return cleaned_needle in cleaned_haystack


def make_url(url):
    if isinstance(url, List) and url:
        url = url[0]
    base_url = "https://www.olx.pl"
    return urljoin(base_url, url)


def hash_string(s):
    hash_object = hashlib.sha256()
    hash_object.update(s.encode())
    return hash_object.hexdigest()


class OlxAd(scrapy.Item):
    # Processed fields.
    title = scrapy.Field()
    price = scrapy.Field(input_processor=extract_amount)
    status = scrapy.Field(
        input_processor=transform_status)

    city = scrapy.Field(input_processor=filter_city)
    district = scrapy.Field(
        input_processor=filter_district)

    published = scrapy.Field(input_processor=parse_date)
    label = scrapy.Field()
    category = scrapy.Field()
    url = scrapy.Field(input_processor=make_url)

    # Data fields.
    scraped_timestamp = scrapy.Field()
    scraped_date = scrapy.Field()


    # Calculated fields.
    title_contains_label_token = scrapy.Field()
    ad_id = scrapy.Field()

    def check_status_for_willingness_to_exchange(self):
        if self["price"] == 9999:
            self["status"] = 4

    def check_if_title_contains_label_token(self):
        self["title_contains_label_token"] = contains_token(
            haystack=self["title"], needle=self["label"]
        )
    def set_hashed_ad_id(self):
        self["ad_id"] = hash_string(self["url"])
