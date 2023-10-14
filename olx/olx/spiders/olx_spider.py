import datetime
import re

import scrapy
from typing import Generator, List
from scrapy.http import Response
from ..items import OlxAd
from ..loaders import OlxItemLoader
from datetime import date


class OlxSpider(scrapy.Spider):
    name = "olx"
    allowed_domains = ["olx.pl"]

    # Custom queries as a list
    queries = [
        "7 cudów",
        "7 cudów świata pojedynek",
        'zwierzęcy front',
        'black orchestra',
        "blitzkrieg world war two in 20 minutes",
        "chaos w starym świecie",
        "chaos in the old world",
        "death may die",
        "cubitos",
        "cyklady",
        "bloody palace",
        "dune imperium",
        "diuna imperium",
        "fallout shelter",
        "fruit ninja",
        "gretchinz",
        "hannibal hamilcar",
        "homeworld fleet command",
        "kemet",
        "pan lodowego ogrodu",
        "metal gear solid",
        "katedra koszmarów",
        "pandemic legacy",
        "pandemic upadek rzymu",
        "pandemic cthulhu",
        "rising sun",
        "summoner wars",
        "twilight struggle",
        "wrath of the lich king",
        "xcom",
        "zona sekret czarnobyla",
    ]

    # BASE_URL = "https://www.olx.pl/sport-hobby/gry-planszowe/q-"

    # Define category URLs as a dictionary with labels
    category_urls = {
        "board_games": "https://www.olx.pl/sport-hobby/gry-planszowe/q-",  # Example URL 1
        # "books": "https://www.olx.pl/muzyka-edukacja/ksiazki/q-",  # Example URL 2
        # Add more labeled category URLs as needed
    }

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        for category_label, category_url in self.category_urls.items():
            for query_label in self.queries:
                # Creating URL by appending query to category URL
                url = f"{category_url}{query_label.replace(' ', '-').lower()}"
                yield scrapy.Request(url, self.parse, cb_kwargs={"category_label": category_label, "query_label": query_label})

    def should_stop_because_no_ads(self, response: Response) -> bool:
        total_count_text = response.css('span[data-testid="total-count"]::text').get()
        match = re.search(r'\d+', total_count_text)
        if match:
            total_count = int(match.group())
            return True if total_count == 0 else False
        return False

    def should_stop_because_ad_from_extended_category(self, url):
        """
        Check if the URL meets any of the specified conditions.

        Parameters:
        - url (str): The URL to check.

        Returns:
        - bool: True if any of the conditions are met, otherwise False.
        """
        conditions = [
            url.endswith("?reason=extended_search_extended_category"),
            url.endswith("l?reason=extended_search_extended_s2v"),
            # Add more conditions as needed
        ]
        return any(conditions)


    def parse(self, response: Response, category_label: str, query_label: str) -> Generator[OlxAd, None, None]:
        if self.should_stop_because_no_ads(response):
            self.logger.info(f"There are no ads for `{query_label}` in this category.")
            return

        # Loop through each product item
        for item in response.css('div[data-cy="l-card"]'):
            # Set up the loader and scrape the actual response.
            loader = OlxItemLoader(item=OlxAd(), selector=item)
            loader.add_css("published", 'p[data-testid="location-date"]::text')
            loader.add_css("title", "h6::text")
            loader.add_css("price", 'p[data-testid="ad-price"]::text')
            loader.add_css("status", "span span::text")
            loader.add_css("city", 'p[data-testid="location-date"]::text')
            loader.add_css("district", 'p[data-testid="location-date"]::text')
            loader.add_css("url", "a::attr(href)")

            # Add values to loader.
            loader.add_value("label", query_label)
            loader.add_value("category", category_label)
            loader.add_value(
                "scraped_timestamp", datetime.datetime.now().isoformat(timespec="seconds")
            )
            loader.add_value(
                "scraped_date", datetime.date.today().isoformat(),
            )

            # Load the item and run calculated fields.
            olx_ad = loader.load_item()
            olx_ad.check_if_title_contains_label_token()
            olx_ad.check_status_for_willingness_to_exchange()
            olx_ad.set_hashed_ad_id()

            if self.should_stop_because_ad_from_extended_category(olx_ad["url"]):
                self.logger.info(f"There are no more ads for `{query_label}` in this category.")
                return

            # Yield the item only if the ad title contains the label you searched for.
            if olx_ad["title_contains_label_token"]:
                yield olx_ad
            # Abort if ad url indicates it comes from a different category.



        # Extract all potential "next page" URLs using XPath
        next_page_urls = response.xpath(
            "/html/body/div[1]/div[2]/div[2]/form/div[5]/div/section[1]/div/ul/a/@href"
        ).getall()
        # Choose the last URL as the "next page" URL
        next_page_url = next_page_urls[-1] if next_page_urls else None
        if next_page_url:
            # Ensure the URL is absolute
            absolute_next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(
                absolute_next_page_url, self.parse, cb_kwargs={"label": label}
            )
