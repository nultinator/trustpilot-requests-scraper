import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]

## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scrape_search_results(keyword, location, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    url = f"https://www.trustpilot.com/search?query={formatted_keyword}&page={page_number+1}"

    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code == 200:
                success = True
            
            else:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")
            script_tag = soup.find("script", id="__NEXT_DATA__")
            if script_tag:
                json_data = json.loads(script_tag.contents[0])
                    
                business_units = json_data["props"]["pageProps"]["businessUnits"]

                for business in business_units:

                    name = business.get("displayName").lower().replace(" ", "").replace("'", "")
                    trustpilot_formatted = business.get("contact")["website"].split("://")[1]
                    location = business.get("location")
                    category_list = business.get("categories")
                    category = category_list[0]["categoryId"] if len(category_list) > 0 else "n/a"

                    ## Extract Data
                    search_data = {
                        "name": business.get("displayName", ""),
                        "stars": business.get("stars", 0),
                        "rating": business.get("trustScore", 0),
                        "num_reviews": business.get("numberOfReviews", 0),
                        "website": business.get("contact")["website"],
                        "trustpilot_url": f"https://www.trustpilot.com/review/{trustpilot_formatted}",
                        "location": location.get("country", "n/a"),
                        "category": category
                    }

                logger.info(f"Successfully parsed data from: {url}")
                success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")




def start_scrape(keyword, pages, location, retries=3):
    for page in range(pages):
        scrape_search_results(keyword, location, page, retries)

if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "uk"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["online bank"]

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        scrape_search_results(keyword, LOCATION, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")

