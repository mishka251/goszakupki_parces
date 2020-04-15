from typing import Dict, List

import urllib.parse as parse_url
from bs4 import BeautifulSoup
import requests
from database import Purchase, orm


def parse_page(page: BeautifulSoup) -> List[str]:
    purchases_objects = page.select('.search-registry-entry-block .registry-entry__body-value')
    object_names = []
    for purchases_object in purchases_objects:
        obj = purchases_object.text
        object_names.append(obj)

    return object_names


def change_db(finished_purchases_objects: List[str]):
    with orm.db_session:
        purchases = list(orm.select(p for p in Purchase))
        for purchase in purchases:
            is_finished = any([obj.startswith(purchase.name) for obj in finished_purchases_objects])
            purchase.is_finished = is_finished


url = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?searchString=&morphology=on&search-filter=%D0%94%D0%B0%D1%82%D0%B5+%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D1%8F&pageNumber=1&sortDirection=false&recordsPerPage=_50&showLotsInfoHidden=false&savedSearchSettingsIdHidden=&sortBy=UPDATE_DATE&fz44=on&fz223=on&pc=on&placingWayList=&okpd2Ids=&okpd2IdsCodes=&selectedSubjectsIdHidden=&npaHidden=&restrictionsToPurchase44=&publishDateFrom=&publishDateTo=&applSubmissionCloseDateFrom=&applSubmissionCloseDateTo=&priceFromGeneral=&priceFromGWS=&priceFromUnitGWS=&priceToGeneral=&priceToGWS=&priceToUnitGWS=&currencyIdGeneral=-1&customerIdOrg=&agencyIdOrg="
base_url = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?"

params = parse_url.parse_qs(url)
params["pageNumber"] = "1"

s = requests.Session()
s.get(base_url)
full_url = base_url + parse_url.urlencode(params, True)
headers = {"Host": "zakupki.gov.ru",
           "Sec-Fetch-Dest": "document",
           "Sec-Fetch-Mode": "navigate",
           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
           }
page = s.get(full_url, verify=False, headers=headers).text
soap = BeautifulSoup(page, features="lxml")
last_page = soap.select('.paginator .page .link-text')[-1]
print(last_page.text)

pages_count = int(last_page.text)

finished_objects = parse_page(soap)

for page_number in range(2, pages_count + 1):
    params["pageNumber"] = str(page_number)
    full_url = base_url + parse_url.urlencode(params)
    page = s.get(full_url, verify=False, headers=headers).text
    soap = BeautifulSoup(page, features="lxml")
    finished_objects.extend(parse_page(soap))

print("end load")
change_db(finished_objects)
print("end")
