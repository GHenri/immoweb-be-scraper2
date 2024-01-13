import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import os

'''
set up: choose below parameters
'''
'''
https://www.immoweb.be/en/search/land/for-sale?countries=BE&postalCodes=BE-3270,BE-3271&page=1&orderBy=relevance
Credits to: https://github.com/pierodellagiustina/immoweb-be-scraper
'''


def convert_postalcodes(pcs):
    gms = []
    for pc in pcs:
        r = requests.get(f"https://api.basisregisters.vlaanderen.be/v1/postinfo/{pc}").json()
        gms.append(r["postnamen"][0]["geografischeNaam"]["spelling"])
    return gms


def scraper(pcs, gms, mp):
    ls_id = []
    ls_customer_name = []
    ls_prop_type = []
    ls_prop_postal_code = []
    ls_prop_street = []
    ls_prop_house_no = []
    ls_prop_land_surface = []
    ls_trans_sale_price = []
    ls_trans_old_sale_price = []
    ls_trans_price_per_sqm = []
    ls_latitude = []
    ls_longitude = []

    for pc, gm in zip(pcs, gms):

        for pg in range(1, mp):
            # request url and format it
            # url = f'https://www.immoweb.be/en/search/{property_type}/{tt}/{gm}/{pc}?countries=BE&minBedroomCount=
            # {min_num_bedrooms}&maxBedroomCount={max_num_bedrooms}&maxSurface={max_floor_area}&minSurface={min_floor_area}&page={pg}&orderBy=relevance'
            r = requests.get(f'https://www.immoweb.be/en/search/land/for-sale/{gm}/{pc}?countries=BE'
                             f'&page={pg}&orderBy=relevance')

            soup = BeautifulSoup(r.text, 'html.parser')

            # extract bits I need
            search_res = soup.find('iw-search')
            res_dic = dict(search_res.attrs)

            # get json string from ':results' and put it into a dictionary
            res_json = json.loads(res_dic[':results'])
            # if the page is empty, do not continue running
            if len(res_json) == 0:
                break
            # loop through the items in the page
            for i in range(len(res_json)):
                elem = res_json[i]
                # keep the keys i'm interested in
                elem_subset = {k: elem[k] for k in ('id', 'customerName', 'flags', 'property', 'transaction', 'price')}
                # flatten the dictionary and append it to df
                elem_prop = elem_subset['property']

                ls_id.append(elem_subset['id'])
                ls_customer_name.append(elem_subset['customerName'])
                ls_prop_type.append(elem_prop['type'])
                ls_prop_postal_code.append(elem_prop['location']['postalCode'])
                ls_prop_street.append(elem_prop['location']['street'])
                ls_prop_house_no.append(elem_prop['location']['number'])
                ls_prop_land_surface.append(elem_prop['landSurface'])
                ls_trans_sale_price.append(elem_subset['transaction']['sale']['price'])
                ls_trans_old_sale_price.append(elem_subset['transaction']['sale']['oldPrice'])
                ls_trans_price_per_sqm.append(elem_subset['transaction']['sale']['pricePerSqm'])
                ls_latitude.append(elem_prop['location']['latitude'])
                ls_longitude.append(elem_prop['location']['longitude'])

    return pd.DataFrame.from_dict({'immoweb_id': ls_id, 'customerName': ls_customer_name, 'propType': ls_prop_type,
                                   'propPostalCode': ls_prop_postal_code, 'propStreet': ls_prop_street,
                                   'propHouseNo': ls_prop_house_no, 'propLandSurface': ls_prop_land_surface,
                                   'transSalePrice': ls_trans_sale_price,
                                   'transPricePerSqm': ls_trans_price_per_sqm,
                                   'latitude': ls_latitude, 'longitude': ls_longitude})


def automated_scraping(pcs: list) -> pd.DataFrame:
    cfg = read_config()
    if len(pcs) == 0:
        pcs = cfg["postal_codes"].split(',')

    return scraper(pcs, convert_postalcodes(pcs), int(cfg["max_pages"]))


def read_config():
    with open(os.path.join(os.path.dirname(__file__), "config.cfg")) as json_file:
        cfg = json.load(json_file)
    return cfg


if __name__ == "__main__":
    config = read_config()

    # select postcodes to search (add them to below list)
    postcodes = config["postal_codes"].split(',')
    gemeenten = convert_postalcodes(postcodes)

    # set the number of pages of results you want to scrape (if in doubt, leave unchanged)
    max_pages = int(config["max_pages"])
    out = scraper(postcodes, gemeenten, max_pages)
    # write to csv
    out.to_csv('out.csv')
