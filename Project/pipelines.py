# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import logging
import cv2
import numpy as np
import cvlib as cv
from cvlib.object_detection import draw_bbox
from urllib.request import urlopen
import urllib.parse
import requests
import pandas as pd
import json

class ImagePipeline:
    _headings = [0, 90, 180, 270]
    _fov = 120
    _size = "800x400"
    _apiKey = "AIzaSyCFW5YC1zjDR36AXc8e8BK9UcpQKJYyU4c"
    _baseUrl = "https://maps.googleapis.com/maps/api/streetview?location="

    def image_detection(self, img):
        bbox, label, conf = cv.detect_common_objects(img)
        return label

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        logging.warning(f"Fetching labels for: {adapter['CVR']}")
        numPeople = 0
        numCars = 0
        
        try:
            for heading in self._headings:
                url = f"{self._baseUrl}{urllib.parse.quote(adapter['DirectorAddress'])}&size={self._size}&fov={self._fov}&heading={heading}&key={self._apiKey}"

                req = urlopen(url)
                image = np.asarray(bytearray(req.read()), dtype="uint8")
                image = cv2.imdecode(image, cv2.IMREAD_COLOR) 
                image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)
                labels = self.image_detection(image)     
                
                # add num of lables to item
                numPeople = int(0 if numPeople is None else numPeople) + labels.count('person')
                numCars = int(0 if numCars is None else numCars) + labels.count('car')
            adapter["NumPeople"] = numPeople
            adapter["NumCars"] = numCars
        except:
            logging.warning(f"{adapter['CVR']} Unable to get images")
        finally:
            return item
    
class DistancePipeline:
    _apiKey = "AIzaSyCFW5YC1zjDR36AXc8e8BK9UcpQKJYyU4c"
    _baseUrl = "https://maps.googleapis.com/maps/api/distancematrix/json?destinations="
    
    def process_item(self, item, spider):
        try:
            adapter = ItemAdapter(item)
            logging.warning(f"Fetching distance for: {adapter['CVR']}")
            url = f"{self._baseUrl}{urllib.parse.quote(adapter['DirectorAddress'])}&origins={urllib.parse.quote(adapter['BusinessAddress'])}&key={self._apiKey}"
            response = requests.get(url)
            data = response.json()
            
            meters = data["rows"][0]["elements"][0]["distance"]["value"]
            duration = data["rows"][0]["elements"][0]["duration"]["value"]
            
            adapter["DistanceToBusinessMeters"] = meters
            adapter["DistanceToBusinessDriveSeconds"] = duration
        except:
            adapter["DistanceToBusinessMeters"] = None
            adapter["DistanceToBusinessDriveSeconds"] = None
            logging.warning(f"{adapter['CVR']} Unable to get distance")
        finally:
            return item
    
class AddressFilterPipeline:
    def __init__(self):
        with open('./countries.json', encoding="utf-8") as json_file:
            self.countries = json.load(json_file)
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        if adapter["BusinessAddress"] == adapter["DirectorAddress"]:
            raise DropItem(f"Business address same as director address: {item!r}")
        elif any(country in adapter["BusinessAddress"] for country in self.countries):
            raise DropItem(f"Director address in another country: {item!r}")
        else:
            logging.warning(f"{adapter['CVR']} has a valid address")
            return item
    
class CapitalFilterPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter["RegisteredCapital"] <= 40000:
            raise DropItem(f"Capital too low: {item!r}")
        else:
            logging.warning(f"{adapter['CVR']} has a valid capital")
            return item
        
class DublicateFilterPipeline:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter['CVR'] in self.ids_seen:
            raise DropItem(f"Duplicate item found: {adapter['CVR']}")
        else:
            self.ids_seen.add(adapter['CVR'])
            return item

class IndustryCodePipeline:
    def __init__(self):
        industry_codes = pd.read_html('https://dinero.dk/ordbog/branchekode/')[0]
        industry_codes.columns = industry_codes.iloc[0]
        industry_codes = industry_codes[1:]

        industry_df = {
            "IndustryGroup": [],
            "IndustryShortCode": []
        }

        for index, row in industry_codes.iterrows():
            if row["Hovedafdeling"] == "Information og kommunikation":
                codes = ["0"+str(x) if len(str(x)) == 1 else str(x) for x in range(58, 64)]
                industry_df["IndustryShortCode"] += codes
                industry_df["IndustryGroup"] += [row["Hovedafdeling"]]*len(codes)
            else:
                try:
                    start, end = [int(x) for x in row["Hovedgrupper"].split("-")]
                    codes = ["0"+str(x) if len(str(x)) == 1 else str(x) for x in range(start, end + 1)]
                    industry_df["IndustryShortCode"] += codes
                    industry_df["IndustryGroup"] += [row["Hovedafdeling"]]*len(codes)
                except:
                    industry_df["IndustryShortCode"] += [row["Hovedgrupper"]] 
                    industry_df["IndustryGroup"] += [row["Hovedafdeling"]]
            
            
        self.industry_codes = pd.DataFrame(industry_df).set_index("IndustryShortCode").to_dict("index")
        
        
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        adapter["IndustryGroup"] = self.industry_codes[adapter["IndustryShortCode"]]["IndustryGroup"]
        return item
    
class LatLongPipeline:
    _apiKey = "AIzaSyCFW5YC1zjDR36AXc8e8BK9UcpQKJYyU4c"
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        try:
            response_director = requests.get(f"https://maps.googleapis.com/maps/api/geocode/json?address={adapter['DirectorAddress']}&key={self._apiKey}")
            resp_json_payload_director = response_director.json()
            adapter["DirectorAddressLatitude"] = resp_json_payload_director['results'][0]['geometry']['location']["lat"]
            adapter["DirectorAddressLongitude"] = resp_json_payload_director['results'][0]['geometry']['location']["lng"]
        except:
            adapter["DirectorAddressLatitude"] = None
            adapter["DirectorAddressLongitude"] = None
        
        try:
            response_business = requests.get(f"https://maps.googleapis.com/maps/api/geocode/json?address={adapter['BusinessAddress']}&key={self._apiKey}")
            resp_json_payload_business = response_business.json()
            adapter["BusinessAddressLatitude"] = resp_json_payload_business['results'][0]['geometry']['location']["lat"]
            adapter["BusinessAddressLongitude"] = resp_json_payload_business['results'][0]['geometry']['location']["lng"]
        except:
            adapter["BusinessAddressLatitude"] = None
            adapter["BusinessAddressLongitude"] = None
        return item