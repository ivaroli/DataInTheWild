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
        finally:
            return item
    
class DuplicatesPipeline:
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter["id"] in self.ids_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        else:
            self.ids_seen.add(adapter["id"])
            return item
