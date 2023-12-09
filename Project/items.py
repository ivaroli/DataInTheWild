# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CompanyItem(scrapy.Item):
    Name = scrapy.Field()
    CVR = scrapy.Field()
    BusinessAddress = scrapy.Field()
    Area = scrapy.Field()
    AreaCode = scrapy.Field()
    StartDate = scrapy.Field()
    Status = scrapy.Field()
    IndustryName = scrapy.Field()
    IndustryCode = scrapy.Field()
    RegisteredCapital = scrapy.Field()
    RegisteredCapitalCurrency = scrapy.Field()
    DirectorName = scrapy.Field()
    DirectorAddress = scrapy.Field()
    DirectorId = scrapy.Field()
    NumEmployees = scrapy.Field()
    NumCars = scrapy.Field()
    NumPeople = scrapy.Field()
    DistanceToBusinessMeters = scrapy.Field()
    DistanceToBusinessDriveSeconds = scrapy.Field()
    DirectorAddressLatitude = scrapy.Field()
    DirectorAddressLongitude = scrapy.Field()
    BusinessAddressLatitude = scrapy.Field()
    BusinessAddressLongitude = scrapy.Field()
    IndustryShortCode = scrapy.Field()
    IndustryGroup = scrapy.Field()