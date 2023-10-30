import scrapy
import re
import logging
import json
from scrapy_selenium import SeleniumRequest, SeleniumMiddleware
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from Project.items import CompanyItem

LIST_URL = "https://datacvr.virk.dk/soegeresultater?sideIndex=0&region=29190623&virksomhedsstatus=aktive&size=1000"
BASE_URL = "https://datacvr.virk.dk"

class CompanySpider(scrapy.Spider):
    name = "company"
    
    def start_requests(self):
        yield SeleniumRequest(
            url="https://datacvr.virk.dk/soegeresultater?sideIndex=0&region=29190623&virksomhedsstatus=aktive&size=10", 
            callback=self.parse, 
            headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"}, 
            wait_until=EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.button.button-unstyled')),
            wait_time=10,
            cb_kwargs={'index':0}
        )

    def parse(self, response: scrapy.Request, index):
        list = response.selector.css('.soegeresultaterTabel>div')
        
        # if len(list) > 0:
        #     yield SeleniumRequest(
        #         url=f"https://datacvr.virk.dk/soegeresultater?sideIndex={index+1}&region=29190623&virksomhedsstatus=aktive&size=1000", 
        #         callback=self.parse, 
        #         headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"}, 
        #         wait_until=EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.button.button-unstyled')),
        #         wait_time=10,
        #         cb_kwargs={'index':index+1}
        #     )
        
        for item in list:
            link = item.css('a.button.button-unstyled::attr(href)').extract_first()
            m = re.search('.*/([0-9]+?)\?.*', link)
            cvr = m.group(1)
            
            full_url = f"https://datacvr.virk.dk/gateway/virksomhed/hentVirksomhed?cvrnummer={cvr}&locale=en"
            logging.warning(f"Accessing: {cvr}")
            
            yield SeleniumRequest(
                url=full_url, 
                callback=self.parse_company, 
                headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"}, 
            )
            
    def parse_company(self, response: scrapy.Request):
        raw_data = response.css('pre::text').get()
        data = json.loads(raw_data)
        
        company = CompanyItem()
        
        company["Name"] = data["stamdata"]["navn"]
        company["CVR"] = data["stamdata"]["cvrnummer"]
        company["BusinessAddress"] = data["stamdata"]["adresse"].replace("\n", ", ")
        company["StartDate"] = data["stamdata"]["startdato"]
        company["Status"] = data["stamdata"]["status"]
        company["IndustryCode"] = data["udvidedeOplysninger"]["hovedbranche"]["branchekode"]
        company["IndustryName"] = data["udvidedeOplysninger"]["hovedbranche"]["titel"]
        company["RegisteredCapital"] = data["udvidedeOplysninger"]["registreretKapital"]
        company["Area"] = data["udvidedeOplysninger"]["kommune"]
        company["AreaCode"] = data["udvidedeOplysninger"]["kommunekode"]
        company["DirectorName"] = None
        company["DirectorAddress"] = None
        company["DirectorId"] = None
        
        for p in data["personkreds"]["personkredser"]:
            p_data = p["personRoller"][0]
            if "erstdist-organisation-rolle-adm_dir" in p_data["titlePrefix"]:
                company["DirectorName"] = p_data['senesteNavn']
                company["DirectorAddress"] = p_data['adresse']
                company["DirectorId"] = p_data['id']
        if company["DirectorName"] == None:
            p_data = data["personkreds"]["personkredser"][0]["personRoller"][0]
            company["DirectorName"] = p_data['senesteNavn']
            company["DirectorAddress"] = p_data['adresse']
            company["DirectorId"] = p_data['id']
        company["DirectorAddress"] = company["DirectorAddress"].replace("\n", ", ")
        yield company