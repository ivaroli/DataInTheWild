import scrapy
import re
import logging
import json
from scrapy_selenium import SeleniumRequest, SeleniumMiddleware
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import string
from Project.items import CompanyItem

class CompanySpider(scrapy.Spider):
    name = "company"
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    
    def start_requests(self):
        for letter in list(string.ascii_lowercase):
            yield SeleniumRequest(
                url=f"https://datacvr.virk.dk/soegeresultater?fritekst={letter}&sideIndex=0&region=29190623&antalAnsatte=ANTAL_2_4%252CANTAL_5_9%252CANTAL_10_19%252CANTAL_20_49%252CANTAL_50_99&virksomhedsform=60%252C130%252C140%252C80%252C210%252C81%252C30&virksomhedsstatus=aktiv%252Cnormal%252Caktive&size=3000", 
                callback=self.parse, 
                headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"}, 
                wait_until=EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.button.button-unstyled')),
                wait_time=15,
                cb_kwargs={'index':0, 'letter': letter}
            )
            break

    def parse(self, response: scrapy.Request, index, letter):
        list = response.selector.css('.soegeresultaterTabel>div')
        
        if len(list) > 0:
            yield SeleniumRequest(
                url=f"https://datacvr.virk.dk/soegeresultater?fritekst={letter}&sideIndex={index+1}&region=29190623&antalAnsatte=ANTAL_2_4%252CANTAL_5_9%252CANTAL_10_19%252CANTAL_20_49%252CANTAL_50_99&virksomhedsform=60%252C130%252C140%252C80%252C210%252C81%252C30&virksomhedsstatus=aktiv%252Cnormal%252Caktive&size=3000", 
                callback=self.parse, 
                headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"}, 
                wait_until=EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.button.button-unstyled')),
                wait_time=15,
                cb_kwargs={'index':index+1}
            )
        
        for item in list:
            link = item.css('a.button.button-unstyled::attr(href)').extract_first()
            m = re.search('.*/([0-9]+?)\?.*', link)
            cvr = m.group(1)
            
            full_url = f"https://datacvr.virk.dk/gateway/virksomhed/hentVirksomhed?cvrnummer={cvr}&locale=en"
            logging.warning(f"Accessing: {cvr}")
            
            yield SeleniumRequest(
                url=full_url, 
                callback=self.parse_company, 
                headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"}, 
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
        company["Area"] = data["udvidedeOplysninger"]["kommune"]
        company["AreaCode"] = data["udvidedeOplysninger"]["kommunekode"]
        try:
            company["NumEmployees"] = data["antalAnsatte"]["maanedsbeskaeftigelse"][0]["antalAnsatte"]
        except:
            return
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
        try:
            company["DirectorAddress"] = company["DirectorAddress"].replace("\n", ", ")
        except:
            pass
        
        reg_cap = data["udvidedeOplysninger"]["registreretKapital"]
        try:
            reg_cap_split = reg_cap.split()
        except:
            return
        
        if len(reg_cap_split) == 2:
            company["RegisteredCapital"] = float(reg_cap_split[0].replace(".", "").replace(",", "."))
            company["RegisteredCapitalCurrency"] = reg_cap_split[1]
        else:
            company["RegisteredCapital"] = 0
            company["RegisteredCapitalCurrency"] = "DKK"
        
        yield company