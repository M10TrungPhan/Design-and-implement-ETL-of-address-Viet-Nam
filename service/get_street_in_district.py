import json
import os
import re
import time

from utils.utils import setup_selenium_firefox
from bs4 import BeautifulSoup


class GetStreetCode:

    def __init__(self, district_id, path_save_data):
        self.path_save_data = path_save_data
        self.district_id = str(district_id)
        self.province_code = None
        self.province_name = None
        self.district_code = None
        self.district_name = None
        self.code_city = None
        self.data = None

    @staticmethod
    def request_html(url):
        while True:
            driver = setup_selenium_firefox()
            res = ""
            for _ in range(5):
                try:
                    driver.get(url)
                    break
                except Exception as e:
                    print(e)
                    res = None
                    continue
            if res is None:
                driver.close()
                time.sleep(30)
                continue
            break
        soup = BeautifulSoup(driver.page_source, "lxml")
        driver.close()
        return soup

    def get_all_code_street(self):
        url = "https://batdongsan.com.vn/Contacts/CommonData/GetStreetList?districtid={}".format(self.district_id)
        soup = self.request_html(url)
        if soup is None:
            return
        text = soup.find("pre").text
        text_json = json.loads(text)
        data = []
        for street in text_json:
            data.append(self.get_information_each_ward(street))
        self.data = data
        return data

    def get_information_each_ward(self, street):
        street_code = street["streetId"]
        street_name = street["streetPrefix"].strip() + " " + street["streetName"].strip()
        street_name = re.sub(r"\s{2,}", " ", street_name)
        return {"StreetCode": street_code, "Street": street_name.strip(),
                "DistrictCode": self.district_code, "District": self.district_name,
                "ProvinceCode": self.province_code, "Province": self.province_name,
                "StandFor": self.code_city}

    def save_data(self, data):
        name_file = "streets/" + self.district_name.replace(" ", "_") \
                    + "_" + self.province_name.replace(" ", "_") + ".json"
        os.makedirs(self.path_save_data + "streets/", exist_ok=True)
        json.dump(data, open(self.path_save_data + name_file, "w", encoding="utf-8"), ensure_ascii=False, indent=4)


if __name__ == "__main__":
    streetCode = GetStreetCode("1", "abc/")
    streetCode.get_all_code_street()
    streetCode.save_data(streetCode.data)

