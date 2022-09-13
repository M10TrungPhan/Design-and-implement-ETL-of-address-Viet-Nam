import json
import os
import time
import re

from utils.utils import setup_selenium_firefox
from bs4 import BeautifulSoup


class GetProvinceCode:

    def __init__(self, path_save_data):
        self.path_save_data = path_save_data
        self.data = None
        self.city_of_country = ["Đà Nẵng", "Hồ Chí Minh", "Hải Phòng", "Hà Nội", "Cần Thơ"]

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

    def get_all_code_city(self):
        url = "https://batdongsan.com.vn/Contacts/CommonData/GetCityList"
        soup = self.request_html(url)
        if soup is None:
            return
        text = soup.find("pre").text
        text_json = json.loads(text)
        data = []
        for province in text_json:
            data.append(self.get_information_each_province(province))
        self.data = data
        return data

    # @staticmethod
    def get_information_each_province(self, province):
        stand_for = province["cityCode"].strip()
        name = province["cityName"].strip()
        if name in self.city_of_country:
            name = "Thành phố " + name
        else:
            name = "Tỉnh " + name
        name = re.sub(r"\s{2,}", " ", name)
        code = province["priority"]
        return {"ProvinceCode": code, "Province": name.strip(), "StandFor": stand_for}

    def save_data(self, data):
        name_file = "provinces.json"
        os.makedirs(self.path_save_data, exist_ok=True)
        json.dump(data, open(self.path_save_data + name_file, "w", encoding="utf-8"), ensure_ascii=False, indent=4)


if __name__ == "__main__":
    ctiyCode = GetProvinceCode("abc/")
    ctiyCode.get_all_code_city()
    ctiyCode.save_data(ctiyCode.data)
