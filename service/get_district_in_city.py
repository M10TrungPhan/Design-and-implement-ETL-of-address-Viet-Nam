import json
import os
import time
import re
import hashlib

from utils.utils import setup_selenium_firefox
from bs4 import BeautifulSoup


class GetDistrictCode:

    def __init__(self, code_city, path_save_data):
        self.path_save_data = path_save_data
        self.code_city = code_city
        self.province_code = None
        self.province_name = None
        self.data = []

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

    def get_all_code_district(self):
        url = "https://batdongsan.com.vn/Contacts/CommonData/GetListDistrictByCity?cityCode={}".format(self.code_city)
        soup = self.request_html(url)
        if soup is None:
            return
        text = soup.find("pre").text
        data = []
        text_json = json.loads(text)
        for district in text_json:
            data.append(self.get_information_each_district(district))
        self.data = data
        return data

    def get_information_each_district(self, district):
        district_code = district["districtId"]
        district_name = district["districtName"]
        if re.search(r"Huyện|Thị xã|Thành phố|Quận", district_name) is not None:
            district_name = district["districtPrefix"].strip() + " " + district["districtName"]
        district_name = re.sub(r"\s{2,}", " ", district_name)
        return {"DistrictCode": district_code, "District": district_name.strip(),
                "ProvinceCode": self.province_code, "Province": self.province_name,
                "StandFor": self.code_city}

    def save_data(self, data):
        name_file = "districts/" + self.province_name.replace(" ", "_") + ".json"
        os.makedirs(self.path_save_data + "districts/", exist_ok=True)
        json.dump(data, open(self.path_save_data + name_file, "w", encoding="utf-8"), ensure_ascii=False, indent=4)


if __name__ == "__main__":
    districtCode = GetDistrictCode("HN", "abc/")
    districtCode.get_all_code_district()
    print(districtCode.data)
    # districtCode.save_data(districtCode.data)

