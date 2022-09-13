import json
import os
import re
import time

from utils.utils import setup_selenium_firefox
from bs4 import BeautifulSoup


class GetWardCode:

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

    def get_all_code_ward(self):
        url = "https://batdongsan.com.vn/Contacts/CommonData/GetWardList?districtId={}".format(self.district_id)
        soup = self.request_html(url)
        if soup is None:
            return
        text = soup.find("pre").text
        text_json = json.loads(text)
        data = []
        for ward in text_json:
            data.append(self.get_information_each_ward(ward))
        self.data = data
        return data

    def get_information_each_ward(self, ward):
        ward_code = ward["wardId"]
        ward_name = ward["wardPrefix"].strip() + " " + ward["wardName"].strip()
        ward_name = re.sub(r"\s{2,}", " ", ward_name)
        return {"WardCode": ward_code, "Ward": ward_name.strip(),
                "DistrictCode": self.district_code, "District": self.district_name,
                "ProvinceCode": self.province_code, "Province": self.province_name,
                "StandFor": self.code_city}

    def save_data(self, data):
        name_file = "wards/" + self.district_name.replace(" ", "_") \
                    + "_" + self.province_name.replace(" ", "_") + ".json"
        os.makedirs(self.path_save_data + "wards/", exist_ok=True)
        json.dump(data, open(self.path_save_data + name_file, "w", encoding="utf-8"), ensure_ascii=False, indent=4)


if __name__ == "__main__":
    wardCode = GetWardCode("1", "abc/")
    wardCode.get_all_code_ward()
    wardCode.save_data(wardCode.data)


