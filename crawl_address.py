import json
import threading
from threading import Thread
from queue import Queue
import concurrent.futures
import time
import os
import re
import shutil
import logging

from service.get_provinces_code import GetProvinceCode
from service.get_district_in_city import GetDistrictCode
from service.get_street_in_district import GetStreetCode
from service.get_ward_in_district import GetWardCode
from service.download_excel_file import DownloadExcelFile
from service.convert_province_code import ConvertProvinceCode
from service.convert_district_code import ConvertDistrictCode
from service.convert_ward_code import ConvertWardtCode
from service.convert_street_code import ConvertStreetCode
from service.convert_village_code import ConvertVillageCode
from config.config import Config


class CrawlAddress(Thread):

    def __init__(self):
        super(CrawlAddress, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = Config()
        self.path_save_data = self.preprocess_directory_save_data(self.config.path_save_data.replace("\\", "/"))
        self.province_queue = Queue()
        self.district_queue_1 = Queue()
        self.district_queue_2 = Queue()
        self.flag_district = False
        self.number_crawler = self.config.number_of_crawler

    @staticmethod
    def preprocess_directory_save_data(directory):
        if re.search(r"(\\|\/)+$", directory) is None:
            return (directory + "/").strip()
        return directory

    def crawler_province(self):

        self.logger.info("START CRAWLER PROVINCE")
        province_code = GetProvinceCode(self.path_save_data)
        province_code.get_all_code_city()
        province_data = province_code.data
        province_code.save_data(province_data)
        convert_province = ConvertProvinceCode(self.path_save_data)
        convert_province.process_data()
        for each in convert_province.data:
            self.province_queue.put(each)
        self.logger.info("DONE PROVINCE")

    def crawler_district(self):
        if not self.province_queue.qsize():
            return
        province = self.province_queue.get()
        province_id = province["StandFor"]
        district_code = GetDistrictCode(province_id, self.path_save_data)
        district_code.province_code = province["ProvinceCode"]
        district_code.province_name = province["Province"]
        district_code.get_all_code_district()
        district_data = district_code.data
        district_code.save_data(district_data)

    def crawler_ward(self):
        if not self.district_queue_1.qsize():
            return
        district = self.district_queue_1.get()
        district_id = district["DistrictCodeBefore"]
        ward_code = GetWardCode(district_id, self.path_save_data)
        ward_code.code_city = district["StandFor"]
        ward_code.district_code = district["DistrictCode"]
        ward_code.district_name = district["District"]
        ward_code.province_code = district["ProvinceCode"]
        ward_code.province_name = district["Province"]
        ward_code.get_all_code_ward()
        ward_data = ward_code.data
        ward_code.save_data(ward_data)

    def crawler_street(self):
        if not self.district_queue_2.qsize():
            return
        district = self.district_queue_2.get()
        district_id = district["DistrictCodeBefore"]
        street_code = GetStreetCode(district_id, self.path_save_data)
        street_code.code_city = district["StandFor"]
        street_code.district_code = district["DistrictCode"]
        street_code.district_name = district["District"]
        street_code.province_code = district["ProvinceCode"]
        street_code.province_name = district["Province"]
        street_code.get_all_code_street()
        street_data = street_code.data
        street_code.save_data(street_data)

    def thread_crawler_district(self):
        self.logger.info("START CRAWLER DISTRICT")
        self.logger.info(f"NUMBER OF PROVINCE {self.province_queue.qsize()}")
        while True:
            if not self.province_queue.qsize():
                break
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.number_crawler) as executor:
                [executor.submit(self.crawler_district) for _ in range(self.number_crawler)]
            # time.sleep(10)
        self.flag_district = True

        # MERGE DISTRICT
        list_district = []
        folder_district = self.path_save_data + "districts/"
        list_file_data = os.listdir(folder_district)
        for each in list_file_data:
            data = json.load(open(folder_district + each, 'r', encoding="utf-8"))
            list_district.append(data)
        json.dump(list_district, open(self.path_save_data + "district.json", "w", encoding="utf-8"),
                  indent=4, ensure_ascii=False)
        convert_district = ConvertDistrictCode(self.path_save_data)
        convert_district.process_data()
        for each in convert_district.data:
            self.district_queue_1.put(each)
            self.district_queue_2.put(each)
        self.logger.info("FINISH CRAWLER DISTRICT")

    def thread_crawler_ward(self):
        self.logger.info("START CRAWLER WARD")
        while True:
            if (not self.district_queue_1.qsize()) and self.flag_district:
                break
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.number_crawler) as executor:
                [executor.submit(self.crawler_ward) for _ in range(self.number_crawler)]
            time.sleep(10)
        # MERGE WARD
        list_ward = []
        folder_ward = self.path_save_data + "wards/"
        list_file_data = os.listdir(folder_ward)
        for each in list_file_data:
            data = json.load(open(folder_ward + each, 'r', encoding="utf-8"))
            list_ward.append(data)
        json.dump(list_ward, open(self.path_save_data + "ward.json", "w", encoding="utf-8"),
                  indent=4, ensure_ascii=False)
        convert_ward = ConvertWardtCode(self.path_save_data)
        convert_ward.process_data()
        self.logger.info("FINISH CRAWLER WARD")

    def thread_crawler_street(self):
        self.logger.info("START CRAWLER STREET")
        while True:
            if (not self.district_queue_2.qsize()) and self.flag_district:
                break
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.number_crawler) as executor:
                [executor.submit(self.crawler_street) for _ in range(self.number_crawler)]
            # time.sleep(10)

        # MERGE STREET
        list_street = []
        folder_street = self.path_save_data + "streets/"
        list_file_data = os.listdir(folder_street)
        for each in list_file_data:
            data = json.load(open(folder_street + each, 'r', encoding="utf-8"))
            list_street.append(data)
        json.dump(list_street, open(self.path_save_data + "street.json", "w", encoding="utf-8"),
                  indent=4, ensure_ascii=False)
        self.logger.info("FINISH CRAWLER STREET")

    def thread_download_excel(self):
        self.logger.info("START DOWNLOAD ADDRESS FROM GOVERNMENT")
        download = DownloadExcelFile(self.path_save_data)
        download.download_file()
        self.logger.info("FINISH DOWNLOAD ADDRESS FROM GOVERNMENT")

    def create_folder_save_data(self):
        os.makedirs(self.path_save_data, exist_ok=True)

    def remove_file_not_use(self):
        list_file = os.listdir(self.path_save_data)
        # if "street.json" in list_file:
        #     os.remove(self.path_save_data + "street.json")
        if "provinces.json" in list_file:
            os.remove(self.path_save_data + "provinces.json")
        if "district.json" in list_file:
            os.remove(self.path_save_data + "district.json")
        if "streets" in list_file:
            shutil.rmtree(self.path_save_data + "streets")
        if "districts" in list_file:
            shutil.rmtree(self.path_save_data + "districts")
        for file_name in os.listdir(self.path_save_data):
            if re.search(r"^Danh", file_name) is not None:
                file_government = file_name
                os.remove(self.path_save_data + file_government)

    def run(self):
        self.logger.info(f"START SERVICE UPDATE ADDRESS")
        self.create_folder_save_data()
        # DOWNLOAD EXCEL
        download_excel_crawler = threading.Thread(target=self.thread_download_excel)
        download_excel_crawler.start()
        download_excel_crawler.join()
        # CRAWL PROVINCE
        province_crawler = threading.Thread(target=self.crawler_province)
        province_crawler.start()
        province_crawler.join()
        # CRAWL DISTRICT
        district_crawler = threading.Thread(target=self.thread_crawler_district)
        district_crawler.start()
        district_crawler.join()
        # CRAWL WARD AND STREET
        # ward_crawler = threading.Thread(target=self.thread_crawler_ward)
        # while not self.district_queue_1.qsize():
        #     pass
        # ward_crawler.start()
        # ward_crawler.join()
        street_crawler = threading.Thread(target=self.thread_crawler_street)
        while not self.district_queue_2.qsize():
            pass
        street_crawler.start()
        street_crawler.join()
        convert_street = ConvertStreetCode(self.path_save_data)
        thread_convert_street = threading.Thread(target=convert_street.convert_data)
        thread_convert_street.start()
        convert_village = ConvertVillageCode(self.path_save_data)
        thread_convert_village = threading.Thread(target=convert_village.convert_data)
        thread_convert_village.start()
        thread_convert_village.join()
        thread_convert_street.join()
        self.remove_file_not_use()


if __name__ == "__main__":
    crawler_address = CrawlAddress()
    crawler_address.start()
    crawler_address.join()


