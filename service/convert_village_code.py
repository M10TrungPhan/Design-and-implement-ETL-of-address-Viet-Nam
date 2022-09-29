import hashlib
import pandas as pd
import json
import re
import os
import random
from glob import glob
import logging

class ConvertVillageCode:

    def __init__(self, path_save_data: str):
        self.path_save_data = path_save_data
        self.logger = logging.getLogger(self.__class__.__name__)
        self.village_dataframe = None
        self.address_government = None
        self.province_dict = {}
        self.district_dict = {}
        self.ward_dict = {}
        self.total_data = []
        self.list_village_id = []
        self.number_total_code = 1000
        self.list_village_code_not_selected = []
        self.list_village_code_existed = []

    def load_village_json_file(self):
        name_village_file = "village.json"
        village_json = json.load(open(self.path_save_data + name_village_file, 'r', encoding="utf-8"))
        self.village_dataframe = pd.DataFrame(village_json)
        return self.village_dataframe

    def load_address_government(self):
        file_type = r'\*xls'
        files = glob(self.path_save_data + file_type)
        if not len(files):
            return None
        file_government = max(files, key=os.path.getctime)
        if re.search(r"^Danh", file_government) is None:
            for file_name in os.listdir(self.path_save_data):
                if re.search(r"^Danh", file_name) is not None:
                    file_government = file_name
                    break
        if file_government is None:
            return None
        self.address_government = pd.read_excel(self.path_save_data + file_government)
        return self.address_government

    def create_list_code(self):
        while True:
            if len(self.total_data) > self.number_total_code:
                self.number_total_code = self.number_total_code + 5000
                # print(self.number_total_code)
            else:
                break
        if self.number_total_code < 5000:
            self.number_total_code = 5000
        # print(f"CODE IN [{self.number_total_code-5000}:{self.number_total_code}]")
        self.list_village_code_not_selected = [i for i in range(self.number_total_code-5000, self.number_total_code)
                                               if i not in self.list_village_code_existed]
        return self.list_village_code_not_selected

    def add_more_code(self):
        number_total_code_new = self.number_total_code + 5000
        self.list_village_code_not_selected = self.list_village_code_not_selected + \
                                             [i for i in range(self.number_total_code, number_total_code_new)]
        # print(self.list_village_code_not_selected[0], self.list_village_code_not_selected[-1])
        self.number_total_code = number_total_code_new
        return self.list_village_code_not_selected

    def extract_province_dict(self):
        dataframe_province = self.address_government[["Mã TP", "Tỉnh / Thành Phố"]].drop_duplicates("Mã TP")
        dict_code_province = {}
        for each in range(len(dataframe_province)):
            each_province = dataframe_province.iloc[each]
            dict_code_province[int(each_province["Mã TP"])] = each_province["Tỉnh / Thành Phố"]
        self.province_dict = dict_code_province
        return self.province_dict

    def extract_district_dict(self):
        dataframe_district = self.address_government[["Mã QH", "Quận Huyện"]].drop_duplicates("Mã QH")
        dict_code_district = {}
        for each in range(len(dataframe_district)):
            each_district = dataframe_district.iloc[each]
            dict_code_district[int(each_district["Mã QH"])] = each_district["Quận Huyện"]
        self.district_dict = dict_code_district
        return self.district_dict

    def extract_ward_dict(self):
        dataframe_ward = self.address_government[["Mã", "Tên"]].drop_duplicates("Mã")
        dict_code_ward = {}
        for each in range(len(dataframe_ward)):
            each_ward = dataframe_ward.iloc[each]
            try:
                dict_code_ward[int(each_ward["Mã"])] = each_ward["Tên"]
            except:
                pass
        self.ward_dict = dict_code_ward
        return self.ward_dict

    def load_total_data(self):
        name_file = "final_village.json"
        if name_file in os.listdir(self.path_save_data):
            self.total_data = json.load(open(self.path_save_data + name_file, "r", encoding="utf-8"))
        else:
            self.total_data = []
        return self.total_data

    def load_list_code_existed(self):
        list_code_existed = []
        for each_data in self.total_data:
            list_code_existed.append(each_data["VillageCode"])
        self.list_village_code_existed = list_code_existed
        return self.list_village_code_existed

    def load_list_hash_village_existed(self):
        list_id_village = []
        for each_data in self.total_data:
            list_id_village.append(each_data["_id"])
        self.list_village_id = list_id_village
        return self.list_village_id

    def process_data_village(self):
        data_village = []
        for each in range(len(self.village_dataframe)):
            each_village = self.village_dataframe.iloc[each]
            new_village = {}
            new_village["Province"] = self.province_dict[int(each_village["ProvinceCode"])]
            new_village["ProvinceCode"] = int(each_village["ProvinceCode"])
            new_village["District"] = self.district_dict[int(each_village["DistrictCode"])]
            new_village["DistrictCode"] = int(each_village["DistrictCode"])
            try:
                new_village["Ward"] = self.ward_dict[int(each_village["WardCode"])]
            except:
                continue
            new_village["WardCode"] = int(each_village["WardCode"])
            new_village["Village"] = each_village["Lang"]

            hash_string = new_village["Province"] + new_village["District"] + new_village["Ward"] + new_village[
                "Village"]
            village_id = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
            if village_id in self.list_village_id:
                continue
            new_village["_id"] = village_id
            new_village["VillageCode"] = self.select_code_for_village()
            self.list_village_id.append(village_id)
            data_village.append(new_village)
        return data_village

    def select_code_for_village(self):
        if not len(self.list_village_code_not_selected):
            self.add_more_code()
            # print(f"Add more code. Number code not selected: {len(self.list_village_code_not_selected)}. "
            #       f"Number code selected: {len(self.list_village_code_existed)}")
        index = random.randint(0, len(self.list_village_code_not_selected) - 1)
        code_selected = self.list_village_code_not_selected[index]
        self.list_village_code_not_selected.remove(code_selected)
        self.list_village_code_existed.append(code_selected)
        return code_selected

    def save_data(self, data, name_file):
        json.dump(data, open(self.path_save_data + name_file, "w", encoding="utf-8"), indent=4, ensure_ascii=False)

    def convert_data(self):
        self.logger.info("START CONVERT VILLAGE CODE")
        self.load_total_data()
        self.load_village_json_file()
        self.load_address_government()
        self.load_list_code_existed()
        self.load_list_hash_village_existed()
        # print(len(self.list_village_id))
        # print(len(self.list_village_code_existed))
        self.create_list_code()
        # print(len(self.list_village_code_not_selected))
        self.extract_ward_dict()
        self.extract_district_dict()
        self.extract_province_dict()
        self.total_data = self.total_data + self.process_data_village()
        self.save_data(self.total_data, "final_village.json")
        self.logger.info("FINISH CONVERT VILLAGE CODE")


if __name__ == "__main__":
    cvt = ConvertVillageCode(r"E:/test_address_8/")
    cvt.convert_data()
