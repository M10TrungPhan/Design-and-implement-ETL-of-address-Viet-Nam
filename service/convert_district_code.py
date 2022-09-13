import pandas as pd
import json
import re
from glob import glob

from utils.utils import *


class ConvertDistrictCode:

    def __init__(self, path_save_data):
        self.folder_data = path_save_data
        self.data_frame = None
        self.district_file = None
        self.data = None

    def load_excel_file(self):
        file_type = r'\*xls'
        files = glob(self.folder_data + file_type)
        file_latest = max(files, key=os.path.getctime)
        excel_file = pd.read_excel(file_latest)
        self.data_frame = excel_file[["Mã TP", "Mã QH", "Quận Huyện"]].drop_duplicates("Mã QH")
        return self.data_frame

    def load_district_file(self):
        name_file = self.folder_data + "district.json"
        self.district_file = json.load(open(name_file, "r", encoding="utf-8"))

    def convert_data(self):
        if self.data_frame is None:
            return
        data_all_district = []
        for each_province in self.district_file:
            province_code = each_province[0]["ProvinceCode"]
            data_frame_province = self.data_frame.loc[self.data_frame["Mã TP"] == province_code]
            data_in_province = self. convert_district_code(each_province, data_frame_province)
            data_all_district += data_in_province
        self.data = data_all_district
        return self.data

    def convert_district_code(self, province, data_frame_province):
        data_district_in_province = []
        list_district_name = data_frame_province["Quận Huyện"].values
        list_district_name_processed = [self.preprocess_district_name(each) for each in list_district_name]
        for each_district in province:
            district_name_in_json = each_district["District"].strip()
            record_code = data_frame_province.loc[data_frame_province["Quận Huyện"] == district_name_in_json]
            if record_code.empty:
                district_name_processed = self.preprocess_district_name(district_name_in_json)
                district_name_find = self.find_district_similarity(district_name_processed,
                                                                   list_district_name_processed, 0.7, n_gram=1)
                if district_name_find is None:
                    continue
                district_name = list_district_name[list_district_name_processed.index(district_name_find)]
                record_code = data_frame_province.loc[data_frame_province["Quận Huyện"] == district_name]
            else:
                district_name = district_name_in_json
            code = record_code.iloc[0]["Mã QH"]
            each_district["District"] = district_name
            each_district["DistrictCodeBefore"] = each_district["DistrictCode"]
            each_district["DistrictCode"] = int(code)
            data_district_in_province.append(each_district)
        return data_district_in_province

    @staticmethod
    def preprocess_district_name(district_name):
        district_name = re.sub(r"Quận|Huyện|Thành phố|Thị xã|Đảo|Thành Phố|Thị Xã", "", district_name)
        return district_name.strip()

    @staticmethod
    def find_district_similarity(district_name: str, list_district_compare: list, scores: float, n_gram=3):
        a_ngram = text_ngram(district_name, n_gram)
        max_score = 0
        district_select = district_name
        for each_province in list_district_compare:
            b_ngram = text_ngram(each_province, n_gram)
            score = jaccard_distance(a_ngram, b_ngram)
            if max_score < score:
                max_score = score
                district_select = each_province
        if max_score < scores:
            return None
        return district_select

    def save_data_after_process(self):
        name_file = self.folder_data + "district.json"
        json.dump(self.data, open(name_file, "w", encoding="utf-8"), indent=4, ensure_ascii=False)

    def process_data(self):
        self.load_excel_file()
        self.load_district_file()
        self.convert_data()
        self.save_data_after_process()




