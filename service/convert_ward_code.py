import pandas as pd
import json
import re
from glob import glob

from utils.utils import *


class ConvertWardtCode:

    def __init__(self, path_save_data):
        self.folder_data = path_save_data
        self.data_frame = None
        self.ward_file = None
        self.data = None
        self.data_2 = None

    def load_excel_file(self):
        file_type = r'\*xls'
        files = glob(self.folder_data + file_type)
        file_latest = max(files, key=os.path.getctime)
        excel_file = pd.read_excel(file_latest)
        self.data_frame = excel_file[["Mã QH", "Mã", "Tên"]]
        return self.data_frame

    def load_ward_file(self):
        name_file = self.folder_data + "ward.json"
        self.ward_file = json.load(open(name_file, "r", encoding="utf-8"))

    @staticmethod
    def preprocess_ward_name(ward_name):
        ward_name = re.sub(r"Xã|Thị trấn|Huyện|Thị Trấn|Phường", "", ward_name)
        return ward_name.strip()

    def convert_data(self):
        data_all_ward = []
        data_all_ward_2 = []
        for each_district in self.ward_file:
            if not len(each_district):
                continue
            district_code = each_district[0]["DistrictCode"]
            data_frame_district = self.data_frame.loc[self.data_frame["Mã QH"] == district_code]
            data_in_district = self.convert_ward_code(each_district, data_frame_district)
            if not len(data_in_district):
                continue
            data_all_ward += data_in_district
            data_all_ward_2.append(data_in_district)
        self.data = data_all_ward
        self.data_2 = data_all_ward_2
        return self.data

    def convert_ward_code(self, district, data_frame_district):
        data_ward_in_district = []
        list_ward_name = data_frame_district["Tên"].values
        try:
            list_ward_name_after_process = [self.preprocess_ward_name(each) for each in list_ward_name]
        except:
            return []
        for each_ward in district:
            ward_name_in_json = each_ward["Ward"].strip()
            record_code = data_frame_district.loc[data_frame_district["Tên"] == ward_name_in_json]
            if record_code.empty:
                ward_name_after_process = self.preprocess_ward_name(ward_name_in_json)
                ward_name_find = self.find_ward_similarity(ward_name_after_process, list_ward_name_after_process, 0.7,
                                                           n_gram=1)
                if ward_name_find is None:
                    continue
                ward_name = list_ward_name[list_ward_name_after_process.index(ward_name_find)]
                record_code = data_frame_district.loc[data_frame_district["Tên"] == ward_name]
            else:
                ward_name = ward_name_in_json
            code = record_code.iloc[0]["Mã"]
            each_ward["Ward"] = ward_name
            each_ward["WardCode"] = int(code)
            data_ward_in_district.append(each_ward)
        return data_ward_in_district

    @staticmethod
    def find_ward_similarity(ward_name: str, list_ward_compare: list, scores: float, n_gram=3):
        a_ngram = text_ngram(ward_name, n_gram)
        max_score = 0
        ward_select = ward_name
        for each_ward in list_ward_compare:
            b_ngram = text_ngram(each_ward, n_gram)
            score = jaccard_distance(a_ngram, b_ngram)
            if max_score < score:
                max_score = score
                ward_select = each_ward
        if max_score < scores:
            return None
        return ward_select

    def save_data_after_process(self):
        name_file = self.folder_data + "ward_after_process.json"
        json.dump(self.data, open(name_file, "w", encoding="utf-8"), indent=4, ensure_ascii=False)
        name_file = self.folder_data + "ward_after_process_2.json"
        json.dump(self.data_2, open(name_file, "w", encoding="utf-8"), indent=4, ensure_ascii=False)

    def process_data(self):
        self.load_excel_file()
        self.load_ward_file()
        self.convert_data()
        self.save_data_after_process()
