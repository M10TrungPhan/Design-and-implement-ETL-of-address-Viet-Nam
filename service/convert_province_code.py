from utils.utils import *
import pandas as pd
import json
import re
from glob import glob


class ConvertProvinceCode:

    def __init__(self, path_save_data):
        self.folder_data = path_save_data
        self.data_frame = None
        self.province_file = None
        self.data = None

    def load_excel_file(self):
        file_type = r'\*xls'
        files = glob(self.folder_data + file_type)
        file_latest = max(files, key=os.path.getctime)
        excel_file = pd.read_excel(file_latest)
        self.data_frame = excel_file[["Tỉnh / Thành Phố", "Mã TP"]].drop_duplicates("Mã TP")

    def load_province_file(self):
        name_file = self.folder_data + "provinces.json"
        self.province_file = json.load(open(name_file, "r", encoding="utf-8"))

    def convert_data(self):
        if self.data_frame is None:
            return
        data_province = []
        list_province_name_in_excel = self.data_frame["Tỉnh / Thành Phố"].values
        for each_province in self.province_file:
            province_name = each_province["Province"]
            record_code = self.data_frame.loc[self.data_frame["Tỉnh / Thành Phố"] == province_name]
            if record_code.empty:
                province_name = self.find_province_similarity(province_name, list_province_name_in_excel)
                record_code = self.data_frame.loc[self.data_frame["Tỉnh / Thành Phố"] == province_name]
            code = record_code.iloc[0]["Mã TP"]
            each_province["ProvinceCode"] = int(code)
            data_province.append(each_province)
        self.data = data_province
        return self.data

    @staticmethod
    def find_province_similarity(province_name: str, list_province_compare:list, n_gram= 3):
        a_ngram = text_ngram(province_name, n_gram)
        max_score = 0
        province_select = province_name
        for each_province in list_province_compare:
            b_ngram = text_ngram(each_province, n_gram)
            score = jaccard_distance(a_ngram, b_ngram)
            if max_score < score:
                max_score = score
                province_select = each_province
        return province_select

    def save_data_after_process(self):
        name_file = self.folder_data + "provinces.json"
        json.dump(self.data, open(name_file, "w", encoding="utf-8"), indent=4, ensure_ascii=False)

    def process_data(self):
        self.load_excel_file()
        self.load_province_file()
        self.convert_data()
        self.save_data_after_process()


if __name__ == "__main__":
    cvtProvince = ConvertProvinceCode()
    cvtProvince.load_excel_file()
    cvtProvince.load_province_file()
    cvtProvince.convert_data()


