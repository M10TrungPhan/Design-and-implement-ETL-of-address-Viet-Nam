import os
import random
import re
import pandas
import hashlib

from utils.utils import *
import pandas as pd
import json
from glob import glob


class ConvertStreetCode:

    def __init__(self, path_save_data: str):
        self.path_save_data = self.preprocess_directory_save_data(path_save_data)
        self.street_code_not_existed = []
        self.total_data = []
        self.street_file = None
        self.vn_road_excel = None
        self.hcm_road_excel = None
        self.address_government = None
        self.list_street_id = []
        self.list_street_code_not_selected = []
        self.list_street_code_existed = []
        self.number_total_code = 1000

    @staticmethod
    def preprocess_directory_save_data(directory):
        if re.search(r"(\\|\/)+$", directory) is None:
            return (directory + "/").strip()
        return directory

    def load_vn_street_xls_file(self):
        name_road_vn_file = "road_vn.xlsx"
        vn_road = pd.read_excel(self.path_save_data + name_road_vn_file)
        vn_road["name_road"] = vn_road["name_road"].apply(self.preprocess_street_name)
        vn_road["name_ward"] = vn_road["name_ward"].apply(self.preprocess_ward_name)
        vn_road["name_district"] = vn_road["name_district"].apply(self.preprocess_district_name)
        vn_road["name_province"] = vn_road["name_province"].apply(self.preprocess_province_name)
        self.vn_road_excel = vn_road
        return self.vn_road_excel

    def load_hcm_street_csv_file(self):
        name_road_hcm_file = "my_data.csv"
        hcm_road = pd.read_csv(self.path_save_data + name_road_hcm_file, delimiter=";")
        hcm_road["name_road"] = hcm_road["name_road"].apply(self.preprocess_street_name)
        hcm_road["name_ward"] = hcm_road["name_ward"].apply(self.preprocess_ward_name)
        hcm_road["name_district"] = hcm_road["name_district"].apply(self.preprocess_district_name)
        hcm_road["name_province"] = hcm_road["name_province"].apply(self.preprocess_province_name)
        self.hcm_road_excel = hcm_road
        return self.hcm_road_excel

    def load_street_json_file(self):
        name_street_file = "street.json"
        street_file = json.load(open(self.path_save_data + name_street_file, "r", encoding="utf-8"))
        self.street_file = street_file
        return self.street_file

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

    def load_list_code_existed(self):
        list_code_existed = []
        for each_data in self.total_data:
            list_code_existed.append(each_data["StreetCode"])
        self.list_street_code_existed = list_code_existed
        return self.list_street_code_existed

    def load_list_hash_street_existed(self):
        list_id_road = []
        for each_data in self.total_data:
            list_id_road.append(each_data["_id"])
        self.list_street_id = list_id_road
        return self.list_street_id

    def create_list_code(self):
        while True:
            if len(self.total_data) > self.number_total_code:
                self.number_total_code = self.number_total_code + 5000
            else:
                break
        if self.number_total_code < 5000:
            self.number_total_code = 5000
        self.list_street_code_not_selected = [i for i in range(self.number_total_code-5000, self.number_total_code) if i not in self.list_street_code_existed]
        return self.list_street_code_not_selected

    def add_more_code(self):
        number_total_code_new = self.number_total_code + 5000
        self.list_street_code_not_selected = self.list_street_code_not_selected + \
                                             [i for i in range(self.number_total_code, number_total_code_new)]
        # print(self.list_street_code_not_selected[0], self.list_street_code_not_selected[-1])
        self.number_total_code = number_total_code_new
        return self.list_street_code_not_selected

    def load_total_data(self):
        name_file = "final_street.json"
        if name_file in os.listdir(self.path_save_data):
            self.total_data = json.load(open(self.path_save_data + name_file, "r", encoding="utf-8"))
        else:
            self.total_data = []
        return self.total_data

    @staticmethod
    def find_address_similarity(address_name: str, list_address_compare: list, scores: float, n_gram=3):
        a_ngram = text_ngram(str(address_name), n_gram)
        max_score = 0
        address_select = address_name
        for each_address in list_address_compare:
            b_ngram = text_ngram(each_address, n_gram)
            score = jaccard_distance(a_ngram, b_ngram)
            if max_score < score:
                max_score = score
                address_select = each_address
        if max_score < scores:
            return None
        return address_select

    def select_address_dataframe(self, address_name, dataframe, address_level):
        list_address = dataframe[address_level].unique()
        if address_name in list_address:
            dataframe_for_address = dataframe.loc[dataframe[address_level] == address_name]
            return dataframe_for_address
        address_name = self.find_address_similarity(address_name, list_address, 0.5)
        if address_name is None:
            return None
        dataframe_for_address = dataframe.loc[dataframe[address_level] == address_name]
        return dataframe_for_address

    @staticmethod
    def preprocess_district_name(district_name):
        district_name = re.sub(r"^(Huyện đảo|Quận|Huyện|Thành phố|Thị Xã|Đảo)", "", str(district_name),
                               flags=re.IGNORECASE)
        return district_name.strip()

    @staticmethod
    def preprocess_province_name(province_name):
        province_name = re.sub(r"^(Tỉnh|Thành phố)", "", str(province_name), flags=re.IGNORECASE)
        return province_name.strip()

    @staticmethod
    def preprocess_ward_name(ward_name):
        ward_name = re.sub(r"^(Xã|Phường|Thị trấn)", "", str(ward_name), flags=re.IGNORECASE).strip()
        if re.search(r"^0", ward_name) is not None:
            ward_name = ward_name[1:]
        return ward_name.strip()

    @staticmethod
    def preprocess_street_name(street_name):
        street_name = str(street_name)
        street_name = re.sub(r"\d+[a-zA-Z]|kiệt(\s+(\d+(\\|\/)?)+)|huyện|quận|Thị xã|Đảo|Thành Phố"
                              r"|Xã|Thị trấn|Phường|(hẻm|hẽm|hèm|hem|hém|ngách|ngõ|ngỏ|đảo|đão)(\s+(\d+(\s?)+(\\|\/)?)+)?|"
                              r"\d+(\s?)+(\\|\/)(\s?)+\d+|[\\\/\-\.]",
                              "", street_name, flags=re.IGNORECASE).strip()
        if re.search(r"\d+$", street_name) is None:
            street_name = re.sub(r"\d+", "", street_name).strip()
        street_name = re.sub(r"\s{2,}", " ", street_name).strip()
        if re.search(r"^(số)|^(so)|^số", street_name) is not None:
            return "Error"
        if re.search(r"(\w+(\s)?\w?)+", street_name) is None:
            return "Error"
        if street_name.strip() == "đường":
            return "Error"
        if re.search(r"đường vào", street_name) is not None:
            return "Error"
        if len(street_name) < 4:
            return "Error"
        return street_name.strip()

    def process_data_street(self, data_government, data_collect):
        data_total_street = []
        number_road = 0
        province_government_not_process = data_government["Tỉnh / Thành Phố"].unique()
        province_government = [self.preprocess_province_name(each) for each in province_government_not_process]
        # LOOP FOR ALL PROVINCE
        for province in data_collect["name_province"].unique():
            # FIND CORRESPONDING PROVINCE IN GOVERMENT FILE
            province_similarity = self.find_address_similarity(province, province_government, 0.8)
            if province_similarity is None:
                continue
            province_similarity = province_government_not_process[province_government.index(province_similarity)]
            # SELECT DATA FRAME FOR EACH PROVINCE
            test_dataframe_province_goverment = self.select_address_dataframe(province_similarity, data_government,
                                                                         "Tỉnh / Thành Phố")
            test_dataframe_province_csv = self.select_address_dataframe(province, data_collect, "name_province")
            province_name = test_dataframe_province_goverment.iloc[0]["Tỉnh / Thành Phố"]
            province_code = int(test_dataframe_province_goverment.iloc[0]["Mã TP"])
            # print(province_name, province_code)
            # STATISTICAL DISTRICT IN PROVINCE
            district_in_province_goverment_not_process = test_dataframe_province_goverment["Quận Huyện"].unique()
            district_in_province_goverment = [self.preprocess_district_name(each) for each in
                                              district_in_province_goverment_not_process]
            district_in_csv = test_dataframe_province_csv["name_district"].unique()
            # LOOP FOR ALL DISTRICT IN PROVINCE IN CSV
            for district in district_in_csv:
                # FIND CORRESPONDING DISTRICT IN GOVERNMENT FILE
                district_similarity = self.find_address_similarity(district, district_in_province_goverment, 0.6)
                if district_similarity is None:
                    continue
                district_similarity = district_in_province_goverment_not_process[
                    district_in_province_goverment.index(district_similarity)]

                # SELECT DATAFRAME FOR EACH DISTRICT
                test_dataframe_district_goverment = self.select_address_dataframe(district_similarity,
                                                                             test_dataframe_province_goverment,
                                                                             "Quận Huyện")
                test_dataframe_district_collect = self.select_address_dataframe(district,
                                                                           test_dataframe_province_csv, "name_district")
                district_name = test_dataframe_district_goverment.iloc[0]["Quận Huyện"]
                district_code = int(test_dataframe_district_goverment.iloc[0]["Mã QH"])

                # STATISTICAL WARD IN DISTRICT
                ward_in_district_goverment_not_process = test_dataframe_district_goverment["Tên"].unique()
                ward_in_district_goverment = [self.preprocess_ward_name(each) for each in
                                              ward_in_district_goverment_not_process]
                ward_in_district_csv = test_dataframe_district_collect["name_ward"].unique()

                # LOOP FOR ALL WARD IN DISTRICT
                for ward in ward_in_district_csv:
                    # FIND CORRESPONDING WARD IN DISTRICT IN GOVERMENT FILE
                    ward_similarity = self.find_address_similarity(ward, ward_in_district_goverment, 0.6)
                    if ward_similarity is None:
                        continue
                    ward_similarity = ward_in_district_goverment_not_process[
                        ward_in_district_goverment.index(ward_similarity)]
                    # SELECT DATAFRAME FOR EACH WARD
                    test_dataframe_ward_goverment = self.select_address_dataframe(ward_similarity,
                                                                             test_dataframe_district_goverment, "Tên")
                    test_dataframe_ward_collect = self.select_address_dataframe(ward,
                                                                           test_dataframe_district_collect, "name_ward")
                    ward_name = test_dataframe_ward_goverment.iloc[0]["Tên"]
                    ward_code = int(test_dataframe_ward_goverment.iloc[0]["Mã"])
                    # STATISTICAL ALL STREET IN WARD
                    street_in_ward_collect = test_dataframe_ward_collect["name_road"].unique()
                    street_in_ward = [street for street in street_in_ward_collect if street != "Error"]
                    if not len(street_in_ward):
                        continue
                    list_new_street_in_ward = []
                    for each_street in street_in_ward:
                        new_data_street = {}
                        if self.find_address_similarity(each_street, list_new_street_in_ward, 0.7) is None:
                            list_new_street_in_ward.append(each_street)
                            new_data_street["Province"] = province_name
                            new_data_street["ProvinceCode"] = province_code
                            new_data_street["District"] = district_name
                            new_data_street["DistrictCode"] = district_code
                            new_data_street["Ward"] = ward_name
                            new_data_street["WardCode"] = ward_code
                            street_name = process_invalid_word(each_street)
                            new_data_street["Street"] = street_name.title()
                            string_hash = province_name + district_name + ward_name + new_data_street["Street"]
                            address_id = hashlib.md5(string_hash.encode("utf-8")).hexdigest()
                            if address_id in self.list_street_id:
                                continue
                            street_code = self.select_code_for_street()
                            if re.search(r"(Tháng|\/)", new_data_street["Street"], flags=re.IGNORECASE) is not None:
                                new_data_street["tags"] = create_alias_name(new_data_street["Street"])
                            else:
                                new_data_street["tags"] = []
                            new_data_street["StreetCode"] = street_code
                            new_data_street["_id"] = address_id
                            self.list_street_id.append(address_id)
                            data_total_street.append(new_data_street)
                            number_road += 1
        return data_total_street

    def select_code_for_street(self):
        if not len(self.list_street_code_not_selected):
            self.add_more_code()
        index = random.randint(0, len(self.list_street_code_not_selected) - 1)
        code_selected = self.list_street_code_not_selected[index]
        self.list_street_code_not_selected.remove(code_selected)
        self.list_street_code_existed.append(code_selected)
        return code_selected

    def process_data_street_in_bds(self):
        data_all_street_in_bds = []
        for each_district in self.street_file:
            for each_street in each_district:
                string_hash = each_street["Province"] + each_street["District"] + each_street["Street"]
                address_id = hashlib.md5(string_hash.encode("utf-8")).hexdigest()
                if address_id in self.list_street_id:
                    continue
                if re.search(r"(Tháng|\/)", each_street["Street"], flags=re.IGNORECASE) is not None:
                    each_street["tags"] = create_alias_name(each_street["Street"])
                else:
                    each_street["tags"] = []
                street_code = self.select_code_for_street()
                each_street["StreetCode"] = street_code
                each_street["_id"] = address_id
                self.list_street_id.append(address_id)
                data_all_street_in_bds.append(each_street)

        return data_all_street_in_bds

    def process_data_province(self):
        dataframe_province = self.address_government.drop_duplicates("Mã TP")
        data_province = []
        for each in range(len(dataframe_province)):
            each_province = dataframe_province.iloc[each]
            new_province = {"Province": each_province["Tỉnh / Thành Phố"], "ProvinceCode": int(each_province["Mã TP"])}
            data_province.append(new_province)
        self.save_data(data_province, "final_province.json")
        return data_province

    def process_data_district(self):
        dataframe_district = self.address_government.drop_duplicates("Mã QH")
        data_district = []
        for each in range(len(dataframe_district)):
            each_district = dataframe_district.iloc[each]
            new_district = {"Province": each_district["Tỉnh / Thành Phố"], "ProvinceCode": int(each_district["Mã TP"]),
                            "District": each_district["Quận Huyện"], "DistrictCode": int(each_district["Mã QH"])}
            data_district.append(new_district)
        self.save_data(data_district, "final_district.json")
        return data_district

    def process_data_ward(self):
        dataframe_ward = self.address_government
        data_ward = []
        for each in range(len(dataframe_ward)):
            each_ward = dataframe_ward.iloc[each]
            new_ward = {"Province": each_ward["Tỉnh / Thành Phố"], "ProvinceCode": int(each_ward["Mã TP"]),
                        "District": each_ward["Quận Huyện"], "DistrictCode": int(each_ward["Mã QH"])}
            try:
                ward_name = each_ward["Tên"]
                ward_name = re.sub(r"\b0(\d+)", r"\1", ward_name)
                new_ward["Ward"] = ward_name
                new_ward["WardCode"] = int(each_ward["Mã"])
            except:
                continue
            data_ward.append(new_ward)
        # print(data_district)
        self.save_data(data_ward, "final_ward.json")

    def save_data(self, data, name_file):
        json.dump(data, open(self.path_save_data + name_file, "w", encoding="utf-8"), indent=4, ensure_ascii=False)

    def convert_data(self):
        self.load_street_json_file()
        self.load_vn_street_xls_file()
        self.load_hcm_street_csv_file()
        self.load_address_government()
        self.process_data_province()
        self.process_data_district()
        self.process_data_ward()
        self.load_total_data()
        self.load_list_code_existed()
        self.load_list_hash_street_existed()
        self.create_list_code()
        self.total_data = self.total_data + self.process_data_street(self.address_government, self.vn_road_excel)
        self.total_data = self.total_data + self.process_data_street(self.address_government, self.hcm_road_excel)
        self.total_data = self.total_data + self.process_data_street_in_bds()
        self.save_data(self.total_data, "final_street.json")


if __name__ == "__main__":
    cvt = ConvertStreetCode(r"E:/test_street/")
    cvt.convert_data()
