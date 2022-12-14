import pandas as pd
import json
import re
from glob import glob

from utils.utils import *


class ConvertWardCodeType2:

    def __init__(self, path_save_data):
        self.folder_data = path_save_data
        self.data_government = None
        self.export_address_data_frame = None
        self.data = None
        self.data_2 = None
        self.dict_ward_alias = {}

    def load_excel_file(self):
        name_file = self.folder_data + "gov.xls"
        excel_file = pd.read_excel(name_file)
        self.data_government = excel_file
        return self.data_government

    def load_ward_file(self):
        name_file = self.folder_data + "Export-Adress.xlsx"
        self.export_address_data_frame = pd.read_excel(name_file)

    def load_alias_name_ward(self):
        dict_ward_alias = {}
        name_file = "alias_ward.json"
        if name_file in os.listdir(self.folder_data):
            alias_street = json.load(open(self.folder_data + name_file, "r", encoding="utf-8"))
        else:
            alias_street = []
        for each in alias_street:
            list_alias = each["tags"].copy()
            list_alias_new = []
            for al in list_alias:
                if len(al) <= 2:
                    continue
                else:
                    list_alias_new.append(al)
            # list_alias_new = [alias for alias in list_alias if len(alias) > 2]
            dict_ward_alias[int(each["WardCode"])] = list_alias_new
        self.dict_ward_alias = dict_ward_alias
        print(self.dict_ward_alias)
        return self.dict_ward_alias


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
            # print(address_select)
            return None
        return address_select

    def convert_data(self):
        data_total_ward = []
        number_ward = 0
        number_district = 0
        number_ward_alias = 0
        province_government_not_process = self.data_government["Tỉnh / Thành Phố"].unique()
        province_government = [self.preprocess_province_name(each) for each in province_government_not_process]
        # LOOP FOR ALL PROVINCE
        for province in self.export_address_data_frame["Province"].unique():
            # FIND CORRESPONDING PROVINCE IN GOVERNMENT FILE
            province_similarity = self.find_address_similarity(province, province_government, 0.8)

            if province_similarity is None:
                continue
            province_similarity = province_government_not_process[province_government.index(province_similarity)]
            # continue
            # SELECT DATA FRAME FOR EACH PROVINCE
            test_dataframe_province_goverment = self.select_address_dataframe(province_similarity, self.data_government,
                                                                         "Tỉnh / Thành Phố")
            test_dataframe_province_csv = self.select_address_dataframe(province, self.export_address_data_frame, "Province")
            province_name = test_dataframe_province_goverment.iloc[0]["Tỉnh / Thành Phố"]
            province_code = int(test_dataframe_province_goverment.iloc[0]["Mã TP"])
            # print(province_name, province_code)
            # continue
            # STATISTICAL DISTRICT IN PROVINCE

            district_in_province_goverment_not_process = test_dataframe_province_goverment["Quận Huyện"].unique()
            # district_in_province_goverment = [self.preprocess_district_name(each) for each in
            #                                   district_in_province_goverment_not_process]
            district_in_csv = test_dataframe_province_csv["District"].unique()

            # continue
            # LOOP FOR ALL DISTRICT IN PROVINCE IN CSV
            for district in district_in_csv:
                # FIND CORRESPONDING DISTRICT IN GOVERNMENT FILE
                district_similarity = self.find_address_similarity(district, district_in_province_goverment_not_process, 0.6)
                if district_similarity is None:
                    continue
                number_district += 1
                # print(district, district_similarity)
                # continue
                # print(district_similarity)
                # continue
                # SELECT DATAFRAME FOR EACH DISTRICT
                test_dataframe_district_goverment = self.select_address_dataframe(district_similarity,
                                                                             test_dataframe_province_goverment,
                                                                             "Quận Huyện")
                test_dataframe_district_collect = self.select_address_dataframe(district,
                                                                           test_dataframe_province_csv, "District")
                district_name = test_dataframe_district_goverment.iloc[0]["Quận Huyện"]
                district_code = int(test_dataframe_district_goverment.iloc[0]["Mã QH"])
                # print(district_code, district_name)
                # continue
                # STATISTICAL WARD IN DISTRICT
                ward_in_district_goverment_not_process = test_dataframe_district_goverment["Tên"].unique()
                ward_in_district_goverment = [str(each) for each in
                                              ward_in_district_goverment_not_process]
                ward_in_district_csv = test_dataframe_district_collect["Area"].unique()
                # print(province_name, district_name)
                # print(ward_in_district_csv)

                # print("___________")

                # # LOOP FOR ALL WARD IN DISTRICT
                for ward_code_string in ward_in_district_csv:
                    # FIND CORRESPONDING WARD IN DISTRICT IN GOVERNMENT FILE
                    new_ward={}
                    code_string, ward_name = self.split_ward_and_code_from_address(ward_code_string)
                    province_code_sub, district_code_sub, ward_code_sub = self.split_code_address(code_string)
                    # print(province_code_sub, district_code_sub, ward_code_sub, ward_name)
                    # continue
                    # print(ward_name)
                    ward_similarity = self.find_address_similarity(ward_name, ward_in_district_goverment, 0.6)
                    if ward_similarity is None:
                        # print(ward_name, district_name, province_name)
                        # print("_____")
                        continue

                    # ward_similarity = ward_in_district_goverment_not_process[
                    #     ward_in_district_goverment_not_process.index(ward_similarity)]

                    # print(ward_name, ward_similarity)
                    # # SELECT DATAFRAME FOR EACH WARD
                    test_dataframe_ward_goverment = self.select_address_dataframe(ward_similarity,
                                                                             test_dataframe_district_goverment, "Tên")
                    ward_code = int(test_dataframe_ward_goverment.iloc[0]["Mã"])
                    new_ward["ProvinceCode"] = province_code
                    new_ward["Province"] = province_name
                    new_ward["DistrictCode"] = district_code
                    new_ward["District"] = district_name
                    new_ward["Ward"] = ward_name
                    new_ward["WardCode"] = ward_code
                    new_ward["ProvinceSubCode"] = province_code_sub
                    new_ward["DistrictSubCode"] = district_code_sub
                    new_ward["WardSubCode"] = ward_code_sub

                    new_ward["MergeCode"] = code_string
                    try:
                        new_ward["tags"] = self.dict_ward_alias[ward_code]
                        number_ward_alias += 1
                    except:
                        pass
                    data_total_ward.append(new_ward)
                    number_ward += 1

                    # test_dataframe_ward_collect = self.select_address_dataframe(ward_name,
                    #                                                        test_dataframe_district_collect, "name_ward")

        print(number_ward)
        print(number_district)
        print(number_ward_alias)
        return data_total_ward


    @staticmethod
    def split_ward_and_code_from_address(input_string):
        input_string = input_string.strip()
        regex_result = re.search(r"\-\w+$", input_string)
        if regex_result is not None:
            start, end = regex_result.span()
            code_string = input_string[start + 1:]
            ward_name = input_string[:start]
            return code_string, ward_name

    @staticmethod
    def split_code_address(code_string):
        regex_result = re.search(r"\d+[A-Z]+", code_string)
        if regex_result is not None:
            start, end = regex_result.span()
            ward_code = code_string[end:]
            code_string_2 = code_string[:end]
            regex_result_2 = re.search(r"[A-Z]+$", code_string_2)
            if regex_result_2 is not None:
                start, end = regex_result_2.span()
                district_code = code_string_2[start:]
                province_code = code_string_2[:start]
                return province_code, district_code, ward_code
            else:
                return None, None, None
        return None, None, None

    def convert_ward_code(self):
        pass

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
        self.load_alias_name_ward()
        data_total_ward = self.convert_data()
        json.dump(data_total_ward, open(self.folder_data +"ward_code_sub.json", "w", encoding="utf-8"),
                  indent=4, ensure_ascii=False)
        # self.extract_ward_dict()
        # self.extract_province_dict()
        # self.extract_district_dict()


if __name__ == "__main__":
    ConvertWard = ConvertWardCodeType2(r"E:\adddress\\")
    ConvertWard.process_data()
    # print(ConvertWard.district_dict)