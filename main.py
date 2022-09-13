import json

from service.get_provinces_code import GetProvinceCode
from service.get_district_in_city import GetDistrictCode
from service.get_street_in_district import GetStreetCode
from service.get_ward_in_district import GetWardCode

if __name__ == "__main__":
    print("GET ADDRESS")
    path_save_data = "E:/address/"
    province_code = GetProvinceCode(path_save_data)
    province_code.get_all_code_city()
    province_data = province_code.data
    province_code.save_data(province_code.data)
    print(len(province_data))
    print("_____________________________________")
    list_district_total = []
    list_ward_total = []
    list_street_total = []
    # ITERABLE EACH PROVINCE
    for each_province in province_data:
        province_id = each_province["StandFor"]
        dict_province = each_province
        # DISTRICT FOR PROVINCE
        list_district = []
        district_code = GetDistrictCode(province_id, path_save_data)
        district_code.get_all_code_district()
        district_data = district_code.data
        for each in district_data:
            district_data_new = {}
            district_data_new.update(each)
            district_data_new.update(each_province)
            list_district.append(district_data_new)
        list_district_total += list_district
        print("-------DISTRICT_IN_PROVINCE----------")
        print(len(list_district_total))
        # ITERABLE EACH DISTRICT
        for each_district in list_district:
            district_id = each_district["DistrictCode"]
            # WARD FOR DISTRICT
            list_ward = []
            ward_code = GetWardCode(district_id, path_save_data)
            ward_code.get_all_code_ward()
            ward_data = ward_code.data
            for each in ward_data:
                ward_data_new = {}
                ward_data_new.update(each)
                ward_data_new.update(each_district)
                list_ward.append(ward_data_new)
            list_ward_total += list_ward
            # STREET FOR DISTRICT
            list_street = []
            street_code = GetStreetCode(district_id, path_save_data)
            street_code.get_all_code_street()
            street_data = street_code.data
            for each in street_data:
                street_data_new = {}
                street_data_new.update(each)
                street_data_new.update(each_district)
                list_street.append(street_data_new)
            list_street_total += list_street
            print("-------WARD_IN_DISTRICT----------")
            print(len(list_ward_total))
            print("-------STREET_IN_DISTRICT----------")
            print(len(list_street_total))
            print("________________________________________________________________________")
        name_district = path_save_data +"district.json"
        json.dump(list_district_total, open(name_district, "w", encoding="utf-8"), indent=4, ensure_ascii=False)
        name_district = path_save_data +"ward.json"
        json.dump(list_ward_total, open(name_district, "w", encoding="utf-8"), indent=4, ensure_ascii=False)
        name_district = path_save_data +"street.json"
        json.dump(list_street_total, open(name_district, "w", encoding="utf-8"), indent=4, ensure_ascii=False)



