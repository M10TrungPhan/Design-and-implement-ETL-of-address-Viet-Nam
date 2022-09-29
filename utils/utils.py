import os
import logging
import string
import re

from nltk import ngrams
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from config.config import Config


def setup_selenium_firefox():
    ser = Service("driverbrowser/geckodriver.exe")
    firefox_options = FirefoxOptions()
    firefox_options.set_preference("media.volume_scale", "0.0")
    firefox_options.set_preference('devtools.jsonview.enabled', False)
    firefox_options.set_preference('dom.webnotifications.enabled', False)
    firefox_options.add_argument("--test-type")
    firefox_options.add_argument('--ignore-certificate-errors')
    firefox_options.add_argument('--disable-extensions')
    firefox_options.add_argument('disable-infobars')
    firefox_options.add_argument("--incognito")
    firefox_options.add_argument("--headless")
    driver = webdriver.Firefox(service=ser, options=firefox_options)
    return driver


dict_invalid_word = {"dủổng": "đường"}

SRC_ACCENT = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗ' \
             u'ỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
DST_ACCENT = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOo' \
             u'OoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'

int_to_string = {"1": "Một", "2": "Hai", "3": "Ba", "4": "Tư", "5": "Năm", "6": "Sáu", "7": "Bảy", "8": "Tám",
                 "9": "Chín", "10": "Mười", "11": "Mười Một", "12": "Mười Hai", '13': "Mười Ba", '14': "Mười Bốn",
                 '15': "Mười Lăm", '16': "Mười Sáu", '17': "Mười Bảy", '18': "Mười Tám", '19': "Mười Chín",
                 '20': "Hai Mươi", '21': "Hai Mốt", '22': "Hai Hai", '23': "Hai Ba", '24': "Hai Tư", '25': "Hai Lăm",
                 '26': "Hai Sáu", '27': "Hai Bảy", '28': "Hai Tám", '29': "Hai Chín", '30': "Ba Mươi", '31': "Ba Mốt"}

string_to_int = {}
for each in int_to_string.items():
    string_to_int[each[1]] = each[0]


def create_alias_name(name_road):
    name_road = re.sub(r"^Đường", "", name_road).strip()
    list_before = []
    list_after = []
    list_alias = []
    start, end = re.search(r"(Tháng|\/)", name_road, flags=re.IGNORECASE).span()
    before = name_road[0:start].strip()
    after = name_road[end:].strip()
    list_before.append(before)
    list_after.append(after)
    if before in string_to_int.keys():
        list_before.append(string_to_int[before])
    elif before in int_to_string.keys():
        list_before.append(int_to_string[before])
    if after in string_to_int.keys():
        list_after.append(string_to_int[after])
    elif after in int_to_string.keys():
        list_after.append(int_to_string[after])
    for each_before in list_before:
        for each_after in list_after:
            list_alias.append(each_before + " Tháng " + each_after)
    return list_alias


def remove_accents(input_str):
    s = ''
    input_str.encode('utf-8')
    for c in input_str:
        if c in SRC_ACCENT:
            s += DST_ACCENT[SRC_ACCENT.index(c)]
        else:
            s += c
    return s


def normalize_text(text):
    if type(text) != str:
        return text
    text = text.lower()
    for p in string.punctuation:
        text = text.replace(p, f" ")
    text = remove_accents(text)
    while "  " in text:
        text = text.replace("  ", " ")
    return text.strip()


def text_ngram(text, n, c_mode=True):
    output = set()
    text = normalize_text(text)
    if c_mode:
        for n_gram in range(n):
            output = output.union(set(ngrams(text.lower(), n_gram + 1)))
    else:
        for n_gram in range(n):
            output = output.union(set(ngrams(text.lower().split(), n_gram + 1)))
    return output


def jaccard_distance(a, b, return_num=False, re_scoring_terms=None):
    if re_scoring_terms is None:
        re_scoring_terms = {}

    def calculate_total_score(candidate_set):
        return sum(1 - re_scoring_terms.get(item, 0) for item in candidate_set)

    nominator = a.intersection(b)
    if return_num:
        return calculate_total_score(nominator)
    else:
        denominator = a.union(b)
        similarity = calculate_total_score(nominator) / calculate_total_score(denominator)
        return similarity


def process_invalid_word(word):
    word = word.lower()
    word_after_process = ""
    for each in word.split():
        if each in dict_invalid_word.keys():
            word_after_process = word_after_process + dict_invalid_word[each] + " "
        else:
            word_after_process = word_after_process + each + " "
    word_after_process = re.sub(r"\s{2,}", " ", word_after_process)
    return word_after_process.strip()


def setup_logging():
    config = Config()
    os.makedirs(config.logging_folder, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | [%(levelname)s] | %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(config.logging_folder, "crawl_data.log"), encoding="utf8"),
            logging.StreamHandler()
        ]
    )

setup_logging()