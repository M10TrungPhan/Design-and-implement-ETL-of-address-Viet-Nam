import os

from common.common_keys import *
from object.singleton import Singleton


class Config(metaclass=Singleton):
    path_save_data = os.getenv(PATH_SAVE_DATA, os.getcwd() + "/data")

    number_of_crawler = os.getenv(NUMBER_OF_CRAWLER, 30)
    logging_folder = "log/"
