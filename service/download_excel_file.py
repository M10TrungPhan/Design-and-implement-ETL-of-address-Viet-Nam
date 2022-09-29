import time

from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import threading
from threading import Thread


class DownloadExcelFile:

    def __init__(self, path_save):
        self.path_save_data = path_save.replace("/", "\\")
        self.driver = None
        self.url = "https://danhmuchanhchinh.gso.gov.vn/"

    def setup_selenium_firefox(self):
        ser = Service("driverbrowser/geckodriver.exe")
        firefox_options = FirefoxOptions()
        firefox_options.set_preference("media.volume_scale", "0.0")
        firefox_options.set_preference('devtools.jsonview.enabled', False)
        firefox_options.set_preference('dom.webnotifications.enabled', False)

        firefox_options.set_preference("browser.download.folderList", 2)
        firefox_options.set_preference("browser.download.manager.showWhenStarting", False)
        firefox_options.set_preference("browser.download.dir", self.path_save_data)
        # Example:profile.set_preference("browser.download.dir", "C:\Tutorial\down")
        firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel")
        firefox_options.add_argument("--test-type")
        firefox_options.add_argument('--ignore-certificate-errors')
        firefox_options.add_argument('--disable-extensions')
        firefox_options.add_argument('disable-infobars')
        firefox_options.add_argument("--incognito")
        firefox_options.add_argument("--headless")
        driver = webdriver.Firefox(service=ser, options=firefox_options)
        return driver

    def download_file(self):
        while True:
            try:
                self.driver = self.setup_selenium_firefox()
                self.driver.get(self.url)
                self.driver.find_element(By.CLASS_NAME, "dxeButtonEditButton_Office2003_Blue").click() # CLICK VIEW MORE LEVEL
                time.sleep(2)
                break
            except:
                self.driver.close()

        list_tag = self.driver.find_elements(By.CLASS_NAME, "dxeListBoxItemRow_Office2003_Blue") # LIST LEVEL
        list_tag[-1].click()    # SELECT LEVEL
        time.sleep(2)
        self.driver.find_element(By.CLASS_NAME, "dxb").click()  # CLICK FIND INFORMATION
        time.sleep(2)
        list_button = self.driver.find_elements(By.CLASS_NAME, "dxbButton_Office2003_Blue")
        list_button[-1].click() # CLICK DOWNLOAD
        time.sleep(20)
        self.driver.close()


if __name__ == "__main__":
    download = DownloadExcelFile("E:/test_address_3/")
    download.download_file()

