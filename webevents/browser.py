import globals.global_settings as gls
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

def build_driver():
    if gls.BROWSER_TYPE == 'FIREFOX':
        service = FirefoxService(gls.GECKO_EXE_LOC)
        options = webdriver.FirefoxOptions()
        options.binary_location = gls.BROWSER_EXE_LOC
    elif gls.BROWSER_TYPE == 'CHROME':
        service = ChromeService(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
    options.add_argument("--log-level=3")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'--user-data-dir={gls.BROWSER_PROFILE_DIR}')
    options.add_argument('--headless')
    options.add_argument("--window-size=0,0")
    if gls.BROWSER_TYPE == 'FIREFOX':
        return webdriver.Firefox(service=service, options=options)
    return webdriver.Chrome(service=service, options=options)
