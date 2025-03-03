from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
import time
import globals.global_settings as gls
from globals import global_utils as mu

print("Loading.... get_latest_injurys")

def find_injury_news():
    service = None
    options = None
    browser_driver = None
    if gls.BROWSER_TYPE == 'FIREFOX':
        service = FirefoxService(gls.GECKO_EXE_LOC)
        options = webdriver.FirefoxOptions()
        options.binary_location = gls.BROWSER_EXE_LOC
    elif gls.BROWSER_TYPE == 'CHOME':
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
    options.add_argument("--log-level=3")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'--user-data-dir={gls.BROWSER_PROFILE_DIR}')
    options.add_argument('--headless')
    options.add_argument('--headless')
    options.add_argument("--window-size=0,0")

    if gls.BROWSER_TYPE == 'FIREFOX':
        browser_driver = webdriver.Firefox(service=service, options=options)
    elif gls.BROWSER_TYPE == 'CHROME':
        browser_driver = webdriver.Chrome(service=service, options=options)
    with browser_driver as driver:
        print(gls.ESPN_NBA_INJURY_URL)
        driver.get(gls.ESPN_NBA_INJURY_URL)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        print('Retreived page_source')
        pretty_page_source = soup.prettify()
        with open(gls.INJURY_SOURCE_HTML, 'w', encoding='utf-8') as file:
            file.write(pretty_page_source)
        tables = soup.find_all('div', class_='Table__league-injuries')
        injuries_data = []
        for table in tables:
            team_name = table.find('span', class_='injuries__teamName').text
            rows = table.find_all('tr', class_='Table__TR--sm')
            for row in rows:
                player_name = row.find('td', class_='col-name').text.strip().replace('.', '')
                player_name = mu.remove_other_suffixs(player_name)
                pos = row.find('td', class_='col-pos').text.strip()
                est_return_date = row.find('td', class_='col-date').text.strip()
                status = row.find('td', class_='col-stat').text.strip()
                comment = row.find('td', class_='col-desc').text.strip()
                parts = team_name.split(' ')
                name_to_check = parts[-1]
                team_short = gls.TEAM_TOSHORT_MAPPER.get(name_to_check, None)
                if team_short is None:
                    print(f'Unknown Shortname for Team {team_name}')
                    exit(0)
                injuries_data.append({
                    'Team': team_name,
                    'Short Name': team_short,
                    'Player Name': player_name,
                    'Position': pos,
                    'Est. Return Date': est_return_date,
                    'Status': status,
                    'Tracked': False,
                    'Comment': comment
                })
        if len(injuries_data) > 0:
            injuries_df = pd.DataFrame(injuries_data)
            injuries_df.to_csv(gls.CURRENT_INJURY_FULLPATH, index=False)
            print(f"CSV file '{gls.CURRENT_INJURY_FULLPATH}' has been created successfully.")
        else:
            print('No injury news found...')
        return injuries_data

print("Loaded.... get_latest_injurys")