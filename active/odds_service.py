import os
import json
import pytz
import time
import logging
import datetime
import subprocess
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',filename='C:/Files/active/data/service_log.txt')

logger = logging.getLogger()
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

logger.info('Starting...')
SPORTSBOOK_API_URL = 'https://sbapi.in.sportsbook.fanduel.com/api/'
BROWSER_PROFILE_DIR = os.getenv('BROWSER_PROFILE_DIR', '')
BROWSER_EXE_LOC = os.getenv('BROWSER_EXE_LOC', '')
GECKO_EXE_LOC = os.getenv('GECKO_EXE_LOC', '')
BROWSER_TYPE = 'FIREFOX'
MAIN_DB_CREDS = 'C:/Files/active/data/settings.json'
POWERSHELL_CRED_DELETE = 'C:/Files/active/data/delete_tempcreds.ps1'
SPORTSBOOK_ODDS_TABLE = 'sportsbook_odds'
TEAM_TOSHORT_MAPPER = {"Hawks": "ATL","Celtics": "BOS","Nets": "BKN","Hornets": "CHA","Bobcats": "CHA","Bulls": "CHI", "Cavaliers": "CLE","Mavericks": "DAL","Nuggets": "DEN","Pistons": "DET","Warriors": "GSW","Rockets": "HOU","Pacers": "IND","Clippers": "LAC","Lakers": "LAL","Grizzlies": "MEM","Heat": "MIA","Bucks": "MIL","Timberwolves": "MIN","Pelicans": "NOP","Knicks": "NYK","Thunder": "OKC","Sonics": "OKC","Magic": "ORL","76ers": "PHI","Suns": "PHX","Trail Blazers": "POR","Blazers": "POR","Kings": "SAC","Spurs": "SAS","Raptors": "TOR","Jazz": "UTA","Wizards": "WAS"}

with open(MAIN_DB_CREDS, 'r') as json_file:
    settings = json.load(json_file)
mys_sv = eventc_value = settings.get('my_sql_s')
mys_us = eventc_value = settings.get('my_sql_u')
mys_ps = eventc_value = settings.get('my_sql_p')

event_type = 'nba'
outbound_odds = 'odds_service_data.csv'
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_time = datetime.datetime.now()
utc_timezone = pytz.timezone('UTC')
est_timezone = pytz.timezone('US/Eastern')

service = FirefoxService(GECKO_EXE_LOC)
options = webdriver.FirefoxOptions()
options.binary_location = BROWSER_EXE_LOC
options.add_argument("--log-level=3")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument(f'--user-data-dir={BROWSER_PROFILE_DIR}')
options.add_argument('--headless')
options.add_argument("--window-size=0,0")
browser_driver = None

def find_upcoming_games():
    global browser_driver
    extracted_data = []
    browser_driver = webdriver.Firefox(service=service, options=options)
    with browser_driver as driver:
        lookup_url = f'{SPORTSBOOK_API_URL}content-managed-page?currencyCode=USD&exchangeLocale=en_US&includePrices=true&language=en&regionCode=NAMERICA&timezone=America%2FNew_York&_ak=FhMFpcPWXMeyZxOx&page=CUSTOM&customPageId={event_type}'
        logger.info(f'url: {lookup_url}')
        driver.get(lookup_url)
        if BROWSER_TYPE == 'FIREFOX':
            wait = WebDriverWait(driver, 5)
            view_raw_button = wait.until(EC.element_to_be_clickable((By.ID, 'rawdata-tab')))
            view_raw_button.click()
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        try:
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'pre')))
            pre_tag = soup.find('pre')
            if pre_tag:
                json_string = pre_tag.text
                json_data = json.loads(json_string)
                for event_info in json_data['attachments']['events'].values():
                    event_name = event_info['name']
                    logger.info(f'Found event: {event_name}')
                    if ' @ ' in event_info['name'] and not event_info['name'].startswith("West @ East"):
                        split_result = event_info['name'].split(' @ ')
                        last_words = [split.strip().split()[-1] for split in split_result]
                        away_team = last_words[0]
                        home_team = last_words[1]
                        away_short = TEAM_TOSHORT_MAPPER.get(away_team, None)
                        home_short = TEAM_TOSHORT_MAPPER.get(home_team, None)
                        opn_date = event_info['openDate']
                        formatdate = datetime.datetime.strptime(opn_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                        given_time_utc = utc_timezone.localize(formatdate)
                        given_time_est = given_time_utc.astimezone(est_timezone).replace(tzinfo=None)
                        extracted_data.append({
                            'eventId': event_info['eventId'],
                            'name': event_info['name'],
                            'eventTypeId': event_info['eventTypeId'],
                            'competitionId': event_info['competitionId'],
                            'home_team': [home_team, home_short],
                            'away_team': [away_team, away_short],
                            'game_id': event_info['eventId'],
                            'open_date': given_time_est
                        })
        except json.JSONDecodeError as e:
            logger.debug(f"Error decoding JSON from page source for event ID {event_info['eventId']}: {e}")
        except Exception as e:
            logger.debug(f"An error occurred for event ID {event_info['eventId']}: {e}")
    if len(extracted_data) > 0:
        logger.info('Got main event data...')
    else:
        logger.info('No new upcoming events or games found...')
    return extracted_data

def fetch_and_process_event_data_with_selenium(ex_data):
    global browser_driver
    extracted_data = []
    browser_driver = webdriver.Firefox(service=service, options=options)
    with browser_driver as driver:
        for event in ex_data:
            event_id = event['eventId']
            event_name = event['name']
            open_date = event['open_date']
            logger.info(f'Checking event ID {event_id} with name {event_name}')
            if open_date < current_time:
                logger.info(f'Skipping event ID {event_id} with name {event_name} startDate: {open_date}  ----   GAME IS LIVE')
                continue
            url_pts = f"{SPORTSBOOK_API_URL}event-page?_ak=FhMFpcPWXMeyZxOx&eventId={event_id}&tab=player-points&useCombinedTouchdownsVirtualMarket=true"
            url_reb = f"{SPORTSBOOK_API_URL}event-page?_ak=FhMFpcPWXMeyZxOx&eventId={event_id}&tab=player-rebounds&useCombinedTouchdownsVirtualMarket=true"
            url_ast = f"{SPORTSBOOK_API_URL}event-page?_ak=FhMFpcPWXMeyZxOx&eventId={event_id}&tab=player-assists&useCombinedTouchdownsVirtualMarket=true"
            getdata_fromrequest(driver, extracted_data, event, url_reb, '_TOTAL_REBOUNDS')
            getdata_fromrequest(driver, extracted_data, event, url_ast, '_TOTAL_ASSISTS')
            getdata_fromrequest(driver, extracted_data, event, url_pts, '_TOTAL_POINTS')
        return extracted_data

def getdata_fromrequest(driver, extracted_data, event, url_pts, market_type):
    logger.info(f'url: {url_pts}')
    driver.get(url_pts)
    if BROWSER_TYPE == 'FIREFOX':
        wait = WebDriverWait(driver, 3)
        view_raw_button = wait.until(EC.element_to_be_clickable((By.ID, 'rawdata-tab')))
        view_raw_button.click()
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'pre')))
    pre_tag = soup.find('pre')
    if pre_tag:
        json_string = pre_tag.text
        json_data = json.loads(json_string)
        if 'attachments' in json_data:
            for market_info in json_data['attachments']['markets'].values():
                if market_info['marketType'].endswith(market_type) and '_ALT_' not in market_info['marketType']:
                    marketinfoname = market_info['marketName']
                    namepart = marketinfoname.rsplit('-', 1)[0].strip()
                    typepart = marketinfoname.rsplit('-', 1)[-1].strip()
                    extracted_data.append({
                        'EventName': event['name'],
                        'OpenDate': event['open_date'],
                        'DateChecked': formatted_date,
                        'BetName': namepart,
                        'BetType': typepart
                    })
                    for runner in market_info.get('runners', []):
                        result_type = runner['result']['type']
                        betvalue = runner['handicap']
                        betodds = runner['winRunnerOdds']['americanDisplayOdds']['americanOddsInt']
                        if result_type == 'OVER':
                            extracted_data[-1]['OverValue'] = betvalue
                            extracted_data[-1]['OverOdds'] = betodds
                        elif result_type == 'UNDER':
                            extracted_data[-1]['UnderValue'] = betvalue
                            extracted_data[-1]['UnderOdds'] = betodds
    return extracted_data

evt_data = find_upcoming_games()
all_extracted_data = fetch_and_process_event_data_with_selenium(evt_data)
if len(all_extracted_data) == 0:
    logger.info('No new data to check stopping....')
    exit(0)

logger.info(f'Parsed upcoming events and saved: {outbound_odds}')
df_events = pd.DataFrame(all_extracted_data)
df_events.to_csv(outbound_odds, index=False)
uniquedates = df_events['OpenDate'].unique()
formatted_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in uniquedates]
formatted_dates_str = ','.join([f"'{date}'" for date in formatted_dates])
engine = create_engine(f'mysql+mysqlconnector://{mys_us}:{mys_ps}@{mys_sv}')
sql_query = f"""SELECT * FROM {SPORTSBOOK_ODDS_TABLE} WHERE OpenDate IN ({formatted_dates_str})"""
database_df = pd.read_sql_query(sql_query, engine)
if len(database_df) > 0:
    merge_columns = ['EventName', 'BetName', 'BetType', 'OverValue', 'UnderValue']
    check_df = pd.merge(df_events, database_df[merge_columns], on=merge_columns, how='left', indicator=True)
    merged_df = check_df[check_df['_merge'] == 'left_only'].drop(columns=['_merge'])
else:
    merged_df = df_events.copy()

new_record_count = len(merged_df)
if new_record_count > 0:
    logger.info(f'Parsed data {new_record_count} new records')
    merged_df.to_csv(outbound_odds, index=False)
    merged_df.to_sql(SPORTSBOOK_ODDS_TABLE, con=engine, if_exists='append', index=False)
    logger.info(f'Synced sportsbook_odds {new_record_count} records...')

engine.dispose()
logger.info('Cleanup sync files...')
if os.path.exists(outbound_odds):
    os.remove(outbound_odds)

logger.info('Removing temp credentials...')
subprocess.run(['powershell.exe', '-ExecutionPolicy', 'Bypass', '-File', POWERSHELL_CRED_DELETE], check=True)

logger.info('Finished...')
logger.info('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')