import os
import json
import shutil
import datetime
import pandas as pd
import time
import globals.global_settings as gls
import globals.run_settings as rns
import get_latest_events as gle, get_latest_injurys as gli
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

print('Starting...')
current_time = datetime.datetime.now()
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

def fetch_and_process_event_data_with_selenium(ex_data):
    global browser_driver
    extracted_data = []
    if gls.BROWSER_TYPE == 'FIREFOX':
        browser_driver = webdriver.Firefox(service=service, options=options)
    elif gls.BROWSER_TYPE == 'CHROME':
        browser_driver = webdriver.Chrome(service=service, options=options)
    with browser_driver as driver:
        for event in ex_data:
            event_id = event['eventId']
            event_name = event['name']
            open_date = event['open_date']
            if open_date < current_time:
                print(f'Skipping event ID {event_id} with name {event_name} startDate: {open_date}  ----   GAME IS LIVE')
                continue
            url = f"{gls.SPORTSBOOK_API_URL}event-page?_ak=FhMFpcPWXMeyZxOx&eventId={event_id}&tab=player-points&useCombinedTouchdownsVirtualMarket=true"
            print(f'Checking event ID {event_id} with name {event_name} url: {url}')
            driver.get(url)
            if gls.BROWSER_TYPE == 'FIREFOX':
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
                        if market_info['marketType'].endswith('_TOTAL_POINTS') and '_ALT_' not in market_info[
                            'marketType']:
                            marketinfoname = market_info['marketName']
                            namepart = marketinfoname.rsplit('-', 1)[0].strip()
                            typepart = marketinfoname.rsplit('-', 1)[-1].strip()
                            extracted_data.append({
                                'EventName': event['name'],
                                'OpenDate': open_date,
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

inj_data = gli.find_injury_news()
evt_data = gle.find_sportsbook_games()
extracted_data = fetch_and_process_event_data_with_selenium(evt_data)

df_market = pd.DataFrame(extracted_data)
df_market.to_csv(gls.CURRENT_ODDS_FULLPATH, index=False)
print(f"CSV file '{gls.CURRENT_ODDS_FULLPATH}' has been created for All Games")

csv_df = df_market.copy()
# csv_df['runnerNameClean'] = csv_df['runnerName'].str.replace(' Over', '').str.replace(' Under', '').str.strip()

txt_df = pd.read_csv(gls.CURRENT_PREDICTION_FULLPATH, delimiter=',', engine='python')
txt_df.columns = txt_df.columns.str.strip()
txt_df['P'] = txt_df['P'].str.strip()

merged_df = pd.merge(csv_df, txt_df, left_on='BetName', right_on='P', how='left')
merged_df['SP_VAL'] = pd.to_numeric(merged_df['OverValue'], errors='coerce')
merged_df['PTS_PRED'] = pd.to_numeric(merged_df['PTS_PRED'], errors='coerce')
# Calculate 'Results'
merged_df['SP_CHK'] = merged_df.apply(
    lambda row: 'X' if abs(row['SP_VAL'] - row['PTS_PRED']) / row['PTS_PRED'] > 0.15 else '', axis=1)
# Calculate 'OverUnder'
merged_df['OverUnder'] = merged_df.apply(
    lambda row: 'Over' if (row['SP_CHK'] == 'X' and row['SP_VAL'] < row['PTS_PRED']) else (
        'Under' if row['SP_CHK'] == 'X' and row['SP_VAL'] > row['PTS_PRED'] else ''), axis=1)
# Calculate 'HOTBET'
merged_df['HOTBET'] = merged_df.apply(
    lambda row: 'X' if row['RT3G_PCT'] > 0 and row['RT5G_PCT'] > 0 and row['RT9G_PCT'] > 0 else '', axis=1)

output_columns = ['EventName', 'BetName', 'BetType', 'SP_VAL', 'OverOdds', 'UnderOdds', 'PTS_PRED', 'RT_PCT', 'RT3G_PCT',
                  'RT5G_PCT', 'RT9G_PCT', 'DIFF_AVG', 'SP_CHK', 'OverUnder', 'HOTBET']
final_output_df = merged_df[output_columns].copy()

final_output_df = final_output_df.rename(columns={'BetName': 'PLAYER'})
final_output_df = final_output_df.rename(columns={'eventName': 'EVENT'})
final_output_df.to_csv(gls.CURRENT_ODDS_FULLPATH, index=False)
print(f"New odds saved to {gls.CURRENT_ODDS_FULLPATH}")

# Save in previous directory
backup_odds = os.path.join(gls.PREVIOUS_ODDS_DATA_DIR, f"{rns.prediction_date}{gls.DEFAULT_CSV_TYPENAME}")
file_index = 2
while os.path.exists(backup_odds):
    backup_odds = os.path.join(gls.PREVIOUS_ODDS_DATA_DIR,f"{rns.prediction_date}-{file_index}{gls.DEFAULT_CSV_TYPENAME}")
    file_index += 1
final_output_df.to_csv(backup_odds, index=False)
print(f"Backup saved: {backup_odds}")

if rns.will_sync_server and rns.use_database:
    final_output_df.to_csv(gls.SYNC_ODDS_PREDICT_FILE, index=False)
    print(f"sync odds predict saved: {gls.SYNC_ODDS_PREDICT_FILE}")
    shutil.copyfile(gls.CURRENT_PREDICTION_FULLPATH, gls.SYNC_PREDICTION_FILE)
    print(f"sync odds predict saved: {gls.SYNC_PREDICTION_FILE}")


print('Finished...')