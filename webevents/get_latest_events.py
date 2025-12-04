import json
import datetime

import pandas as pd
import pytz
import time
import globals.global_settings as gls
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService

print("Loading.... get_latest_events")

formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_time = datetime.datetime.now()
utc_timezone = pytz.timezone('UTC')
est_timezone = pytz.timezone('US/Eastern')

def scrape_game_officials():
    service = FirefoxService(gls.GECKO_EXE_LOC)
    options = webdriver.FirefoxOptions()
    options.binary_location = gls.BROWSER_EXE_LOC
    options.add_argument("--log-level=3")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--headless')
    options.add_argument("--window-size=0,0")
    options.add_argument(f'--user-data-dir={gls.BROWSER_PROFILE_DIR}')
    browser_driver = webdriver.Firefox(service=service, options=options)
    url = "https://www.rotowire.com/basketball/nba-lineups.php"
    browser_driver.get(url)
    wait = WebDriverWait(browser_driver, 10)
    games_data = []
    try:
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "lineups")))
        games = browser_driver.find_elements(By.CSS_SELECTOR, 'div[class*="lineup is-nba"]')
        for game in games:
            try:
                teams = game.find_elements(By.CLASS_NAME, "lineup__abbr")
                if len(teams) >= 2:
                    team_1 = teams[0].text
                    team_2 = teams[1].text
                    game_matchup = f"{team_1}/{team_2}"
                else:
                    continue

                umpire_div = game.find_element(By.CLASS_NAME, "lineup__umpire")
                referees = [
                    ref.text for ref in umpire_div.find_elements(By.TAG_NAME, "a")
                ]
                referees_list = ",".join(referees)

                games_data.append({
                    "GAME": game_matchup,
                    "REFEREES": referees_list
                })
            except Exception as e:
                print(f"Error processing game: {e}")

    except Exception as e:
        print(f"Error scraping lineups: {e}")
    finally:
        browser_driver.quit()
    game_officials = pd.DataFrame(games_data)
    game_officials[['REF1', 'REF2', 'REF3']] = game_officials['REFEREES'].str.split(',', expand=True)
    unique_officials = pd.read_csv(gls.UNIQUE_OFFICIALS)
    print("Loaded unique_officials...")
    for ref_col in ['REF1', 'REF2', 'REF3']:
        game_officials = game_officials.merge(
            unique_officials[['OFFICIAL_ID', 'OFFICIAL_NAME']],
            how='left',
            left_on=ref_col,
            right_on='OFFICIAL_NAME'
        )
        game_officials = game_officials.rename(columns={'OFFICIAL_ID': f"{ref_col}ID"})
        game_officials = game_officials.drop(columns=['OFFICIAL_NAME'])
    for ref_id_col in ['REF1ID', 'REF2ID', 'REF3ID']:
        game_officials[ref_id_col] = pd.to_numeric(game_officials[ref_id_col], errors='coerce').astype('Int64')
    game_officials = game_officials.drop(columns=['REFEREES'])
    game_officials.to_csv(gls.OFFICIALS_TODAY, index=False)
    print(f"Data saved to {gls.OFFICIALS_TODAY}")
    return game_officials

def find_todays_nba_lineups():
    service = FirefoxService(gls.GECKO_EXE_LOC)
    options = webdriver.FirefoxOptions()
    options.binary_location = gls.BROWSER_EXE_LOC
    options.add_argument("--log-level=3")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'--user-data-dir={gls.BROWSER_PROFILE_DIR}')
    options.add_argument('--headless')
    options.add_argument("--window-size=0,0")
    browser_driver = webdriver.Firefox(service=service, options=options)
    lineups_data = []
    with browser_driver as driver:
        lookup_url = 'https://www.nba.com/players/todays-lineups'
        print(f'url: {lookup_url}')
        driver.get(lookup_url)
        wait = WebDriverWait(driver, 3)
        try:
            wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'div[class*="LineupsView_games"]')))
            games = driver.find_elements(By.CSS_SELECTOR, 'div[class*="LineupsView_games"] > div')
            for game in games:
                matchup = game.find_element(By.CSS_SELECTOR, 'h1[class*="Block_blockTitleText"]').text
                print(f'Found game: {matchup}')
                boxscore_link = game.find_element(By.CSS_SELECTOR, 'a[class*="Block_blockTitleLink"]').get_attribute('href')
                game_id = boxscore_link.split('-00')[-1].rstrip('/')
                teamA_players = extract_players(game)
                teamB_button = game.find_elements(By.CSS_SELECTOR, 'div[class*="ButtonGroup_bg"] button')[1]
                driver.execute_script("arguments[0].click();", teamB_button)
                time.sleep(1)
                teamB_players = extract_players(game)
                teams = matchup.split(' VS ')
                away_data = {'name': teams[0], 'players': teamA_players}
                home_data = {'name': teams[1], 'players': teamB_players}
                game_data = {'matchup': matchup, 'game_id':game_id, 'game_time':'', 'home_team': home_data, 'away_team': away_data}
                lineups_data.append(game_data)
        except Exception as e:
            print(f"An error occurred while scraping NBA lineups: {e}")
        print('Got lineups_data data...')
    return lineups_data

def extract_players(game_element):
    players_dict = {}
    player_elements = game_element.find_elements(By.CSS_SELECTOR, 'li[class*="DailyLineup_dlPlayer"]')
    for player in player_elements:
        status_span = player.find_element(By.CSS_SELECTOR, 'span[class*="DailyLineup_dlStatus"]')
        status = status_span.get_attribute("title")
        name = player.find_element(By.CSS_SELECTOR, 'span[class*="DailyLineup_dlName"]').text
        position = player.find_element(By.CSS_SELECTOR, 'span[class*="DailyLineup_dlPos"]').text
        if name not in players_dict:
            players_dict[name] = {'name': name, 'position': position, 'status': status}
        else:
            if not players_dict[name]['position'] and position:
                players_dict[name] = {'name': name, 'position': position, 'status': status}
    player_list = list(players_dict.values())
    return player_list

def find_sportsbook_games():
    service = FirefoxService(gls.GECKO_EXE_LOC)
    options = webdriver.FirefoxOptions()
    options.binary_location = gls.BROWSER_EXE_LOC
    options.add_argument("--log-level=3")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'--user-data-dir={gls.BROWSER_PROFILE_DIR}')
    options.add_argument('--headless')
    options.add_argument("--window-size=0,0")
    browser_driver = webdriver.Firefox(service=service, options=options)
    extracted_data = []
    with browser_driver as driver:
        lookup_url = f'{gls.SPORTSBOOK_API_URL}content-managed-page?currencyCode=USD&exchangeLocale=en_US&includePrices=true&language=en&regionCode=NAMERICA&timezone=America%2FNew_York&_ak=FhMFpcPWXMeyZxOx&page=CUSTOM&customPageId=nba'
        print(f'url: {lookup_url}')
        driver.get(lookup_url)
        if gls.BROWSER_TYPE == 'FIREFOX':
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
                    print(f'Found event: {event_name}')
                    if ' @ ' in event_info['name'] and not event_info['name'].startswith("West @ East"):
                        split_result = event_info['name'].split(' @ ')
                        last_words = [split.strip().split()[-1] for split in split_result]
                        away_team = last_words[0]
                        home_team = last_words[1]
                        away_short = gls.TEAM_TOSHORT_MAPPER.get(away_team, None)
                        home_short = gls.TEAM_TOSHORT_MAPPER.get(home_team, None)
                        opn_date = event_info['openDate']
                        formatdate = datetime.datetime.strptime(opn_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                        given_time_utc = utc_timezone.localize(formatdate)
                        given_time_est = given_time_utc.astimezone(est_timezone).replace(tzinfo=None)
                        time_str = given_time_est.strftime('%H:%M')
                        extracted_data.append({
                            'eventId': event_info['eventId'],
                            'name': event_info['name'],
                            'eventTypeId': event_info['eventTypeId'],
                            'competitionId': event_info['competitionId'],
                            'home_team': [home_team, home_short],
                            'away_team': [away_team, away_short],
                            'game_id': event_info['eventId'],
                            'open_date': given_time_est,
                            'game_time': time_str
                        })

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from page source for event ID {event_info['eventId']}: {e}")
        except Exception as e:
            print(f"An error occurred for event ID {event_info['eventId']}: {e}")
    if len(extracted_data) > 0:
        print('Got sportsbook data...')
    else:
        print('No new upcoming events or games found...')
    return extracted_data

# find_sportsbook_games()
# find_todays_nba_lineups()

print("Loaded.... get_latest_events")