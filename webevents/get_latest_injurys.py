from bs4 import BeautifulSoup
import pandas as pd
import time
import globals.global_settings as gls
from globals import global_utils as mu
from webevents import browser

print("Loading.... get_latest_injurys")

def find_injury_news():
    browser_driver = browser.build_driver()
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
                    raise ValueError(f'Unknown Shortname for Team {team_name}')
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