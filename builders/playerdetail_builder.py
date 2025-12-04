import os
import time
from datetime import datetime

import pandas as pd
from unidecode import unidecode

import globals.global_settings as gls
import globals.run_settings as rns
from globals import global_utils as mu
from nba_api.stats.endpoints import playercareerstats, commonplayerinfo, commonallplayers

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

print("Loading.... playerdetail_builder")

#########################################################################################################################################
####  CREATE/UPDATE PLAYER DETAILS IN DIRECTORY -> '/DATA/PLAYER/DETAILS/'
# Uses players from game_logs located in DIRECTORY -> '/DATA/GAME/'
# SET 'rns.update_active_players_team' to 'TRUE' to update players that are not retired.
#########################################################################################################################################

def calculate_years_in_team(group):
    group = group.sort_values('SEASON')
    years = [1]
    for i in range(1, len(group)):
        if group.iloc[i]['TEAM'] == group.iloc[i - 1]['TEAM']:
            years.append(years[-1] + 1)
        else:
            years.append(1)
    group['YEARS_IN_TEAM'] = years
    return group

def fill_missing_data(pos, reference):
    curr_pos = pos.iloc[0]
    if curr_pos in gls.POSITION_TOINT_MAPPER:
        if reference == 'WEIGHT':
            return gls.POSITION_TOSIZE_MAPPER[curr_pos]['avg_weight']
        if reference == 'HEIGHT':
            return gls.POSITION_TOSIZE_MAPPER[curr_pos]['avg_height']
    raise ValueError(f"Missing or invalid position {pos}")

def fix_schools():
    print('fix_schools')
    for filename in os.listdir(gls.PLAYER_DETAIL_DIR):
        if filename.endswith(gls.DEFAULT_CSV_TYPENAME):
            file_path = os.path.join(gls.PLAYER_DETAIL_DIR, filename)
            df_fs = pd.read_csv(file_path)
            if 'SCHOOL' in df_fs.columns:
                if df_fs['SCHOOL'].astype(str).str.match(r'^\s*$').any():
                    print(f"Fixing school for : {file_path}")
                    df_fs['SCHOOL'] = df_fs['SCHOOL'].replace(r'^\s*$', '', regex=True)
                    df_fs.to_csv(file_path, index=False)

def fix_filenames():
    print('fix_filenames')
    for filename in os.listdir(gls.PLAYER_DETAIL_DIR):
        if filename.endswith(gls.DEFAULT_CSV_TYPENAME):
            file_path = os.path.join(gls.PLAYER_DETAIL_DIR, filename)
            mu.fix_invalid_teams(file_path)
            parts = filename.split('_')
            player_id = parts[2]
            player_name = ' '.join(parts[3:]).replace('.csv', '').replace(' Jr', 'Jr').replace(' Sr', 'Sr').replace(".","")
            player_name = mu.remove_suffixs(player_name)
            player_name = player_name.replace(" ", "_")
            new_filename = f"{gls.DEFAULT_PLAYER_DETAIL_NAME}{player_id}_{player_name}{gls.DEFAULT_CSV_TYPENAME}"
            if filename != new_filename:
                print(f"finalRename '{filename}' to '{new_filename}'")
                if os.path.exists(os.path.join(gls.PLAYER_DETAIL_DIR, new_filename)):
                    print(f"Removing already existing file '{new_filename}'")
                    os.remove(os.path.join(gls.PLAYER_DETAIL_DIR, new_filename));
                os.rename(os.path.join(gls.PLAYER_DETAIL_DIR, filename),os.path.join(gls.PLAYER_DETAIL_DIR, new_filename))

def extract_players(game_log_dir):
    print('extract_players...')
    entries = os.listdir(game_log_dir)
    file_count = sum(os.path.isfile(os.path.join(game_log_dir, entry)) for entry in entries)
    print(f"Check files in directory {game_log_dir} -> file_count: {file_count}")
    player_df = pd.DataFrame(columns=['PLAYER_ID', 'PLAYER_NAME'])
    player_df['PLAYER_NAME'] = player_df['PLAYER_NAME'].apply(unidecode)
    for file_name in os.listdir(game_log_dir):
        directory = f'{gls.GAMES_DATA_DIR}{file_name}'
        if not os.path.isdir(directory):
            file_path = os.path.join(game_log_dir, file_name)
            df_log = pd.read_csv(file_path)
            df_log['COMMENT'] = df_log['COMMENT'].astype(str)
            # print(f"Check path players in path -> {file_path}")
            df_filtered = df_log[~df_log['COMMENT'].str.contains('DNP|OUT', na=False, regex=True)]
            unique_players = df_filtered[['PLAYER_ID', 'PLAYER_NAME']].drop_duplicates()
            unique_players['PLAYER_NAME'] = unique_players['PLAYER_NAME'].apply(unidecode)
            player_df = pd.merge(player_df, unique_players, on=['PLAYER_ID', 'PLAYER_NAME'], how='outer')
    print(f"Total Players Loaded:{len(player_df)}")
    print(f"Players :{player_df}")
    return player_df

def fetch_and_save_player_detail(player_item, current_all_players, force_update=False):
    pl_id = player_item[0]
    pl_name = player_item[1].replace(' ', '_').replace('.', '')
    detail_filepath = os.path.join(gls.PLAYER_DETAIL_DIR, f"{gls.DEFAULT_PLAYER_DETAIL_NAME}{pl_id}_{pl_name}{gls.DEFAULT_CSV_TYPENAME}")
    det_df = None
    is_active = False
    if os.path.exists(detail_filepath):
        det_df = pd.read_csv(detail_filepath)
        is_active = (det_df['RETIRED'] == 0).all()
    if det_df is None or (rns.update_active_players_team and is_active) or force_update:
        time.sleep(2)
        print(f"Fetching detail for Player ID:{pl_id}  |  PlayerName: {pl_name}")
        player_details = commonplayerinfo.CommonPlayerInfo(player_id=pl_id, league_id_nullable='00').get_data_frames()[0]
        career_stats = playercareerstats.PlayerCareerStats(player_id=pl_id, league_id_nullable='00').get_data_frames()[0]
        combined_data = pd.merge(career_stats, player_details, how='left', left_on='PLAYER_ID', right_on='PERSON_ID', validate='many_to_many')
        combined_data = combined_data[combined_data['TEAM_ID_x'] != 0]
        combined_data = combined_data[combined_data['SEASON_ID'] >= '2005-06']
        relevant_columns = ['SEASON_ID', 'TEAM_ID_x', 'TEAM_ABBREVIATION_x','PLAYER_AGE','PLAYER_ID','DISPLAY_FIRST_LAST','BIRTHDATE','SCHOOL','COUNTRY','HEIGHT','WEIGHT','POSITION']
        player_detail_data = combined_data[relevant_columns]
        player_detail_data = player_detail_data.rename(columns={'SEASON_ID': 'SEASON','TEAM_ID_x': 'TEAM_ID','TEAM_ABBREVIATION_x' : 'TEAM','PLAYER_AGE' : 'AGE','DISPLAY_FIRST_LAST': 'PLAYER_NAME'})
        player_detail_data['PLAYER_NAME'] = player_detail_data['PLAYER_NAME'].apply(unidecode)
        if len(player_detail_data) < 1:
            print(f"Player ({pl_name}) has no gamelogs generating basic player detail..")
            birthdate_str = player_details.iloc[0]['BIRTHDATE']
            birth_date = datetime.strptime(birthdate_str, "%Y-%m-%dT%H:%M:%S")
            age = datetime.now() - birth_date
            age_years = age.days // 365
            player_details['PLAYER_AGE'] = age_years
            relevant_columns = ['FROM_YEAR', 'TEAM_ID', 'TEAM_ABBREVIATION', 'PLAYER_AGE', 'PERSON_ID', 'DISPLAY_FIRST_LAST', 'BIRTHDATE', 'SCHOOL', 'COUNTRY', 'HEIGHT', 'WEIGHT', 'POSITION']
            player_detail_data = player_details[relevant_columns]
            player_detail_data = player_detail_data.rename(columns={'FROM_YEAR': 'SEASON', 'TEAM_ABBREVIATION': 'TEAM', 'PLAYER_AGE': 'AGE', 'PERSON_ID':'PLAYER_ID', 'DISPLAY_FIRST_LAST': 'PLAYER_NAME'})
            player_detail_data['PLAYER_NAME'] = player_detail_data['PLAYER_NAME'].apply(unidecode)
        print(f"player_detail_data: {player_detail_data}")
        if player_detail_data['POSITION'].isna().any() or (player_detail_data['POSITION'] == '').any():
            print(f"Position was blank for player: {pl_id}")
            player_detail_data['POSITION'] = "Forward"
        if player_detail_data['WEIGHT'].isna().any() or (player_detail_data['WEIGHT'] == '').any():
            player_detail_data['WEIGHT'] = fill_missing_data(player_detail_data['POSITION'], 'WEIGHT')
        if player_detail_data['HEIGHT'].isna().any() or (player_detail_data['HEIGHT'] == '').any():
            player_detail_data['HEIGHT'] = fill_missing_data(player_detail_data['POSITION'], 'HEIGHT')
        player_detail_data['AGE'] = player_detail_data['AGE'].astype(int)
        player_detail_data['WEIGHT'] = player_detail_data['WEIGHT'].astype(int)
        player_detail_data['BIRTHDATE'] = player_detail_data['BIRTHDATE'].str.split('T').str[0]
        player_detail_data['HEIGHT'] = int(mu.height_to_inches(player_detail_data['HEIGHT'].iloc[0]))
        player_detail_data['SEASON'] = player_detail_data['SEASON'].astype(str).str.split('-').str[0]
        player_detail_data = player_detail_data.groupby('PLAYER_ID').apply(calculate_years_in_team).reset_index(drop=True)
        player_detail_data['RETIRED'] = 0
        if len(player_detail_data) >= 1:
            last_row_index = player_detail_data.index[-1]
            temp_val_season = player_detail_data.loc[last_row_index, 'SEASON']
            if temp_val_season is not None and temp_val_season != 'None':
                season_last_row = int(temp_val_season)
                mark_retired = False if season_last_row >= int(rns.prediction_season) else True
                print(f"Player: ({pl_name}) LastSeason: ({season_last_row}) Retired: ({mark_retired})")
                player_detail_data.loc[last_row_index, 'RETIRED'] = 0 if season_last_row == int(rns.prediction_season) else 0
                player_detail_data.to_csv(detail_filepath, index=False)
                print(f"Saved PlayerID: {pl_id} to {detail_filepath}")
    if os.path.exists(detail_filepath):
        det_df = pd.read_csv(detail_filepath)
        last_row = det_df.iloc[-1]
        last_teamid = last_row['TEAM_ID']
        c_players = current_all_players[current_all_players['PERSON_ID'] == pl_id]
        current_player = c_players.iloc[0] if len(c_players) > 0 else None
        curr_teamid = current_player['TEAM_ID'] if current_player is not None else 0
        if curr_teamid != 0 and last_teamid != curr_teamid:
            new_team = current_player['TEAM_ABBREVIATION']
            new_row = last_row.copy()
            new_row['TEAM_ID'] = curr_teamid
            new_row['TEAM'] = new_team
            new_row['YEARS_IN_TEAM'] = 1
            new_row['SEASON'] = rns.prediction_season
            print(f"Update - Player: {pl_id}  |  PlayerName: {pl_name}  From Team --> {last_row['TEAM']}  To Team --> {new_team}")
            new_row_df = pd.DataFrame([new_row])
            det_df = pd.concat([det_df, new_row_df], ignore_index=True)
            det_df.to_csv(detail_filepath, index=False)


def removing_invalid_player(game_log_dir, player_id):
    print('removing_invalid_player...')
    print(f"Check directory {game_log_dir} -> for PlayerID: ({player_id})")
    for file_name in os.listdir(game_log_dir):
        directory = f'{gls.GAMES_DATA_DIR}{file_name}'
        if not os.path.isdir(directory):
            file_path = os.path.join(game_log_dir, file_name)
            df_log = pd.read_csv(file_path)
            player_exists = (df_log['PLAYER_ID'] == int(player_id)).any()
            if player_exists:
                df_log = df_log[df_log['PLAYER_ID'] != player_id]
                print(f"Removed PlayerID: ({player_id}) from {file_path}")
                df_log.to_csv(file_path, index=False)

def create_player_details():
    print('create_player_details...')
    current_all_players = commonallplayers.CommonAllPlayers(is_only_current_season='1', league_id='00').get_data_frames()[0]
    current_season_dir = f'{gls.GAMES_DATA_DIR}{rns.prediction_season}/'
    player_df = extract_players(current_season_dir)
    player_df = mu.playername_log_to_detail(player_df)
    print(f'Creating player details...  player_df size:{len(player_df)} update_active_players_team = {rns.update_active_players_team}')
    for index, row in player_df.iterrows():
        player_id = row['PLAYER_ID']
        player_name = row['PLAYER_NAME']
        fetch_and_save_player_detail((player_id, player_name), current_all_players)
    fix_filenames()
    fix_schools()

def is_ascii(filename):
    try:
        filename.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False

def delete_invalid_player_details():
    print("delete_invalid_player_details")
    for filename in os.listdir(gls.PLAYER_DETAIL_DIR):
        file_path = os.path.join(gls.PLAYER_DETAIL_DIR, filename)
        if os.path.isfile(file_path) and not is_ascii(filename):
            print(f"Deleting file: {filename}")
            os.remove(file_path)

print("Loaded.... playerdetail_builder")