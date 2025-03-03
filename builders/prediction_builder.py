import os

import pandas as pd
import globals.global_settings as gls
import globals.run_settings as rns
import webevents.get_latest_injurys as gli
import webevents.get_latest_events as gle
from unidecode import unidecode

print("Loading.... prediction_builder")

# Pandas Setup
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def remove_old_predictions():
    print('remove_old_predictions...')
    pred_year = rns.prediction_season
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if pred_year not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            df_log = pd.read_csv(file_path)
            remove_file = False
            if 'IS_PREDICTOR' in df_log.columns:
                first_row = df_log.head(1)
                is_pred = first_row['IS_PREDICTOR'][0]
                if is_pred == 1:
                    remove_file = True
            if remove_file:
                print(f'File prediction exists removing {file_path}..')
                os.remove(file_path)

def process_directory(inj_data, evt_data, spb_data, off_data):
    season_path = f'{gls.GAMES_DATA_DIR}/{rns.prediction_season}/'
    files = [f for f in os.listdir(season_path) if os.path.isfile(os.path.join(season_path, f))]
    highest_name_file = max(files) if files else None
    gparts = highest_name_file.split('-')
    latest_game_id = '_'.join(gparts[3:]).replace('.csv', '')
    print(f'Latest Game ID:{latest_game_id}')
    files_sorted_descending = sorted(files, reverse=True)
    for item1 in spb_data:
        t1,t2 = item1['home_team'][1],item1['away_team'][1]
        for item2 in evt_data:
            tt1,tt2 = item2['home_team']['name'],item2['away_team']['name']
            if t1 == tt1 and t2 is tt2:
                item2['game_time'] = item1['game_time']
                break

    for event in evt_data:
        t1, t2 = event['home_team']['name'], event['away_team']['name']
        homedf, awaydf = None, None
        for file in files_sorted_descending:
            file_path = os.path.join(season_path, file)
            curr_df = pd.read_csv(file_path)
            if curr_df['TEAM_NAME'].astype(str).str.contains(t1).any() and homedf is None:
                homedf = curr_df[curr_df['TEAM_NAME'].astype(str).str.contains(t1, na=False)]
                print(f"Found home {file_path}")
            if curr_df['TEAM_NAME'].astype(str).str.contains(t2).any() and awaydf is None:
                awaydf = curr_df[curr_df['TEAM_NAME'].astype(str).str.contains(t2, na=False)]
                print(f"Found away {file_path}")
            if homedf is not None and awaydf is not None:
                print(f"Previous game mapped for {t1} vs {t2}")
                condition1 = homedf['COMMENT'].isin(["DNP - Coach's Decision","OUT - Inactive Player"])
                condition2 = awaydf['COMMENT'].isin(["DNP - Coach's Decision","OUT - Inactive Player"])
                players_to_update_df1 = homedf.loc[condition1, ['PLAYER_ID', 'COMMENT']]
                players_to_update_df2 = awaydf.loc[condition2, ['PLAYER_ID', 'COMMENT']]
                previous_invalids = pd.concat([players_to_update_df1, players_to_update_df2]).drop_duplicates(subset='PLAYER_ID')
                print(f"previous_invalids:\n{previous_invalids}")
                homedf,awaydf = homedf.copy(),awaydf.copy()
                break

        homedf['PLAYER_NAME'] = homedf['PLAYER_NAME'].apply(unidecode)
        awaydf['PLAYER_NAME'] = awaydf['PLAYER_NAME'].apply(unidecode)
        e_gid = event['game_id']
        e_gametime = event['game_time']
        distance_df = pd.read_csv(gls.LOCATION_DISTANCES)
        specific_distance = distance_df[(distance_df['HOME'] == t1) & (distance_df['AWAY'] == t2)]
        homedf.loc[:,'IS_HOME'] = 1
        homedf.loc[:,'DISTANCE'] = 0
        homedf.loc[:,'OPP_DISTANCE'] = specific_distance['DISTANCE'].values[0]
        awaydf.loc[:,'IS_HOME'] = 0
        awaydf.loc[:,'DISTANCE'] = specific_distance['DISTANCE'].values[0]
        awaydf.loc[:,'OPP_DISTANCE'] = 0
        TEAM_NAME = homedf.iloc[0]['TEAM_NAME']
        matching_row = off_data[off_data['GAME'].str.contains(TEAM_NAME, na=False)]
        if e_gid == '22400492':
            print('check....')
        if matching_row.empty:
            raise ValueError(f"GAME NOT FOUND ERROR FOR TEAM: {TEAM_NAME}")
        else:
            ref1_id = matching_row.iloc[0]['REF1ID']
            ref2_id = matching_row.iloc[0]['REF2ID']
            homedf['OFFICIAL1'] = ref1_id
            homedf['OFFICIAL2'] = ref2_id
            awaydf['OFFICIAL1'] = ref1_id
            awaydf['OFFICIAL2'] = ref2_id
        # awaydf.loc[:, 'OFFICIAL1'] = homedf.iloc[0]['OFFICIAL1']
        # awaydf.loc[:, 'OFFICIAL2'] = homedf.iloc[0]['OFFICIAL2']
        combined_df = pd.concat([homedf, awaydf], ignore_index=True)
        combined_df.loc[:, 'GAME_ID'] = e_gid
        combined_df.loc[:, 'GAME_DATE'] = rns.prediction_date
        combined_df.loc[:, 'IS_PREDICTOR'] = 1
        if len(e_gametime)> 0:
            combined_df.loc[:, 'GAME_TIME'] = e_gametime
        for item in ['MIN','FGM','FGA','FG_PCT','FG3M','FG3A','FG3_PCT','FTM','FTA','FT_PCT','OREB','DREB','REB','AST','STL','BLK','TO','PF','PTS','PLUS_MINUS']:
            combined_df[item] = 0

        for index, row in combined_df.iterrows():
            player_id = row['PLAYER_ID']
            player_name = row['PLAYER_NAME']
            is_invalid = False
            for inj in inj_data:
                if player_name in inj['Player Name'] and 'Out' in inj['Status']:
                    print(f'Removing injured Player: {player_name}')
                    is_invalid = True
                    break
            for ptu_index, ptu_row in previous_invalids.iterrows():
                if player_id == ptu_row['PLAYER_ID'] and is_invalid == False:
                    print(f"Removing inactive Player: {player_name}")
                    is_invalid = True
                    break
            if not is_invalid:
                combined_df.at[index, 'COMMENT'] = ''
                combined_df.at[index, 'START_POSITION'] = 'B'
            if is_invalid:
                combined_df.at[index, 'COMMENT'] = 'OUT'
                combined_df.at[index, 'START_POSITION'] = ''

        home_players, away_players = event['home_team']['players'], event['away_team']['players']
        for player in home_players:
            player['name'] = unidecode(player['name'])
        for player in away_players:
            player['name'] = unidecode(player['name'])

        for player in home_players:
            combined_df = set_postion_status(combined_df, player)
        for player in away_players:
            combined_df = set_postion_status(combined_df, player)
        count_C_positions = (combined_df['START_POSITION'] == 'C').sum()
        count_G_positions = (combined_df['START_POSITION'] == 'G').sum()
        count_F_positions = (combined_df['START_POSITION'] == 'F').sum()
        count_B_positions = (combined_df['START_POSITION'] == 'B').sum()
        predictor_f_path = f'{gls.GAMES_DATA_DIR}{rns.prediction_season}/{rns.prediction_date}-{e_gid}{gls.DEFAULT_CSV_TYPENAME}'
        if count_C_positions != 2:
            raise ValueError(f"Count of 'C' in START_POSITION is invalid: {count_C_positions} for path {predictor_f_path}")
        if count_G_positions != 4:
            raise ValueError(f"Count of 'G' in START_POSITION is invalid: {count_G_positions} for path {predictor_f_path}")
        if count_F_positions != 4:
            raise ValueError(f"Count of 'F' in START_POSITION is invalid: {count_F_positions} for path {predictor_f_path}")
        if count_B_positions < 2:
            raise ValueError(f"Count of 'B' in START_POSITION is invalid: {count_B_positions} for path {predictor_f_path}")

        print(f"Generated prediciton for GameID:{e_gid} saved to Path:{predictor_f_path}")
        combined_df.to_csv(predictor_f_path, index=False)
    print("Finished generating prediction games....")


def set_postion_status(df, player):
    p_name, p_position, p_status = player['name'], player['position'], player['status']
    if p_name in df['PLAYER_NAME'].values:
        new_position = '' if 'Inactive' in p_status else 'F' if p_position in ['SF', 'PF'] else 'G' if p_position in ['SG', 'PG'] else 'C' if p_position == 'C' else 'B'
        new_status = 'OUT' if 'Inactive' in p_status else ''
        df.loc[df['PLAYER_NAME'] == p_name, 'START_POSITION'] = new_position
        df.loc[df['PLAYER_NAME'] == p_name, 'COMMENT'] = new_status
    else:
        print(f'Unknown Player:{p_name} skipping...')
    return df

def generate_predicitons_start():
    inj_data = gli.find_injury_news()
    print(f'Injury News:{inj_data}')
    evt_data = gle.find_todays_nba_lineups()
    off_data = gle.scrape_game_officials()
    spb_data = gle.find_sportsbook_games()
    remove_old_predictions()
    process_directory(inj_data, evt_data, spb_data, off_data)

print("Loaded.... prediction_builder")