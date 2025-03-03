import json
import os
import time
import random
import numpy as np
import pandas as pd
import globals.global_settings as gls
from globals import global_utils as mu
from datetime import datetime, timedelta
from nba_api.stats.endpoints import leaguegamefinder

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

print("Loaded.... gamelog_builder")

#########################################################################################################################################
####  CREATE/UPDATE GAME LOGS IN DIRECTORY -> '/DATA/GAME/{SEASON}/'
# Uses latest_gamedate saved in the config.ini located in DIRECTORY -> '/SETTINGS/'
# Create games from latest_gamedate to current date of system.
# Updates and skips dates found in valid_skip_dates.csv to avoid searching dates more than once.
# SET 'print_all_info' to 'TRUE' to see details for each game created.
# SET 'CREATE_ALL' to 'TRUE' to force create from list of game_ids.
#########################################################################################################################################
PRINT_ALL_INFO = False
CREATE_ALL = False

def create_game_data(game_id, alt_directory=None, will_save=True):
    game_id = game_id[2:] if game_id.startswith('00') else game_id
    player_stats, team_stats, game_summary, other_stats, inactive_players, line_score, officials, gameinfos = mu.get_game_details(f'00{game_id}')
    inactive_players['GAME_ID'] = game_id
    inactive_players['PLAYER_NAME'] = inactive_players['FIRST_NAME'] + ' ' + inactive_players['LAST_NAME']
    inactive_players['NICKNAME'] = inactive_players['FIRST_NAME']
    inactive_players['START_POSITION'] = ''
    inactive_players['COMMENT'] = 'OUT - Inactive Player'
    inactive_players['MIN'] = None
    inactive_players.drop(['JERSEY_NUM', 'TEAM_NAME'], axis=1, inplace=True)
    if PRINT_ALL_INFO:
        print_game_info(player_stats,team_stats,game_summary,other_stats,inactive_players,line_score,officials,gameinfos)
    for col in player_stats.columns:
        if col not in inactive_players.columns:
            inactive_players[col] = np.nan
    inactive_players = inactive_players[player_stats.columns]
    combined_df = pd.concat([player_stats, inactive_players], ignore_index=True, sort=False)
    columns_to_nan = ['FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB','AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS']
    combined_df[columns_to_nan] = combined_df[columns_to_nan].apply(lambda x: x.fillna(np.nan))
    combined_df['MIN'] = combined_df['MIN'].astype(str).str.split(':', expand=True)[0]
    season = game_summary['SEASON'].iloc[0]
    combined_df['SEASON'] = season
    line_score[['WINS', 'LOSSES']] = line_score['TEAM_WINS_LOSSES'].str.split('-', expand=True)
    combined_df = pd.merge(combined_df, line_score[['TEAM_ABBREVIATION', 'WINS', 'LOSSES']], on='TEAM_ABBREVIATION',how='left')
    official_ids = officials['OFFICIAL_ID'].astype(str).tolist()[:3]
    officials_dict = officials.set_index('OFFICIAL_ID')[['FIRST_NAME', 'LAST_NAME']].apply(lambda x: f"{x['FIRST_NAME']} {x['LAST_NAME']}", axis=1).to_dict()
    officials_dict = {str(key): value for key, value in officials_dict.items()}
    combined_df['OFFICIAL1'] = official_ids[0] if len(official_ids) > 0 else None
    combined_df['OFFICIAL1NAME'] = officials_dict.get(official_ids[0], None) if len(official_ids) > 0 else None
    combined_df['OFFICIAL2'] = official_ids[1] if len(official_ids) > 1 else None
    combined_df['OFFICIAL2NAME'] = officials_dict.get(official_ids[1], None) if len(official_ids) > 1 else None
    combined_df['OFFICIAL3'] = official_ids[2] if len(official_ids) > 2 else None
    combined_df['OFFICIAL3NAME'] = officials_dict.get(official_ids[2], None) if len(official_ids) > 2 else None
    combined_df['GAME_DATE'] = pd.to_datetime(gameinfos['GAME_DATE'].iloc[0]).strftime('%Y-%m-%d')
    combined_df['GAME_TIME'] = '2:30'
    season_col = combined_df.pop('SEASON')
    combined_df.insert(0, 'SEASON', season_col)
    date_col = combined_df.pop('GAME_DATE')
    combined_df.insert(2, 'GAME_DATE', date_col)
    time_col = combined_df.pop('GAME_TIME')
    combined_df.insert(3, 'GAME_TIME', time_col)
    wins_col = combined_df.pop('WINS')
    combined_df.insert(7, 'WINS', wins_col)
    losses_col = combined_df.pop('LOSSES')
    combined_df.insert(8, 'LOSSES', losses_col)
    official1_col = combined_df.pop('OFFICIAL1')
    combined_df.insert(12, 'OFFICIAL1', official1_col)
    # official1name_col = combined_df.pop('OFFICIAL1NAME')
    # combined_df.insert(13, 'OFFICIAL1NAME', official1name_col)
    official2_col = combined_df.pop('OFFICIAL2')
    combined_df.insert(13, 'OFFICIAL2', official2_col)
    # official2name_col = combined_df.pop('OFFICIAL2NAME')
    # combined_df.insert(15, 'OFFICIAL2NAME', official2name_col)
    official3_col = combined_df.pop('OFFICIAL3')
    combined_df.insert(14, 'OFFICIAL3', official3_col)
    # official3name_col = combined_df.pop('OFFICIAL3NAME')
    # combined_df.insert(17, 'OFFICIAL3NAME', official3name_col)
    combined_df['GAME_ID'] = combined_df['GAME_ID'].astype(str).str.lstrip('0')
    combined_df['sort_key'] = combined_df.iloc[:, -4:].isnull().any(axis=1) | (combined_df.iloc[:, -4:] == '').any(axis=1)
    combined_df = combined_df.sort_values(by=['sort_key','TEAM_ID']).drop('sort_key', axis=1)
    game_id_to_home_team_id = game_summary.set_index('GAME_ID')['HOME_TEAM_ID'].to_dict()
    combined_df['IS_HOME'] = combined_df.apply(lambda row: 1 if game_id_to_home_team_id.get('00'+row['GAME_ID']) == row['TEAM_ID'] else 0, axis=1)
    ishome_col = combined_df.pop('IS_HOME')
    combined_df.insert(9, 'IS_HOME', ishome_col)
    combined_df = combined_df.rename(columns={'TEAM_ABBREVIATION': 'TEAM_NAME'})
    teams = combined_df.groupby(['GAME_ID', 'TEAM_ID'])['PTS'].sum().reset_index()
    teams['IS_WIN'] = 0
    win_indices = teams.groupby('GAME_ID')['PTS'].idxmax()
    teams.loc[win_indices, 'IS_WIN'] = 1
    combined_df = pd.merge(combined_df, teams[['GAME_ID', 'TEAM_ID', 'IS_WIN']], on=['GAME_ID', 'TEAM_ID'], how='left')
    is_win_col = combined_df.pop('IS_WIN')
    combined_df.insert(10, 'IS_WIN', is_win_col)
    combined_df['IS_PREDICTOR'] = 0
    combined_df.drop(['OFFICIAL1NAME'], axis=1, inplace=True)
    combined_df.drop(['OFFICIAL2NAME'], axis=1, inplace=True)
    combined_df.drop(['OFFICIAL3NAME'], axis=1, inplace=True)
    print(f'Loaded GameID:{game_id}')
    if will_save:
        unique_date = combined_df['GAME_DATE'].unique()
        unique_date_str = ', '.join(unique_date.tolist())
        save_directory = f'{gls.GAMES_DATA_DIR}{season}/'
        if alt_directory is not None:
            print(f'Saving to alt directory: {alt_directory}')
            save_directory = alt_directory
        file_path = f'{save_directory}{unique_date_str}-{game_id}{gls.DEFAULT_CSV_TYPENAME}'
        os.makedirs(save_directory, exist_ok=True)
        if os.path.exists(file_path):
            print('File already exists removing old file..')
            os.remove(file_path)
        combined_df.to_csv(file_path, index=False)
        print(f'Saved: {file_path}')
    time.sleep(random.randint(1, 4))
    return combined_df

def print_game_info(player_stats, team_stats, game_summary, other_stats, inactive_players, line_score, officials, gameinfos):
    print("Player Stats:")
    print(player_stats)
    print("\nTeam Stats:")
    print(team_stats)
    print("\nGame Summary:")
    print(game_summary)
    print("\nOther Stats:")
    print(other_stats)
    print("\nInactive Players:")
    print(inactive_players)
    print("\nLine Score:")
    print(line_score)
    print("\nOfficials:")
    print(officials)
    print("\nGame Info:")
    print(gameinfos)

def create_all_games():
    all_games = pd.read_csv(gls.ALL_GAMES)
    games_df = all_games[all_games['CAPTURED'] == 0]
    print(f'Loaded Games to find... need {len(games_df)} games...')
    for index, row in games_df.iterrows():
        game_id = row['GAME_ID']
        create_game_data(game_id)

def fetch_game_ids(date):
    date_str = date.strftime('%m/%d/%Y')
    gamefinder = leaguegamefinder.LeagueGameFinder(date_from_nullable=date_str, date_to_nullable=date_str, league_id_nullable='00')
    games_df = gamefinder.get_data_frames()[0]
    print(f'games_df =  {games_df}')
    games_df = games_df[~games_df['SEASON_ID'].astype(str).str.contains('320')]
    game_ids = games_df['GAME_ID'].unique()
    return game_ids.tolist()

def create_recent_games():
    print('create_recent_games...')
    current_date = datetime.today().date()
    latest_gamedate = datetime.strptime(mu.read_from_config('latest_gamedate', 1), '%Y-%m-%d').date()
    valid_skip_games_array = pd.read_csv(gls.VALID_SKIP_DATES)['VALID_DATE'].values if os.path.exists(gls.VALID_SKIP_DATES) else []
    while latest_gamedate <= current_date:
        nd_str = latest_gamedate.strftime('%Y-%m-%d')
        if nd_str not in valid_skip_games_array:
            game_ids = fetch_game_ids(latest_gamedate)
            print(f'Checked date {latest_gamedate}.  Returned game_ids ---> {game_ids}')
            if len(game_ids) > 0:
                for game_id in game_ids:
                    create_game_data(game_id)
                print("Saved all valid games")
            else:
                if latest_gamedate != current_date:
                    print(f'Adding date to valid_skip_games_array... No games found for ---> {latest_gamedate}')
                    valid_skip_games_array = np.append(valid_skip_games_array, nd_str)
                    df = pd.DataFrame(valid_skip_games_array, columns=['VALID_DATE'])
                    df.to_csv(gls.VALID_SKIP_DATES, index=False)
                else:
                    print('Not updating valid_skip_games_array since date is today..')
        else:
            print(f"Skipping date in valid_skip_games_array...  Already contains --> {latest_gamedate}")
        nd_str = latest_gamedate.strftime('%Y-%m-%d')
        mu.write_to_config(key='latest_gamedate',val=nd_str)
        latest_gamedate = latest_gamedate + timedelta(days=1)

def recreate_location_distances():
    print('recreate_location_distances...')
    with open(gls.TEAM_LOCATIONS_JSON, 'r', encoding='utf-8') as file:
        nba_data = json.load(file)
    rows = []
    for home, h_values in nba_data.items():
        for away, a_values in nba_data.items():
            if home != away:
                distance = mu.haversine(h_values['lat'], h_values['long'], a_values['lat'], a_values['long'])
                int_distance = int(distance)
                rows.append({'HOME': home, 'AWAY': away, 'DISTANCE': int_distance})
    if os.path.exists(gls.LOCATION_DISTANCES):
        os.remove(gls.LOCATION_DISTANCES)
    distance_df = pd.DataFrame(rows, columns=['HOME', 'AWAY', 'DISTANCE'])
    distance_df.to_csv(gls.LOCATION_DISTANCES, index=False)

def create_game_logs_start():
    if CREATE_ALL:
        create_all_games()
    else:
        create_recent_games()
    recreate_location_distances()

print("Loaded.... gamelog_builder")
