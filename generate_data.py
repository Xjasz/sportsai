import json
import os
import time

import numpy as np
import pandas as pd
import globals.global_settings as gls
import globals.run_settings as rns
from globals import global_utils as glu
from builders import ai_builder as aib, gamelog_builder as cgl, prediction_builder as gnd, playerdetail_builder as pld
from unidecode import unidecode

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

#########################################################################################################################################
####  CREATE/UPDATE GAME LOGS INTO SEASONS IN DIRECTORY -> '/DATA/SEASON/'
# Generates remaining gamelog data for seasons.
# SET 'run_from_start_to_finish' to 'TRUE' to overwrite season gamelogs.
# SET 'current_season_only' to 'TRUE' to overwrite current season gamelogs.
#########################################################################################################################################
OUT_KEYWORDS_SET = {'INJ', 'NOT WITH TEAM', 'MWT', 'DNT', 'DID NOT TRAVEL', 'NWT', 'SUSPENSION', 'PERSONAL', 'INACTIVE', 'DND', 'DID NOT DRESS'}

def optimized_rolling_averages(df, col, count):
    return df.groupby(['SEASON', 'PLAYER_ID'])[col].transform(lambda x: x.shift().rolling(window=count, min_periods=1).mean())

def optimized_rolling_avg_season(df, col):
    return df.groupby(['SEASON', 'PLAYER_ID'])[col].transform(lambda x: x.shift().expanding(min_periods=1).mean())

def rolling_by_official_avg_season(df, col, official_col):
    return df.groupby(['SEASON', 'PLAYER_ID', official_col])[col].transform(lambda x: x.shift().expanding(min_periods=1).mean())

def rolling_by_official(df, col, official_col, count):
    filtered_df = df[df[official_col] == col]
    rolling_means = filtered_df.groupby(['SEASON', 'PLAYER_ID'])[col].apply(lambda x: x.shift().rolling(window=count, min_periods=1).mean())
    rolling_means = rolling_means.reset_index(level=['SEASON', 'PLAYER_ID'], drop=True)
    return rolling_means

def rolling_home_or_away_consecutive(df, col, is_home):
    filtered_df = df[df['IS_HOME'] == is_home]
    rolling_means = filtered_df.groupby(['SEASON', 'PLAYER_ID'])[col].apply(lambda x: x.shift().rolling(window=3, min_periods=1).mean())
    rolling_means = rolling_means.reset_index(level=['SEASON', 'PLAYER_ID'], drop=True)
    return rolling_means

def process_player_group(df):
    df['change'] = df['IS_OUT'].diff().fillna(0).ne(0).astype(int)
    df['group'] = df['change'].cumsum()
    df['first_out_date'] = df[df['IS_OUT'] == 1].groupby('group')['GAME_DATE'].transform('min')
    df['first_in_date'] = df[df['IS_OUT'] == 0].groupby('group')['GAME_DATE'].transform('min')
    df['last_known_out'] = df['first_out_date'].ffill()
    df['DAYS_OUT'] = np.where(pd.notna(df['first_in_date']),(df['first_in_date'] - df['last_known_out']).dt.days, 0)
    df['DAYS_OUT'] = df['DAYS_OUT'].clip(lower=0).fillna(0).astype(int)
    return df

def update_opponent(opp_obj, index, pos_df):
    opp_player_id_col,def_pts_col,def_ast_col,def_reb_col = pos_df.columns.get_loc('OPP_PLAYER_ID'),pos_df.columns.get_loc('DEF_PTS'),pos_df.columns.get_loc('DEF_AST'),pos_df.columns.get_loc('DEF_REB')
    opp_player_id, opp_pts, opp_ast, opp_reb  = opp_obj.iloc[0]['PLAYER_ID'],opp_obj.iloc[0]['PTS'],opp_obj.iloc[0]['AST'],opp_obj.iloc[0]['REB']
    pos_df.iat[index, opp_player_id_col] = opp_player_id
    pos_df.iat[index, def_pts_col] = opp_pts
    pos_df.iat[index, def_ast_col] = opp_ast
    pos_df.iat[index, def_reb_col] = opp_reb
    pos_df.loc[pos_df.index[pos_df['PLAYER_ID'] == opp_player_id], 'MATCHED_OPPONENT'] = 1
    return pos_df

def find_opponent(item, pos_df, benchcheck=False):
    array_vals = ['TEAM_NAME', 'PLAYER_ID', 'START_POSITION', 'POSITION', 'PTS', 'AST', 'REB', 'MIN']
    iposition = item['POSITION']
    ioppteam = item['OPP_NAME']
    imin = item['MIN']
    opp_obj = None
    string_iposition = str(iposition) if pd.notna(iposition) else ''
    if string_iposition == '':
        raise ValueError(f"Invalid Position Found for {item['PLAYER_ID']} in game {item['GAME_ID']}")
    positions = [iposition] + iposition.split('-') if '-' in iposition else [iposition]
    base_opp_obj = pos_df[(pos_df['TEAM_NAME'] == ioppteam) & (pos_df['MATCHED_OPPONENT'] == 0)]
    if len(positions) < 1:
        raise ValueError(f"No Valid Positions Found for {item['PLAYER_ID']}  in game {item['GAME_ID']}")
    for pos in positions:
        opp_obj = base_opp_obj[base_opp_obj['POSITION'] == pos][array_vals]
        if not opp_obj.empty: break

    if opp_obj.empty:
        opp_obj = base_opp_obj[array_vals]
        if not opp_obj.empty and len(opp_obj) > 1:
            closest_min_index = (opp_obj['MIN'] - imin).abs().idxmin()
            opp_obj = opp_obj.loc[[closest_min_index]]

    if benchcheck and opp_obj.empty:
        bench_opp_obj  = pos_df[pos_df['TEAM_NAME'] == ioppteam]
        for pos in positions:
            opp_obj = bench_opp_obj [bench_opp_obj ['POSITION'] == pos][array_vals]
            if not opp_obj.empty: break

            if opp_obj.empty:
                opp_obj = bench_opp_obj [array_vals]
                if not opp_obj.empty and len(opp_obj) > 1:
                    closest_min_index = (opp_obj['MIN'] - imin).abs().idxmin()
                    opp_obj = opp_obj.loc[[closest_min_index]]

    if opp_obj.empty:
        raise ValueError("No Valid Opponent Found...")
    return opp_obj


def optimized_createmainrollingtotals(gl_df, gl_path):
    roll_columns = ['MIN', 'PTS', 'REB', 'AST', 'STL', 'BLK', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'PF']
    new_columns = {}
    if 'RT3_PTS' not in gl_df.columns:
        for col in roll_columns:
            new_columns['AVG_' + col] = optimized_rolling_avg_season(gl_df, col).round(2).fillna(0)
            new_columns['RT3_' + col] = optimized_rolling_averages(gl_df, col, 3).round(2).fillna(0)
            new_columns['RT5_' + col] = optimized_rolling_averages(gl_df, col, 5).round(2).fillna(0)
            new_columns['RT9_' + col] = optimized_rolling_averages(gl_df, col, 9).round(2).fillna(0)
        new_columns_df = pd.DataFrame(new_columns, index=gl_df.index)
        gl_df = pd.concat([gl_df, new_columns_df], axis=1)
        gl_df.sort_values(by='GAME_DATE', ascending=True, inplace=True)
        gl_df.to_csv(gl_path, index=False)
        gl_df.sort_values(by=['SEASON', 'PLAYER_ID', 'GAME_DATE'], ascending=True, inplace=True)
        for col in roll_columns:
            gl_df[f'RT3H_{col}'] = rolling_home_or_away_consecutive(gl_df, col, 1).round(2)
            gl_df[f'RT3H_{col}'] = gl_df[f'RT3H_{col}'].round(2).fillna(0)
            gl_df[f'RT3A_{col}'] = rolling_home_or_away_consecutive(gl_df, col, 0).round(2)
            gl_df[f'RT3A_{col}'] = gl_df[f'RT3A_{col}'].round(2).fillna(0)
        gl_df.to_csv(gl_path, index=False)
        for col in roll_columns:
            gl_df[f'RTZ_{col}'] = gl_df[f'RT3H_{col}'] + gl_df[f'RT3A_{col}']
            gl_df = gl_df.drop([f'RT3H_{col}', f'RT3A_{col}'], axis=1)
        gl_df.to_csv(gl_path, index=False)
        print(f"Processed: createrollingtotals: {gl_path}")
    return gl_df

def set_extra_stats(gl_df, gl_path):
    change_made = False
    stats_columns = ['PTS', 'AST', 'REB', 'STL', 'BLK', 'MIN', 'FGA', 'FGM', 'FTA', 'FTM', 'FG3A', 'FG3M', 'PF']
    if 'PREV_PTS' not in gl_df.columns:
        for column in stats_columns:
            gl_df[f'PREV_{column}'] = gl_df.groupby(['SEASON', 'PLAYER_ID'])[column].shift(fill_value=0)
        change_made = True
    if 'MAX_PTS' not in gl_df.columns:
        for column in stats_columns:
            max_column = f'MAX_{column}'
            if max_column not in gl_df.columns:
                gl_df[max_column] = 0
        for column in stats_columns:
            max_column = f'MAX_{column}'
            gl_df[max_column] = gl_df.groupby(['PLAYER_ID', 'SEASON'])[column].transform(lambda x: x.cummax().shift(1)).fillna(0).astype(int)
            change_made = True
    if 'GAME_DAY' not in gl_df.columns:
        game_date_str = pd.to_datetime(gl_df['GAME_DATE'], format='%Y-%m-%d')
        gl_df['GAME_DAY'] = game_date_str.dt.day_name()
        change_made = True
    if 'LAST_GAME_DAYS' not in gl_df.columns:
        gl_df['GAME_DATE'] = pd.to_datetime(gl_df['GAME_DATE'])
        gl_df = gl_df.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
        gl_df['LAST_GAME_DAYS'] = gl_df.groupby('PLAYER_ID')['GAME_DATE'].diff().dt.days.fillna(0).astype(int)
        gl_df['BACKTOBACKGAME'] = (gl_df['LAST_GAME_DAYS'] == 1).astype(int)
        gl_df['WEEK_PLAYTIME'] = 0
        for player_id in gl_df['PLAYER_ID'].unique():
            player_index = gl_df['PLAYER_ID'] == player_id
            gl_df.loc[player_index, 'WEEK_PLAYTIME'] = gl_df.loc[player_index].set_index('GAME_DATE')['MIN'].rolling('7D').sum().values
        gl_df['WEEK_PLAYTIME'].fillna(0, inplace=True)
        gl_df['WEEK_PLAYTIME'] = gl_df['WEEK_PLAYTIME'] - gl_df['MIN']
        change_made = True
    if 'IS_STARTING' not in gl_df.columns:
        gl_df['IS_STARTING'] = np.where(gl_df['START_POSITION'].isin(['C', 'F', 'G']), 1, 0)
        change_made = True
    if change_made:
        gl_df.to_csv(gl_path, index=False)
        print(f"Processed: set_extra_stats: {gl_path}")
    return gl_df

def set_opponent_name(df_log,file_path):
    print('set_opponent_name started...')
    if 'OPP_PLAYER_NAME' not in df_log.columns:
        print(f'Generating OPP_PLAYER_NAME for file:{file_path}')
        player_id_to_name = pd.Series(df_log.PLAYER_NAME.values, index=df_log.PLAYER_ID).to_dict()
        df_log['OPP_PLAYER_NAME'] = df_log['OPP_PLAYER_ID'].map(player_id_to_name).fillna('')
        df_log.to_csv(file_path, index=False)
    print('set_opponent_name completed...')

def set_opponent_def(df_log,file_path):
    print('set_opponent_def started...')
    if not 'OPP_LAST_GAME_DAYS' in df_log.columns:
        start_time = time.time()
        for col in ['OPP_LAST_GAME_DAYS','OPP_BACKTOBACKGAME','OPP_WEEK_PLAYTIME','OPP_PREV_PF','OPP_PREV_STL','OPP_PREV_BLK']:
            df_log[col] = 0
        for col in [ 'OPP_RT3_PF','OPP_AVG_PF','OPP_RT3_STL','OPP_AVG_STL','OPP_RT3_BLK','OPP_AVG_BLK']:
            df_log[col] = 0.0
        opponent_info = {(row['PLAYER_ID'], row['TEAM_NAME'], row['GAME_DATE']): row for index, row in df_log.iterrows()}
        for index, row in df_log.iterrows():
            key = (row['OPP_PLAYER_ID'], row['OPP_NAME'], row['GAME_DATE'])
            if key in opponent_info:
                opponent_row = opponent_info[key]
                df_log.at[index, 'OPP_LAST_GAME_DAYS'] = opponent_row['LAST_GAME_DAYS'] if pd.notna(opponent_row['LAST_GAME_DAYS']) and opponent_row['LAST_GAME_DAYS'] != 0 else 0
                df_log.at[index, 'OPP_BACKTOBACKGAME'] = opponent_row['BACKTOBACKGAME'] if pd.notna(opponent_row['BACKTOBACKGAME']) and opponent_row['BACKTOBACKGAME'] != 0 else 0
                df_log.at[index, 'OPP_WEEK_PLAYTIME'] = opponent_row['WEEK_PLAYTIME'] if pd.notna(opponent_row['WEEK_PLAYTIME']) and opponent_row['WEEK_PLAYTIME'] != 0 else 0
                df_log.at[index, 'OPP_PREV_PF'] = opponent_row['PREV_PF'] if pd.notna(opponent_row['PREV_PF']) and opponent_row['PREV_PF'] != 0 else 0
                df_log.at[index, 'OPP_RT3_PF'] = opponent_row['RT3_PF'] if pd.notna(opponent_row['RT3_PF']) and opponent_row['RT3_PF'] != 0 else 0.0
                df_log.at[index, 'OPP_AVG_PF'] = opponent_row['AVG_PF'] if pd.notna(opponent_row['AVG_PF']) and opponent_row['AVG_PF'] != 0 else 0.0
                df_log.at[index, 'OPP_PREV_STL'] = opponent_row['PREV_STL'] if pd.notna(opponent_row['PREV_STL']) and opponent_row['PREV_STL'] != 0 else 0
                df_log.at[index, 'OPP_RT3_STL'] = opponent_row['RT3_STL'] if pd.notna(opponent_row['RT3_STL']) and opponent_row['RT3_STL'] != 0 else 0.0
                df_log.at[index, 'OPP_AVG_STL'] = opponent_row['AVG_STL'] if pd.notna(opponent_row['AVG_STL']) and opponent_row['AVG_STL'] != 0 else 0.0
                df_log.at[index, 'OPP_PREV_BLK'] = opponent_row['PREV_BLK'] if pd.notna(opponent_row['PREV_BLK']) and opponent_row['PREV_BLK'] != 0 else 0
                df_log.at[index, 'OPP_RT3_BLK'] = opponent_row['RT3_BLK'] if pd.notna(opponent_row['RT3_BLK']) and opponent_row['RT3_BLK'] != 0 else 0.0
                df_log.at[index, 'OPP_AVG_BLK'] = opponent_row['AVG_BLK'] if pd.notna(opponent_row['AVG_BLK']) and opponent_row['AVG_BLK'] != 0 else 0.0
            else:
                raise ValueError(f"No player found for GAME_ID:{row['GAME_ID']} --> OPP_PLAYER_ID:{row['OPP_PLAYER_ID']}-TEAM:{row['OPP_NAME']}  PLAYER_ID:{row['PLAYER_ID']}")
        df_log.to_csv(file_path, index=False)
        print(f'Generated (OPP_PLAYER_NAME, OPP_LAST_GAME_DAYS,OPP_BACKTOBACKGAME,OPP_WEEK_PLAYTIME,OPP_PREV_PF,OPP_RT3_STL,OPP_AVG_BLK) completed in {round(time.time() - start_time, 2)} seconds..')
    df_log = pd.read_csv(file_path)
    if 'OPP_DEF_PTS1' not in df_log.columns:
        start_time = time.time()
        df_log['GAME_DATE'] = pd.to_datetime(df_log['GAME_DATE'])
        df_log = df_log.sort_values(by='GAME_DATE')
        stats = ['DEF_PTS', 'DEF_REB', 'DEF_AST']
        df_temp = df_log[['PLAYER_ID', 'GAME_DATE'] + stats].copy()
        df_temp.rename(columns={'PLAYER_ID': 'OPP_PLAYER_ID'}, inplace=True)
        df_log = pd.merge(df_log, df_temp, on=['OPP_PLAYER_ID', 'GAME_DATE'], how='left', suffixes=('', '_PREV'))
        for stat in stats:
            for lag in [1, 3, 5, 9]:
                column_name = f'OPP_{stat}{lag}'
                df_log[column_name] = df_log.groupby(['OPP_PLAYER_ID'])[f'{stat}_PREV'].transform(lambda x: x.shift().rolling(window=lag, min_periods=1).mean().round(2))
                df_log[column_name].fillna(0, inplace=True)
            avg_column_name = f'OPP_{stat}AVG'
            df_log[avg_column_name] = df_log.groupby(['OPP_PLAYER_ID'])[f'{stat}_PREV'].transform(lambda x: x.shift().expanding(min_periods=1).mean().round(2))
            df_log[avg_column_name].fillna(0, inplace=True)
        df_log.drop(columns=[f'{stat}_PREV' for stat in stats], inplace=True)
        df_log.to_csv(file_path, index=False)
        print(f'Generated (OPP_DEF1_PTS,OPP_DEF1_AST,OPP_DEF1_REB) completed in {round(time.time() - start_time, 2)} seconds..')
    print('set_opponent_def completed...')
    return df_log


def setup_season_values():
    print('setup_season_values started...')
    for file_name in os.listdir(gls.SEASON_DATA_DIR):
        file_path = os.path.join(gls.SEASON_DATA_DIR, file_name)
        df_log = pd.read_csv(file_path)
        print(f'Filtering {file_path} starting...')
        df_log = optimized_createmainrollingtotals(df_log, file_path)
        df_log = set_extra_stats(df_log, file_path)
        df_log = set_opponent_def(df_log, file_path)
        set_opponent_name(df_log, file_path)
    print('setup_season_values completed...')

def combine_games_to_season():
    print('combine_games_to_season started...')
    for item in os.listdir(gls.GAMES_DATA_DIR):
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        df_all_logs = pd.DataFrame()
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Season {item} starting...')
        f_path = f'{gls.SEASON_DATA_DIR}{item}{gls.DEFAULT_CSV_TYPENAME}'
        if not os.path.exists(f_path) or rns.run_from_start_to_finish:
            for file_name in os.listdir(directory):
                file_path = os.path.join(directory, file_name)
                df_log = pd.read_csv(file_path)
                if not df_log.empty:
                    df_all_logs = pd.concat([df_all_logs, df_log], ignore_index=True)
            df_all_logs['WINS'] = df_all_logs['WINS'].astype('Int32')
            df_all_logs['LOSSES'] = df_all_logs['LOSSES'].astype('Int32')
            df_all_logs['OFFICIAL1'] = df_all_logs['OFFICIAL1'].astype('Int32')
            df_all_logs['OFFICIAL2'] = df_all_logs['OFFICIAL2'].astype('Int32')
            df_all_logs['MIN'] = df_all_logs['MIN'].astype('Int32')
            df_all_logs['FGM'] = df_all_logs['FGM'].astype('Int32')
            df_all_logs['FGA'] = df_all_logs['FGA'].astype('Int32')
            df_all_logs['FG3M'] = df_all_logs['FG3M'].astype('Int32')
            df_all_logs['FG3A'] = df_all_logs['FG3A'].astype('Int32')
            df_all_logs['FTM'] = df_all_logs['FTM'].astype('Int32')
            df_all_logs['FTA'] = df_all_logs['FTA'].astype('Int32')
            df_all_logs['OREB'] = df_all_logs['OREB'].astype('Int32')
            df_all_logs['DREB'] = df_all_logs['DREB'].astype('Int32')
            df_all_logs['REB'] = df_all_logs['REB'].astype('Int32')
            df_all_logs['AST'] = df_all_logs['AST'].astype('Int32')
            df_all_logs['STL'] = df_all_logs['STL'].astype('Int32')
            df_all_logs['BLK'] = df_all_logs['BLK'].astype('Int32')
            df_all_logs['TO'] = df_all_logs['TO'].astype('Int32')
            df_all_logs['PF'] = df_all_logs['PF'].astype('Int32')
            df_all_logs['PTS'] = df_all_logs['PTS'].astype('Int32')
            df_all_logs['OPP_WINS'] = df_all_logs['OPP_WINS'].astype('Int32')
            df_all_logs['OPP_LOSSES'] = df_all_logs['OPP_LOSSES'].astype('Int32')
            df_all_logs['PLUS_MINUS'] = df_all_logs['PLUS_MINUS'].astype('Int32')
            df_all_logs['TEAM_DNP'] = df_all_logs['TEAM_DNP'].astype('Int32')
            df_all_logs['TEAM_OUT'] = df_all_logs['TEAM_OUT'].astype('Int32')
            df_all_logs['OPP_DNP'] = df_all_logs['OPP_DNP'].astype('Int32')
            df_all_logs['OPP_OUT'] = df_all_logs['OPP_OUT'].astype('Int32')
            df_all_logs['DISTANCE'] = df_all_logs['DISTANCE'].astype('Int32')
            df_all_logs['OPP_DISTANCE'] = df_all_logs['OPP_DISTANCE'].astype('Int32')
            df_all_logs['TWIN'] = df_all_logs['TWIN'].astype('Int32')
            df_all_logs['TLOSS'] = df_all_logs['TLOSS'].astype('Int32')
            df_all_logs['OWIN'] = df_all_logs['OWIN'].astype('Int32')
            df_all_logs['OLOSS'] = df_all_logs['OLOSS'].astype('Int32')
            df_all_logs['GAMES_IN'] = df_all_logs['GAMES_IN'].astype('Int32')
            df_all_logs['GAMES_OUT'] = df_all_logs['GAMES_OUT'].astype('Int32')
            df_all_logs['GAMES_CONT'] = df_all_logs['GAMES_CONT'].astype('Int32')
            df_all_logs['GAMES_START'] = df_all_logs['GAMES_START'].astype('Int32')
            df_all_logs['GAMES_BENCH'] = df_all_logs['GAMES_BENCH'].astype('Int32')
            df_all_logs['TEAM_OUT_START'] = df_all_logs['TEAM_OUT_START'].astype('Int32')
            df_all_logs['TEAM_OUT_BENCH'] = df_all_logs['TEAM_OUT_BENCH'].astype('Int32')
            df_all_logs['OPP_OUT_START'] = df_all_logs['OPP_OUT_START'].astype('Int32')
            df_all_logs['OPP_OUT_BENCH'] = df_all_logs['OPP_OUT_BENCH'].astype('Int32')
            df_all_logs['DEF_PTS'] = df_all_logs['DEF_PTS'].astype('Int32')
            df_all_logs['DEF_AST'] = df_all_logs['DEF_AST'].astype('Int32')
            df_all_logs['DEF_REB'] = df_all_logs['DEF_REB'].astype('Int32')
            df_all_logs['OPP_PLAYER_ID'] = df_all_logs['OPP_PLAYER_ID'].astype('Int32')
            df_all_logs['IS_OUT'] = np.where(df_all_logs['COMMENT'].isna() | (df_all_logs['COMMENT'] == ''), 0, 1).astype(int)
            df_all_logs['GAME_DATE'] = pd.to_datetime(df_all_logs['GAME_DATE'])
            df_all_logs = df_all_logs.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
            df_all_logs = df_all_logs.groupby('PLAYER_ID').apply(process_player_group)
            df_all_logs.reset_index(drop=True, inplace=True)
            df_all_logs.drop(columns=['change','group','first_out_date','first_in_date','last_known_out'], inplace=True)
            df_all_logs = df_all_logs[(df_all_logs['COMMENT'].isna()) | (df_all_logs['COMMENT'] == '')]
            df_all_logs = df_all_logs.drop(columns=['COMMENT'])
            if os.path.exists(f_path):
                print('File already exists removing old file..')
                os.remove(f_path)
            df_all_logs.to_csv(f_path, index=False)
            print(f'Saved: {f_path}')
        else:
            print('Skipping not force updating..')
    print('combine_games_to_season completed...')

def track_player_out_events():
    print('track_player_out_events started...')
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        PLAYER_TRACKER = {}
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'GAMES_IN'):
                df_log = pd.read_csv(file_path)
                df_log['GAMES_IN'] = 0
                df_log['GAMES_OUT'] = 0
                df_log['GAMES_CONT'] = 0
                df_log['GAMES_START'] = 0
                df_log['GAMES_BENCH'] = 0
                for index, row in df_log.iterrows():
                    comment = row['COMMENT']
                    start_position = row['START_POSITION']
                    player_id = row['PLAYER_ID']
                    keyword = ''
                    if pd.notna(comment) and len(comment) > 0:
                        keyword = comment.split('-')[0].strip()
                        keyword = str(keyword).upper()
                        keyword = 'OUT' if any(status_keyword in keyword for status_keyword in OUT_KEYWORDS_SET) else keyword
                    player_data = PLAYER_TRACKER.get(player_id, [0, 0, 0, 0, 0])
                    if keyword == '':
                        player_data[0] = player_data[0] + 1
                        player_data[2] = player_data[2] + 1
                        if start_position in ['G','C','F']:
                            player_data[3] = player_data[3] + 1
                        elif start_position == 'B':
                            player_data[4] = player_data[4] + 1
                        else:
                            print(f'Error Unknown position... player_id: {player_id} position: {start_position}')
                    if keyword != '':
                        player_data[1] = player_data[1] + 1
                        player_data[2] = 0
                    PLAYER_TRACKER[player_id] = player_data
                    df_log.at[index, 'GAMES_IN'] = PLAYER_TRACKER[player_id][0]
                    df_log.at[index, 'GAMES_OUT'] = PLAYER_TRACKER[player_id][1]
                    df_log.at[index, 'GAMES_CONT'] = PLAYER_TRACKER[player_id][2]
                    df_log.at[index, 'GAMES_START'] = PLAYER_TRACKER[player_id][3]
                    df_log.at[index, 'GAMES_BENCH'] = PLAYER_TRACKER[player_id][4]
                df_log.to_csv(file_path, index=False)
    print('track_player_out_events completed...')

def set_distance_altitude():
    print('set_distance_altitude started...')
    distance_df = pd.read_csv(gls.LOCATION_DISTANCES)
    with open(gls.TEAM_LOCATIONS_JSON, 'r', encoding='utf-8') as file:
        altitudes_json = json.load(file)
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'DISTANCE'):
                df_log = pd.read_csv(file_path)
                home_team_abbrev = df_log[df_log['IS_HOME'] == 1]['TEAM_NAME'].iloc[0]
                away_team_abbrev = df_log[df_log['IS_HOME'] == 0]['TEAM_NAME'].iloc[0]
                specific_distance = distance_df[(distance_df['HOME'] == home_team_abbrev) & (distance_df['AWAY'] == away_team_abbrev)]
                if len(specific_distance) < 1:
                    print(f'Error distance not found for: {home_team_abbrev} and {away_team_abbrev}...')
                else:
                    distance_val = specific_distance['DISTANCE'].values[0]
                    df_log['DISTANCE'] = df_log.apply(lambda x: 0 if x['IS_HOME'] == 1 else distance_val, axis=1)
                    df_log['OPP_DISTANCE'] = df_log.apply(lambda x: distance_val if x['IS_HOME'] == 1 else 0, axis=1)
                    ht_row = df_log[df_log['IS_HOME'] == 1].iloc[0]
                    ht_name = ht_row['TEAM_NAME']
                    altitude = altitudes_json[ht_name]['altitude']
                    df_log['ALTITUDE'] = int(altitude)
                    df_log.to_csv(file_path, index=False)
    print('set_distance_altitude completed...')

def set_opponents():
    print('set_opponents started...')
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'OPP_NAME'):
                df_log = pd.read_csv(file_path)
                unique_teams_rows = df_log.drop_duplicates(subset=['TEAM_NAME'])
                t1, t2 = unique_teams_rows.iloc[0], unique_teams_rows.iloc[1]
                df_log['OPP_NAME'] = df_log['TEAM_NAME'].apply(lambda x: t2['TEAM_NAME'] if x == t1['TEAM_NAME'] else t1['TEAM_NAME'])
                df_log['OPP_ID'] = df_log['TEAM_NAME'].apply(lambda x: t2['TEAM_ID'] if x == t1['TEAM_NAME'] else t1['TEAM_ID'])
                df_log['OPP_WINS'] = df_log['TEAM_NAME'].apply(lambda x: t2['WINS'] if x == t1['TEAM_NAME'] else t1['WINS'])
                df_log['OPP_LOSSES'] = df_log['TEAM_NAME'].apply(lambda x: t2['LOSSES'] if x == t1['TEAM_NAME'] else t1['LOSSES'])
                df_log.to_csv(file_path, index=False)
    print('set_opponents completed...')

def set_positions_and_cleanup():
    print('set_positions_and_cleanup started...')
    df_all_details = pd.DataFrame()
    for filename in os.listdir(gls.PLAYER_DETAIL_DIR):
        file_path = os.path.join(gls.PLAYER_DETAIL_DIR, filename)
        df_detail = pd.read_csv(file_path)
        if not df_detail.empty:
            df_all_details = pd.concat([df_all_details, df_detail])
    df_all_details.sort_values(by=['SEASON', 'TEAM'], ascending=True, inplace=True)
    print('Loaded player details..')
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if '22400014' in file_name:
                print('check....')
            if glu.file_contains_value(file_path, 'NICKNAME'):
                df_log = pd.read_csv(file_path)
                df_log.drop(['NICKNAME'], axis=1, inplace=True)
                df_log.drop(['OFFICIAL3'], axis=1, inplace=True)
                mask = df_log['COMMENT'].isna() & df_log['START_POSITION'].isna()
                df_log.loc[mask, 'START_POSITION'] = 'B'
                df_log.to_csv(file_path, index=False)
            if glu.file_contains_value(file_path, 'OFFICIAL1NAME'):
                df_log = pd.read_csv(file_path)
                df_log.drop(['OFFICIAL1NAME'], axis=1, inplace=True)
                df_log.to_csv(file_path, index=False)
            if glu.file_contains_value(file_path, 'OFFICIAL2NAME'):
                df_log = pd.read_csv(file_path)
                df_log.drop(['OFFICIAL2NAME'], axis=1, inplace=True)
                df_log.to_csv(file_path, index=False)
            if glu.file_contains_value(file_path, 'OFFICIAL3NAME'):
                df_log = pd.read_csv(file_path)
                df_log.drop(['OFFICIAL3NAME'], axis=1, inplace=True)
                df_log.to_csv(file_path, index=False)
            if not glu.file_contains_value(file_path, ',POSITION'):
                df_log = pd.read_csv(file_path)
                merged_df = pd.merge(df_log, df_all_details[['PLAYER_ID', 'TEAM_ID', 'SEASON', 'POSITION']],on=['PLAYER_ID', 'TEAM_ID', 'SEASON'], how='left')
                df_log['POSITION'] = merged_df['POSITION'].str.upper()
                missing_position_rows = df_log[df_log['POSITION'].isna()]
                missing_position_rows = missing_position_rows[missing_position_rows['COMMENT'].str.len() < 1]
                if not missing_position_rows.empty:
                    game_id = missing_position_rows['GAME_ID'].iloc[0]
                    print(f"Missing POSITIONS in GAME_ID: {game_id}")
                    for _, row in missing_position_rows.iterrows():
                        print(f"PLAYER_ID: {row['PLAYER_ID']}")
                df_log.to_csv(file_path, index=False)
    print('set_positions_and_cleanup completed...')

def set_player_opponents():
    print('set_player_opponents started...')
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'MATCHED_OPPONENT'):
                # try:
                df_log = pd.read_csv(file_path)
                df_log['MATCHED_OPPONENT'] = 0
                ###### Match Centers
                c_indices = df_log[df_log['START_POSITION'] == 'C'].index[:2]
                count_C_positions = (df_log['START_POSITION'] == 'C').sum()
                if count_C_positions != 2:
                    raise ValueError(f"Count of 'C' in START_POSITION is invalid: {count_C_positions}")
                for src_col, dest_col in [('PLAYER_ID', 'OPP_PLAYER_ID'), ('PTS', 'DEF_PTS'), ('AST', 'DEF_AST'), ('REB', 'DEF_REB')]:
                    temp = df_log.at[c_indices[0], src_col]
                    df_log.at[c_indices[0], dest_col] = df_log.at[c_indices[1], src_col]
                    df_log.at[c_indices[1], dest_col] = temp
                df_log.loc[c_indices, 'MATCHED_OPPONENT'] = 1
                ###### Match Forwards
                pos_df = df_log[(df_log['START_POSITION'] == 'F')]
                pos_df.reset_index(inplace=True)
                for index, row in pos_df.iterrows():
                    opp_obj = find_opponent(item=row, pos_df=pos_df)
                    pos_df = update_opponent(opp_obj=opp_obj, pos_df=pos_df, index=index)
                df_log.set_index('PLAYER_ID', inplace=True)
                pos_df.set_index('PLAYER_ID', inplace=True)
                df_log.update(pos_df)
                df_log.reset_index(inplace=True)
                ###### Match Guards
                pos_df = df_log[(df_log['START_POSITION'] == 'G')]
                pos_df.reset_index(inplace=True)
                for index, row in pos_df.iterrows():
                    opp_obj = find_opponent(item=row, pos_df=pos_df)
                    pos_df = update_opponent(opp_obj=opp_obj, pos_df=pos_df, index=index)
                df_log.set_index('PLAYER_ID', inplace=True)
                pos_df.set_index('PLAYER_ID', inplace=True)
                df_log.update(pos_df)
                df_log.reset_index(inplace=True)
                ###### Match Bench
                pos_df = df_log[(df_log['START_POSITION'] == 'B')]
                pos_df.reset_index(inplace=True)
                for index, row in pos_df.iterrows():
                    opp_obj = find_opponent(item=row, pos_df=pos_df, benchcheck=True)
                    pos_df = update_opponent(opp_obj=opp_obj, pos_df=pos_df, index=index)
                df_log.set_index('PLAYER_ID', inplace=True)
                pos_df.set_index('PLAYER_ID', inplace=True)
                df_log.update(pos_df)
                df_log.reset_index(inplace=True)
                df_log.to_csv(file_path, index=False)
                # except Exception as e:
                #     print(f"Error processing {file_path}: {e}")
    print('set_player_opponents completed...')

def set_out_totals():
    print('set_out_totals started...')
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
                file_path = os.path.join(directory, file_name)
                if not glu.file_contains_value(file_path, 'TEAM_OUT_START'):
                    df_log = pd.read_csv(file_path)
                    df_log['TEAM_OUT_START'] = 0
                    df_log['TEAM_OUT_BENCH'] = 0
                    df_log['OPP_OUT_START'] = 0
                    df_log['OPP_OUT_BENCH'] = 0
                    for (game_id, team_name), group in df_log.groupby(['GAME_ID', 'TEAM_NAME']):
                        team_out_start = group[(group['COMMENT'].notna())]['GAMES_START'].sum()
                        team_out_bench = group[(group['COMMENT'].notna())]['GAMES_BENCH'].sum()
                        team_mask = (df_log['GAME_ID'] == game_id) & (df_log['TEAM_NAME'] == team_name)
                        df_log.loc[team_mask, 'TEAM_OUT_START'] = team_out_start
                        df_log.loc[team_mask, 'TEAM_OUT_BENCH'] = team_out_bench
                        opp_team = df_log[(df_log['GAME_ID'] == game_id) & (df_log['TEAM_NAME'] != team_name)]['TEAM_NAME'].unique()[0]
                        opp_group = df_log[(df_log['GAME_ID'] == game_id) & (df_log['TEAM_NAME'] == opp_team)]
                        opp_out_start = opp_group[(opp_group['COMMENT'].notna())]['GAMES_START'].sum()
                        opp_out_bench = opp_group[(opp_group['COMMENT'].notna())]['GAMES_BENCH'].sum()
                        opp_mask = (df_log['GAME_ID'] == game_id) & (df_log['OPP_NAME'] == opp_team)
                        df_log.loc[opp_mask, 'OPP_OUT_START'] = opp_out_start
                        df_log.loc[opp_mask, 'OPP_OUT_BENCH'] = opp_out_bench
                    df_log.to_csv(file_path, index=False)
    print('set_out_totals completed...')

def set_current_wins():
    print('set_current_wins started...')
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'TWIN'):
                df_log = pd.read_csv(file_path)
                condition = df_log['IS_WIN'] == 1
                df_log['TWIN'] = np.where(condition, df_log['WINS'] - 1, df_log['WINS'])
                df_log['TLOSS'] = np.where(condition, df_log['LOSSES'], df_log['LOSSES'] - 1)
                df_log['OWIN'] = np.where(condition, df_log['OPP_WINS'], df_log['OPP_WINS'] - 1)
                df_log['OLOSS'] = np.where(condition, df_log['OPP_LOSSES'] - 1, df_log['OPP_LOSSES'])
                df_log.to_csv(file_path, index=False)
    print('set_current_wins completed...')

def set_invalid_players():
    print('set_invalid_players started...')
    all_keywords = {'DNP': 0, 'OUT': 0}
    t1_keywords = {'TEAM':'', 'DNP': 0, 'OUT': 0}
    t2_keywords = {'TEAM':'', 'DNP': 0, 'OUT': 0}
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'TEAM_DNP'):
                df_log = pd.read_csv(file_path)
                for index, row in df_log.iterrows():
                    comment = row['COMMENT']
                    if row['IS_HOME'] == 1 and len(t1_keywords['TEAM']) == 0:
                        t1_keywords['TEAM'] = row['TEAM_NAME']
                    if row['IS_HOME'] == 0 and len(t2_keywords['TEAM']) == 0:
                        t2_keywords['TEAM'] = row['TEAM_NAME']
                    if pd.notna(comment) and len(comment) > 0:
                        keyword = comment.split('-')[0].strip()
                        keyword = str(keyword).upper()
                        keyword = 'OUT' if any(status_keyword in keyword for status_keyword in OUT_KEYWORDS_SET) else keyword
                        if keyword not in all_keywords:
                            print(f'Issue checking {keyword} not in -> {all_keywords}')
                        else:
                            all_keywords[keyword] += 1
                        if row['IS_HOME'] == 1:
                            t1_keywords[keyword] += 1
                        else:
                            t2_keywords[keyword] += 1
                df_log['TEAM_DNP'] = df_log['TEAM_NAME'].apply(lambda x: t1_keywords['DNP'] if x == t1_keywords['TEAM'] else t2_keywords['DNP'])
                df_log['TEAM_OUT'] = df_log['TEAM_NAME'].apply(lambda x: t1_keywords['OUT'] if x == t1_keywords['TEAM'] else t2_keywords['OUT'])
                df_log['OPP_DNP'] = df_log['TEAM_NAME'].apply(lambda x: t2_keywords['DNP'] if x == t1_keywords['TEAM'] else t1_keywords['DNP'])
                df_log['OPP_OUT'] = df_log['TEAM_NAME'].apply(lambda x: t2_keywords['OUT'] if x == t1_keywords['TEAM'] else t1_keywords['OUT'])
                df_log.to_csv(file_path, index=False)
                for keyword, count in all_keywords.items():
                    all_keywords[keyword] = 0
                    t1_keywords[keyword] = 0
                    t1_keywords['TEAM'] = ''
                    t2_keywords[keyword] = 0
                    t2_keywords['TEAM'] = ''
    print('set_invalid_players completed...')

def fix_prediction_values():
    print('fix_prediction_values...')
    pred_year = rns.prediction_season
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if pred_year not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            df_log = pd.read_csv(file_path)
            if 'IS_PREDICTOR' in df_log.columns:
                first_row = df_log.head(1)
                is_pred = first_row['IS_PREDICTOR'][0]
                if is_pred == 1:
                    df_log['TWIN'] = df_log['WINS']
                    df_log['TLOSS'] = df_log['LOSSES']
                    df_log['OWIN'] = df_log['OPP_WINS']
                    df_log['OLOSS'] = df_log['OPP_LOSSES']
                    df_log.to_csv(file_path, index=False)
                    print(f'Updated pred values for {file_path}')

def clear_calculations():
    print('clear_calculations started...')
    drop_columns = ['TEAM_DNP', 'TEAM_OUT', 'OPP_DNP', 'OPP_OUT', 'OPP_NAME', 'OPP_ID', 'OPP_WINS','OPP_LOSSES', 'DISTANCE',
                    'OPP_DISTANCE', 'TWIN', 'TLOSS','OWIN','OLOSS','GAMES_IN', 'GAMES_OUT', 'GAMES_CONT', 'GAMES_START',
                    'GAMES_BENCH','TEAM_OUT_START','TEAM_OUT_BENCH','OPP_OUT_START','OPP_OUT_BENCH','ALTITUDE','POSITION'
                    ,'DEF_PTS','DEF_AST','DEF_REB','OPP_PLAYER_ID','MATCHED_OPPONENT','OPP_PLAYER_NAME']
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if rns.current_season_only and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        directory = f'{gls.GAMES_DATA_DIR}{item}/'
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            df_log = pd.read_csv(file_path)
            for col in drop_columns:
                if col in df_log.columns:
                    df_log = df_log.drop(columns=col)

            df_log['PLAYER_NAME'] = df_log['PLAYER_NAME'].apply(unidecode)
            df_log.to_csv(file_path, index=False)
        f_path = f'{gls.SEASON_DATA_DIR}{item}{gls.DEFAULT_CSV_TYPENAME}'
        if os.path.exists(f_path):
            print(f'Removed season file for {item}')
            os.remove(f_path)
    print('clear_calculations completed...')

print('Starting...')
if rns.run_from_start_to_finish:
    glu.remove_folder_and_contents(gls.TOP_OUTPUT_DIR)
    cgl.create_game_logs_start()
gnd.generate_predicitons_start()
clear_calculations()
pld.create_player_details()
pld.delete_invalid_player_details()
set_invalid_players()
set_positions_and_cleanup()
set_distance_altitude()
track_player_out_events()
set_opponents()
set_player_opponents()
set_out_totals()
set_current_wins()
fix_prediction_values()
combine_games_to_season()
setup_season_values()
aib.build_all_files()
print('Finished...')
