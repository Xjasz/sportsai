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

#########################################################################################################################################
####  CREATE/UPDATE GAME LOGS INTO SEASONS IN DIRECTORY -> '/DATA/SEASON/'
# Generates remaining gamelog data for seasons.
# SET 'run_from_start_to_finish' to 'TRUE' to overwrite season gamelogs.
# SET 'current_season_only' to 'TRUE' to overwrite current season gamelogs.
#########################################################################################################################################
OUT_KEYWORDS_SET = {'INJ', 'NOT WITH TEAM', 'MWT', 'DNT', 'DID NOT TRAVEL', 'NWT', 'SUSPENSION', 'PERSONAL', 'INACTIVE', 'DND', 'DID NOT DRESS', 'DNP_TRADE'}

def comment_keyword(comment):
    if pd.isna(comment) or len(comment) == 0:
        return ''
    keyword = str(comment.split('-')[0].strip()).upper()
    return 'OUT' if any(status_keyword in keyword for status_keyword in OUT_KEYWORDS_SET) else keyword

def season_dirs(prediction_only=False):
    for item in os.listdir(gls.GAMES_DATA_DIR):
        if (prediction_only or rns.current_season_only) and rns.prediction_season not in item:
            continue
        print(f'Checking Season -> {item}')
        yield item, f'{gls.GAMES_DATA_DIR}{item}/'

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
        bench_opp_obj = pos_df[pos_df['TEAM_NAME'] == ioppteam]
        for pos in positions:
            opp_obj = bench_opp_obj[bench_opp_obj['POSITION'] == pos][array_vals]
            if not opp_obj.empty: break

        if opp_obj.empty:
            opp_obj = bench_opp_obj[array_vals]
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
        gl_df.sort_values(by=['SEASON', 'PLAYER_ID', 'GAME_DATE'], ascending=True, inplace=True)
        for col in roll_columns:
            gl_df[f'RT3H_{col}'] = rolling_home_or_away_consecutive(gl_df, col, 1).round(2)
            gl_df[f'RT3H_{col}'] = gl_df[f'RT3H_{col}'].round(2).fillna(0)
            gl_df[f'RT3A_{col}'] = rolling_home_or_away_consecutive(gl_df, col, 0).round(2)
            gl_df[f'RT3A_{col}'] = gl_df[f'RT3A_{col}'].round(2).fillna(0)
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
        gl_df['WEEK_PLAYTIME'] = gl_df['WEEK_PLAYTIME'].fillna(0)
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

OPP_DEF_FIELDS = [
    ('LAST_GAME_DAYS', 'OPP_LAST_GAME_DAYS', 0), ('BACKTOBACKGAME', 'OPP_BACKTOBACKGAME', 0), ('WEEK_PLAYTIME', 'OPP_WEEK_PLAYTIME', 0),
    ('PREV_PF', 'OPP_PREV_PF', 0), ('PREV_STL', 'OPP_PREV_STL', 0), ('PREV_BLK', 'OPP_PREV_BLK', 0),
    ('RT3_PF', 'OPP_RT3_PF', 0.0), ('AVG_PF', 'OPP_AVG_PF', 0.0), ('RT3_STL', 'OPP_RT3_STL', 0.0),
    ('AVG_STL', 'OPP_AVG_STL', 0.0), ('RT3_BLK', 'OPP_RT3_BLK', 0.0), ('AVG_BLK', 'OPP_AVG_BLK', 0.0),
]

def set_opponent_def(df_log,file_path):
    print('set_opponent_def started...')
    if not 'OPP_LAST_GAME_DAYS' in df_log.columns:
        start_time = time.time()
        key_cols = ['PLAYER_ID', 'TEAM_NAME', 'GAME_DATE']
        opp_lookup = df_log[key_cols + [src for src, _, _ in OPP_DEF_FIELDS]].drop_duplicates(subset=key_cols, keep='last')
        opp_lookup = opp_lookup.rename(columns={'PLAYER_ID': 'OPP_PLAYER_ID', 'TEAM_NAME': 'OPP_NAME',
                                                 **{src: dest for src, dest, _ in OPP_DEF_FIELDS}})
        df_log = df_log.merge(opp_lookup, on=['OPP_PLAYER_ID', 'OPP_NAME', 'GAME_DATE'], how='left', indicator=True)
        unmatched = df_log[df_log['_merge'] == 'left_only']
        if not unmatched.empty:
            row = unmatched.iloc[0]
            raise ValueError(f"No player found for GAME_ID:{row['GAME_ID']} --> OPP_PLAYER_ID:{row['OPP_PLAYER_ID']}"
                              f"-TEAM:{row['OPP_NAME']}  PLAYER_ID:{row['PLAYER_ID']}")
        df_log = df_log.drop(columns='_merge')
        for _, dest, default in OPP_DEF_FIELDS:
            filled = df_log[dest].where(df_log[dest].notna() & (df_log[dest] != 0), default)
            df_log[dest] = pd.to_numeric(filled, downcast='integer') if isinstance(default, int) else filled.astype('float64')
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
                df_log[column_name] = df_log[column_name].fillna(0)
            avg_column_name = f'OPP_{stat}AVG'
            df_log[avg_column_name] = df_log.groupby(['OPP_PLAYER_ID'])[f'{stat}_PREV'].transform(lambda x: x.shift().expanding(min_periods=1).mean().round(2))
            df_log[avg_column_name] = df_log[avg_column_name].fillna(0)
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
            int32_columns = ['WINS', 'LOSSES', 'OFFICIAL1', 'OFFICIAL2', 'MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA',
                              'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'OPP_WINS', 'OPP_LOSSES',
                              'PLUS_MINUS', 'TEAM_DNP', 'TEAM_OUT', 'OPP_DNP', 'OPP_OUT', 'DISTANCE', 'OPP_DISTANCE',
                              'TWIN', 'TLOSS', 'OWIN', 'OLOSS', 'GAMES_IN', 'GAMES_OUT', 'GAMES_CONT', 'GAMES_START',
                              'GAMES_BENCH', 'TEAM_OUT_START', 'TEAM_OUT_BENCH', 'OPP_OUT_START', 'OPP_OUT_BENCH',
                              'DEF_PTS', 'DEF_AST', 'DEF_REB', 'OPP_PLAYER_ID']
            for col in int32_columns:
                df_all_logs[col] = df_all_logs[col].astype('Int32')
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
    for item, directory in season_dirs():
        PLAYER_TRACKER = {}
        for file_name in sorted(os.listdir(directory)):
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
                    keyword = comment_keyword(comment)
                    player_data = PLAYER_TRACKER.get(player_id, [0, 0, 0, 0, 0])
                    if keyword == '':
                        player_data[0] = player_data[0] + 1
                        player_data[2] = player_data[2] + 1
                        if start_position in ['G','C','F']:
                            player_data[3] = player_data[3] + 1
                        elif start_position == 'B':
                            player_data[4] = player_data[4] + 1
                        else:
                            raise ValueError(f'Error Unknown position... player_id: {player_id} position: {start_position}')
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
    for item, directory in season_dirs():
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'DISTANCE'):
                df_log = pd.read_csv(file_path)
                home_team_abbrev = df_log[df_log['IS_HOME'] == 1]['TEAM_NAME'].iloc[0]
                away_team_abbrev = df_log[df_log['IS_HOME'] == 0]['TEAM_NAME'].iloc[0]
                specific_distance = distance_df[(distance_df['HOME'] == home_team_abbrev) & (distance_df['AWAY'] == away_team_abbrev)]
                if len(specific_distance) < 1:
                    raise ValueError(f'Error distance not found for: {home_team_abbrev} and {away_team_abbrev}...')
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
    for item, directory in season_dirs():
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
    df_all_details = glu.load_player_details()
    print('Loaded player details..')
    for item, directory in season_dirs():
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
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
                missing_position_rows = df_log[df_log['POSITION'].isna() | (df_log['POSITION'].str.strip() == '')]
                if not missing_position_rows.empty:
                    missing_position_row_comments = missing_position_rows[missing_position_rows['COMMENT'].str.len() < 1]
                    if not missing_position_row_comments.empty:
                        game_id = missing_position_row_comments['GAME_ID'].iloc[0]
                        for _, row in missing_position_row_comments.iterrows():
                            print(f"Missing PLAYER_ID: {row['PLAYER_ID']}")
                        raise ValueError(f"Error Missing POSITIONS in GAME_ID: {game_id}")
                df_log.to_csv(file_path, index=False)
    print('set_positions_and_cleanup completed...')

def set_player_opponents():
    print('set_player_opponents started...')
    for item, directory in season_dirs():
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if not glu.file_contains_value(file_path, 'MATCHED_OPPONENT'):
                df_log = pd.read_csv(file_path)
                df_log['MATCHED_OPPONENT'] = 0
                ###### Match Centers
                c_indices = df_log[df_log['START_POSITION'] == 'C'].index[:2]
                count_C_positions = (df_log['START_POSITION'].astype(str).str.strip().str.upper() == 'C').sum()
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
    print('set_player_opponents completed...')

def set_out_totals():
    print('set_out_totals started...')
    for item, directory in season_dirs():
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
    for item, directory in season_dirs():
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
    for item, directory in season_dirs():
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
                        keyword = comment_keyword(comment)
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
                all_keywords = {'DNP': 0, 'OUT': 0}
                t1_keywords = {'TEAM': '', 'DNP': 0, 'OUT': 0}
                t2_keywords = {'TEAM': '', 'DNP': 0, 'OUT': 0}
    print('set_invalid_players completed...')

def fix_prediction_values():
    print('fix_prediction_values...')
    for item, directory in season_dirs(prediction_only=True):
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            if gnd.is_prediction_file(file_path):
                df_log = pd.read_csv(file_path)
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
    for item, directory in season_dirs():
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

if __name__ == '__main__':
    print('Starting...')
    if rns.run_from_start_to_finish:
        glu.remove_folder_and_contents(gls.TOP_OUTPUT_DIR)
        cgl.create_game_logs_start()
    gnd.generate_predictions_start()
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
