import os
import configparser

from globals import global_utils as mu
import globals.global_settings as gls
import globals.run_settings as rns
import pandas as pd

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

print("Loading.... ai_builder")

def remove_previous_files():
    print('remove_previous_files... Started')
    if os.path.exists(gls.ALL_FINAL):
        os.remove(gls.ALL_FINAL)
    if os.path.exists(gls.ALL_LOGS):
        os.remove(gls.ALL_LOGS)
    if os.path.exists(gls.ALL_DETAILS):
        os.remove(gls.ALL_DETAILS)
    if os.path.exists(gls.ALL_COMBINED):
        os.remove(gls.ALL_COMBINED)
    mu.write_to_config(gls.CLEAN_INT_SAVE, 0)
    print('remove_previous_files... Finished')

def create_all_details():
    print('create_all_details... Started')
    df_all_details = pd.DataFrame()
    for filename in os.listdir(gls.PLAYER_DETAIL_DIR):
        file_path = os.path.join(gls.PLAYER_DETAIL_DIR, filename)
        df_detail = pd.read_csv(file_path)
        if not df_detail.empty:
            df_all_details = pd.concat([df_all_details, df_detail])
    df_all_details.sort_values(by=['SEASON', 'TEAM'], ascending=True, inplace=True)
    df_all_details.to_csv(gls.ALL_DETAILS, index=False)
    print('create_all_details... Finished')

def create_all_logs():
    print('create_all_logs started...')
    df_all_logs = pd.DataFrame()
    if rns.current_season_only and os.path.exists(gls.ALL_PARTIALS):
        print('Loading from all partials')
        all_partials = pd.read_csv(gls.ALL_PARTIALS)
        final_season = f'{rns.prediction_season}.csv'
        file_path = os.path.join(gls.SEASON_DATA_DIR, final_season)
        df_log = pd.read_csv(file_path, dtype=mu.get_col_types(file_path))
        print(f'Loaded {final_season}')
        if not df_log.empty:
            df_all_logs = pd.concat([all_partials, df_log], ignore_index=True)
            df_all_logs.sort_values(by=['GAME_DATE', 'GAME_ID'], ascending=True, inplace=True)
            df_all_logs.to_csv(gls.ALL_LOGS, index=False)
    else:
        for file_name in os.listdir(gls.SEASON_DATA_DIR):
            if rns.prediction_season in file_name:
                df_all_logs.sort_values(by=['GAME_DATE', 'GAME_ID'], ascending=True, inplace=True)
                df_all_logs.to_csv(gls.ALL_PARTIALS, index=False)
            file_path = os.path.join(gls.SEASON_DATA_DIR, file_name)
            df_log = pd.read_csv(file_path, dtype=mu.get_col_types(file_path))
            if not df_log.empty:
                df_all_logs = pd.concat([df_all_logs, df_log], ignore_index=True)
            print(f'Loaded {file_name}')
            mu.print_memory_usage(df_all_logs, "df_all_logs")
            df_all_logs.sort_values(by=['GAME_DATE', 'GAME_ID'], ascending=True, inplace=True)
            df_all_logs.to_csv(gls.ALL_LOGS, index=False)
    print('create_all_logs completed...')

def create_games_from_logs():
    print('create_games_from_logs started...')
    all_dataframe = pd.read_csv(gls.ALL_LOGS)
    games = all_dataframe['GAME_ID'].unique()
    games_dataframe = pd.DataFrame(games, columns=['GAME_ID'])
    games_dataframe['CAPTURED'] = 0
    games_dataframe = games_dataframe.sort_values(by=['GAME_ID'], ascending=True)
    games_dataframe.to_csv(gls.ALL_GAMES, index=False)
    print('create_games_from_logs completed...')

def set_int_dates():
    print('set_int_dates started...')
    if not mu.file_contains_value(gls.ALL_FINAL, 'GAME_DATE_INT'):
        all_dataframe = pd.read_csv(gls.ALL_FINAL)
        date_columns = ['GAMEDATEINT', 'BIRTHDATEINT']
        all_dataframe['GAMEDATEINT'] = all_dataframe['GAME_DATE']
        all_dataframe['BIRTHDATEINT'] = all_dataframe['BIRTHDATE']
        all_dataframe = mu.convert_dates_to_numeric(all_dataframe, date_columns)
        all_dataframe['GAMEDATEINT'] = all_dataframe['GAMEDATEINT'].astype('Int32')
        all_dataframe['BIRTHDATEINT'] = all_dataframe['BIRTHDATEINT'].astype('Int32')
        all_dataframe.to_csv(gls.ALL_FINAL, index=False)
        print(f'Changed {date_columns} Dates to int : {gls.ALL_FINAL}')
    print('set_int_dates completed...')

def reverse_backwards_linears():
    print('reverse_backwards_linears started...')
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    all_dataframe['TEAM_ALLOUT'] = all_dataframe[['TEAM_DNP', 'TEAM_OUT']].sum(axis=1)
    all_dataframe['OPP_ALLOUT'] = all_dataframe[['OPP_DNP', 'OPP_OUT']].sum(axis=1)
    print('Sum (TEAM_ALLOUT,OPP_ALLOUT) are set...')

    all_dataframe['OPP_DEF_PREVRANK'] = all_dataframe[['OPP_PREV_PF', 'OPP_PREV_STL', 'OPP_PREV_BLK']].sum(axis=1)
    all_dataframe['OPP_DEF_RT3RANK'] = all_dataframe[['OPP_RT3_PF', 'OPP_RT3_STL', 'OPP_RT3_BLK']].sum(axis=1)
    all_dataframe['OPP_DEF_AVGRANK'] = all_dataframe[['OPP_AVG_PF', 'OPP_AVG_STL', 'OPP_AVG_BLK']].sum(axis=1)
    print('Sum (OPP_DEF_PREVRANK,OPP_DEF_RT3RANK,OPP_DEF_AVGRANK) are set...')

    altitude_back = all_dataframe['ALTITUDE'].max()
    all_dataframe['ALTITUDE_B'] = altitude_back - all_dataframe['ALTITUDE'] + 1
    losses_back = all_dataframe['LOSSES'].max()
    all_dataframe['LOSSES_B'] = losses_back - all_dataframe['LOSSES'] + 1
    distance_back = all_dataframe['DISTANCE'].max()
    all_dataframe['DISTANCE_B'] = distance_back - all_dataframe['DISTANCE'] + 1
    gamesout_back = all_dataframe['GAMES_OUT'].max()
    all_dataframe['GAMES_OUT_B'] = gamesout_back - all_dataframe['GAMES_OUT'] + 1
    oppwins_back = all_dataframe['OPP_WINS'].max()
    all_dataframe['OPP_WINS_B'] = oppwins_back - all_dataframe['OPP_WINS'] + 1
    tloss_back = all_dataframe['TLOSS'].max()
    all_dataframe['TLOSS_B'] = tloss_back - all_dataframe['TLOSS'] + 1
    owin_back = all_dataframe['OWIN'].max()
    all_dataframe['OWIN_B'] = owin_back - all_dataframe['OWIN'] + 1
    daysout_back = all_dataframe['DAYS_OUT'].max()
    print('Reverse (ALTITUDE_B,LOSSES_B,DISTANCE_B,GAMES_OUT_B,OPP_WINS_B,TLOSS_B,OWIN_B) are set...')

    all_dataframe['DAYS_OUT_B'] = daysout_back - all_dataframe['DAYS_OUT'] + 1
    backtoback_back = all_dataframe['BACKTOBACKGAME'].max()
    all_dataframe['BACKTOBACKGAME_B'] = backtoback_back - all_dataframe['BACKTOBACKGAME'] + 1
    variable_tomax = all_dataframe['WEEK_PLAYTIME'].max()
    all_dataframe['WEEK_PLAYTIME_B'] = variable_tomax - all_dataframe['WEEK_PLAYTIME'] + 1
    variable_tomax = all_dataframe['OPP_LAST_GAME_DAYS'].max()
    all_dataframe['OPP_LAST_GAME_DAYS_B'] = variable_tomax - all_dataframe['OPP_LAST_GAME_DAYS'] + 1
    print('Reverse (DAYS_OUT_B,BACKTOBACKGAME_B,WEEK_PLAYTIME_B,OPP_LAST_GAME_DAYS_B) are set...')

    all_dataframe['TEAM_CENTERS_IN'] = all_dataframe[['TEAM_CENTER', 'TEAM_CENTER-FORWARD']].sum(axis=1)
    all_dataframe['TEAM_FORWARDS_IN'] = all_dataframe[['TEAM_FORWARD', 'TEAM_FORWARD-CENTER', 'TEAM_FORWARD-GUARD']].sum(axis=1)
    all_dataframe['TEAM_GUARDS_IN'] = all_dataframe[['TEAM_GUARD', 'TEAM_GUARD-FORWARD']].sum(axis=1)
    variable_tomax = all_dataframe['TEAM_CENTERS_IN'].max()
    all_dataframe['TEAM_CENTERS_IN_B'] = variable_tomax - all_dataframe['TEAM_CENTERS_IN'] + 1
    variable_tomax = all_dataframe['TEAM_FORWARDS_IN'].max()
    all_dataframe['TEAM_FORWARDS_IN_B'] = variable_tomax - all_dataframe['TEAM_FORWARDS_IN'] + 1
    variable_tomax = all_dataframe['TEAM_GUARDS_IN'].max()
    all_dataframe['TEAM_GUARDS_IN_B'] = variable_tomax - all_dataframe['TEAM_GUARDS_IN'] + 1
    print('Reverse (TEAM_CENTERS_IN,TEAM_FORWARDS_IN,TEAM_GUARDS_IN,TEAM_CENTERS_IN_B,TEAM_FORWARDS_IN_B,TEAM_GUARDS_IN_B) are set...')

    all_dataframe['OPP_CENTERS_IN'] = all_dataframe[['OPP_CENTER', 'OPP_CENTER-FORWARD']].sum(axis=1)
    all_dataframe['OPP_FORWARDS_IN'] = all_dataframe[['OPP_FORWARD', 'OPP_FORWARD-CENTER', 'OPP_FORWARD-GUARD']].sum(axis=1)
    all_dataframe['OPP_GUARDS_IN'] = all_dataframe[['OPP_GUARD', 'OPP_GUARD-FORWARD']].sum(axis=1)
    variable_tomax = all_dataframe['OPP_CENTERS_IN'].max()
    all_dataframe['OPP_CENTERS_IN_B'] = variable_tomax - all_dataframe['OPP_CENTERS_IN'] + 1
    variable_tomax = all_dataframe['OPP_FORWARDS_IN'].max()
    all_dataframe['OPP_FORWARDS_IN_B'] = variable_tomax - all_dataframe['OPP_FORWARDS_IN'] + 1
    variable_tomax = all_dataframe['OPP_GUARDS_IN'].max()
    all_dataframe['OPP_GUARDS_IN_B'] = variable_tomax - all_dataframe['OPP_GUARDS_IN'] + 1
    print('Reverse (OPP_CENTERS_IN,OPP_FORWARDS_IN,OPP_GUARDS_IN,OPP_CENTERS_IN_B,OPP_FORWARDS_IN_B,OPP_GUARDS_IN_B) are set...')


    variable_tomax = all_dataframe['OPP_PREV_PF'].max()
    all_dataframe['OPP_PREV_PF_B'] = variable_tomax - all_dataframe['OPP_PREV_PF'] + 1
    variable_tomax = all_dataframe['OPP_RT3_PF'].max()
    all_dataframe['OPP_RT3_PF_B'] = variable_tomax - all_dataframe['OPP_RT3_PF'] + 1
    variable_tomax = all_dataframe['OPP_AVG_PF'].max()
    all_dataframe['OPP_AVG_PF_B'] = variable_tomax - all_dataframe['OPP_AVG_PF'] + 1
    variable_tomax = all_dataframe['OPP_PREV_STL'].max()
    all_dataframe['OPP_PREV_STL_B'] = variable_tomax - all_dataframe['OPP_PREV_STL'] + 1
    variable_tomax = all_dataframe['OPP_RT3_STL'].max()
    all_dataframe['OPP_RT3_STL_B'] = variable_tomax - all_dataframe['OPP_RT3_STL'] + 1
    variable_tomax = all_dataframe['OPP_AVG_STL'].max()
    all_dataframe['OPP_AVG_STL_B'] = variable_tomax - all_dataframe['OPP_AVG_STL'] + 1
    variable_tomax = all_dataframe['OPP_PREV_BLK'].max()
    all_dataframe['OPP_PREV_BLK_B'] = variable_tomax - all_dataframe['OPP_PREV_BLK'] + 1
    variable_tomax = all_dataframe['OPP_RT3_BLK'].max()
    all_dataframe['OPP_RT3_BLK_B'] = variable_tomax - all_dataframe['OPP_RT3_BLK'] + 1
    variable_tomax = all_dataframe['OPP_AVG_BLK'].max()
    all_dataframe['OPP_AVG_BLK_B'] = variable_tomax - all_dataframe['OPP_AVG_BLK'] + 1
    print('Reverse (OPP_PREV_PF,OPP_PREV_STL,OPP_PREV_BLK) are set...')

    variable_tomax = all_dataframe['OPP_DEF_PREVRANK'].max()
    all_dataframe['OPP_DEF_PREVRANK_B'] = variable_tomax - all_dataframe['OPP_DEF_PREVRANK'] + 1
    variable_tomax = all_dataframe['OPP_DEF_RT3RANK'].max()
    all_dataframe['OPP_DEF_RT3RANK_B'] = variable_tomax - all_dataframe['OPP_DEF_RT3RANK'] + 1
    variable_tomax = all_dataframe['OPP_DEF_AVGRANK'].max()
    all_dataframe['OPP_DEF_AVGRANK_B'] = variable_tomax - all_dataframe['OPP_DEF_AVGRANK'] + 1
    print('Reverse (OPP_DEF_PREVRANK_B,OPP_DEF_RT3RANK_B,OPP_DEF_AVGRANK_B) are set...')


    all_dataframe.to_csv(gls.ALL_FINAL, index=False)
    print('reverse_backwards_linears completed...')

def remove_initial_games():
    print('remove_initial_games started...')
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    games_before = all_dataframe.groupby('SEASON').nth(500)
    games_before = games_before[['SEASON', 'GAME_DATE']]
    print(games_before)
    for index, row in games_before.iterrows():
        season = row['SEASON']
        game_date = row['GAME_DATE']
        all_dataframe = all_dataframe[~((all_dataframe['SEASON'] == season) & (all_dataframe['GAME_DATE'] < game_date))]
    all_dataframe.reset_index(drop=True, inplace=True)
    all_dataframe.to_csv(gls.ALL_FINAL, index=False)
    print('remove_initial_games completed...')

def set_game_active_positions():
    print('set_game_active_positions started...')
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    grouped = all_dataframe.groupby(['GAME_ID', 'TEAM_NAME', 'POSITION']).size().reset_index(name='count')
    pivot_table = grouped.pivot_table(index=['GAME_ID', 'TEAM_NAME'], columns='POSITION', values='count',fill_value=0).reset_index()
    pivot_table.columns = ['GAME_ID', 'TEAM_NAME'] + ['POSITION_' + str(col) for col in pivot_table.columns[2:]]
    pivot_table.sort_values(by=['GAME_ID'], ascending=True)
    print(f'pivot_table size: {len(pivot_table)}')
    pivot_table = pivot_table.rename(columns=lambda x: x if x in ['GAME_ID', 'TEAM_NAME'] else 'TEAM_' + x[9:].upper())
    team_merge = all_dataframe.merge(pivot_table, how='left', on=['GAME_ID', 'TEAM_NAME'])
    pivot_table_opp = pivot_table.rename(columns=lambda x: 'OPP_' + x[5:] if 'TEAM_' in x else x)
    pivot_table_opp = pivot_table_opp.rename(columns={'TEAM_NAME': 'OPP_NAME'})
    tempdf = team_merge.merge(pivot_table_opp, how='left', on=['GAME_ID', 'OPP_NAME'], suffixes=('', '_OPP'))
    columns_to_drop = [col for col in tempdf.columns if'_OPP' in col and col.replace('_OPP', '') in pivot_table.columns]
    tempdf = tempdf.drop(columns=columns_to_drop)
    tempdf.columns = [col.replace('_OPP', '') for col in tempdf.columns]
    tempdf.sort_values(by=['GAME_DATE', 'GAME_ID'], ascending=True)
    all_dataframe = tempdf.copy()
    print(all_dataframe.head())
    all_dataframe.to_csv(gls.ALL_FINAL, index=False)
    print('set_game_active_positions completed...')

# def official_invalid_fix():
#     all_dataframe = pd.read_csv(gls.ALL_FINAL)
#     all_dataframe.sort_values(by=['PLAYER_ID', 'GAME_DATE'], inplace=True)
#     all_dataframe.loc[all_dataframe['GAME_ID'] == 22400498, ['OFFICIAL1', 'OFFICIAL2']] = [202049, 1628951]
#     all_dataframe.loc[all_dataframe['GAME_ID'] == 22400500, ['OFFICIAL1', 'OFFICIAL2']] = [202041, 203593]
#     all_dataframe.loc[all_dataframe['GAME_ID'] == 22400499, ['OFFICIAL1', 'OFFICIAL2']] = [101284, 1151]
#     all_dataframe.loc[all_dataframe['GAME_ID'] == 22400501, ['OFFICIAL1', 'OFFICIAL2']] = [1830, 202007]
#     all_dataframe.sort_values(by=['GAME_DATE', 'GAME_ID'], ascending=True, inplace=True)
#     cols_to_remove = [col for col in all_dataframe.columns if 'OFFICIAL' in col and 'LAST' in col]
#     all_dataframe.drop(columns=cols_to_remove, inplace=True)
#     all_dataframe.to_csv(gls.ALL_FINAL, index=False)

def create_official_past_totals():
    print('create_official_past_totals started...')
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    all_dataframe.sort_values(by=['PLAYER_ID', 'GAME_DATE'], inplace=True)
    nan_counts = all_dataframe.isna().sum().sum()
    print(f"Total NaN values before create_official_past_totals: {nan_counts}")
    cols = ['PTS', 'AST', 'REB', 'TO', 'PF']
    officials = ['OFFICIAL1', 'OFFICIAL2']
    grouped_data = {
        (official, col): all_dataframe.groupby(['PLAYER_ID', official])[col]
        for official in officials
        for col in cols
    }
    for (official, col), group in grouped_data.items():
        all_dataframe[f'{official}_LAST_{col}'] = group.shift(1)
        all_dataframe[f'{official}_2NDLAST_{col}'] = group.shift(2)
        all_dataframe[f'{official}_3RDLAST_{col}'] = group.shift(3)
        all_dataframe[f'{official}_4THLAST_{col}'] = group.shift(4)
        all_dataframe[f'{official}_5THLAST_{col}'] = group.shift(5)
        all_dataframe[f'{official}_6THLAST_{col}'] = group.shift(6)

    nan_counts = all_dataframe.isna().sum().sum()
    print(f"Total create_official_past_totals NaN: {nan_counts}")
    all_dataframe.fillna(-1, inplace=True)
    negative_one_counts = (all_dataframe == -1).sum().sum()
    print(f"Total create_official_past_totals set to -1: {negative_one_counts}")
    print(all_dataframe.head())
    print('Saving OFFICIAL1_LAST_PTS/OFFICIAL1_LAST_AST/OFFICIAL1_LAST_REB/OFFICIAL1_LAST_TO/OFFICIAL1_LAST_PF')
    all_dataframe.sort_values(by=['GAME_DATE', 'GAME_ID'], ascending=True, inplace=True)
    all_dataframe.to_csv(gls.ALL_FINAL, index=False)
    print('create_official_past_totals completed...')

def final_parse():
    print('final_parse started...')
    os.makedirs(gls.PRED_OUTPUT_DIR, exist_ok=True)
    os.makedirs(gls.DATAFRAME_AI_DIR, exist_ok=True)
    reverse_backwards_linears()
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    print('ALL_FINAL: set fill NA values...')
    all_dataframe['BIRTHDATEINT'] = all_dataframe['BIRTHDATEINT'].fillna(10000)
    all_dataframe['RETIRED'] = all_dataframe['RETIRED'].fillna(0)
    all_dataframe['YEARS_IN_TEAM'] = all_dataframe['YEARS_IN_TEAM'].fillna(1)
    all_dataframe['WEIGHT'] = all_dataframe['WEIGHT'].fillna(200)
    all_dataframe['HEIGHT'] = all_dataframe['HEIGHT'].fillna(75)
    all_dataframe['AGE'] = all_dataframe['AGE'].fillna(27)
    all_dataframe['WEEK_PLAYTIME'] = all_dataframe['WEEK_PLAYTIME'].fillna(0)
    all_dataframe['IS_OUT'] = all_dataframe['IS_OUT'].fillna(0)
    all_dataframe['DAYS_OUT'] = all_dataframe['DAYS_OUT'].fillna(0)
    all_dataframe['IS_STARTING'] = all_dataframe['IS_STARTING'].fillna(0)
    print('ALL_FINAL: (TEAM_) set Int32 values...')
    all_dataframe['TEAM_CENTER'] = all_dataframe['TEAM_CENTER'].astype('Int32')
    all_dataframe['TEAM_CENTER-FORWARD'] = all_dataframe['TEAM_CENTER-FORWARD'].astype('Int32')
    all_dataframe['TEAM_FORWARD'] = all_dataframe['TEAM_FORWARD'].astype('Int32')
    all_dataframe['TEAM_FORWARD-CENTER'] = all_dataframe['TEAM_FORWARD-CENTER'].astype('Int32')
    all_dataframe['TEAM_FORWARD-GUARD'] = all_dataframe['TEAM_FORWARD-GUARD'].astype('Int32')
    all_dataframe['TEAM_GUARD'] = all_dataframe['TEAM_GUARD'].astype('Int32')
    all_dataframe['TEAM_GUARD-FORWARD'] = all_dataframe['TEAM_GUARD-FORWARD'].astype('Int32')
    print('ALL_FINAL: (OPP_) set Int32 values...')
    all_dataframe['OPP_CENTER'] = all_dataframe['OPP_CENTER'].astype('Int32')
    all_dataframe['OPP_CENTER-FORWARD'] = all_dataframe['OPP_CENTER-FORWARD'].astype('Int32')
    all_dataframe['OPP_FORWARD'] = all_dataframe['OPP_FORWARD'].astype('Int32')
    all_dataframe['OPP_FORWARD-CENTER'] = all_dataframe['OPP_FORWARD-CENTER'].astype('Int32')
    all_dataframe['OPP_FORWARD-GUARD'] = all_dataframe['OPP_FORWARD-GUARD'].astype('Int32')
    all_dataframe['OPP_GUARD'] = all_dataframe['OPP_GUARD'].astype('Int32')
    all_dataframe['OPP_GUARD-FORWARD'] = all_dataframe['OPP_GUARD-FORWARD'].astype('Int32')
    print('ALL_FINAL: (PREV_) set Int32 values...')
    all_dataframe['PREV_PTS'] = all_dataframe['PREV_PTS'].astype('Int32')
    all_dataframe['PREV_AST'] = all_dataframe['PREV_AST'].astype('Int32')
    all_dataframe['PREV_REB'] = all_dataframe['PREV_REB'].astype('Int32')
    all_dataframe['PREV_STL'] = all_dataframe['PREV_STL'].astype('Int32')
    all_dataframe['PREV_BLK'] = all_dataframe['PREV_BLK'].astype('Int32')
    all_dataframe['PREV_MIN'] = all_dataframe['PREV_MIN'].astype('Int32')
    all_dataframe['PREV_FGA'] = all_dataframe['PREV_FGA'].astype('Int32')
    all_dataframe['PREV_FGM'] = all_dataframe['PREV_FGM'].astype('Int32')
    all_dataframe['PREV_FTA'] = all_dataframe['PREV_FTA'].astype('Int32')
    all_dataframe['PREV_FTM'] = all_dataframe['PREV_FTM'].astype('Int32')
    all_dataframe['PREV_FG3A'] = all_dataframe['PREV_FG3A'].astype('Int32')
    all_dataframe['PREV_FG3M'] = all_dataframe['PREV_FG3M'].astype('Int32')
    all_dataframe['PREV_PF'] = all_dataframe['PREV_PF'].astype('Int32')
    print('ALL_FINAL: (MAX_) set Int32 values...')
    all_dataframe['MAX_PTS'] = all_dataframe['MAX_PTS'].astype('Int32')
    all_dataframe['MAX_AST'] = all_dataframe['MAX_AST'].astype('Int32')
    all_dataframe['MAX_REB'] = all_dataframe['MAX_REB'].astype('Int32')
    all_dataframe['MAX_STL'] = all_dataframe['MAX_STL'].astype('Int32')
    all_dataframe['MAX_BLK'] = all_dataframe['MAX_BLK'].astype('Int32')
    all_dataframe['MAX_MIN'] = all_dataframe['MAX_MIN'].astype('Int32')
    all_dataframe['MAX_FGA'] = all_dataframe['MAX_FGA'].astype('Int32')
    all_dataframe['MAX_FGM'] = all_dataframe['MAX_FGM'].astype('Int32')
    all_dataframe['MAX_FTA'] = all_dataframe['MAX_FTA'].astype('Int32')
    all_dataframe['MAX_FTM'] = all_dataframe['MAX_FTM'].astype('Int32')
    all_dataframe['MAX_FG3A'] = all_dataframe['MAX_FG3A'].astype('Int32')
    all_dataframe['MAX_FG3M'] = all_dataframe['MAX_FG3M'].astype('Int32')
    print('ALL_FINAL: (COMMON) set Int32 values...')
    all_dataframe['BIRTHDATEINT'] = all_dataframe['BIRTHDATEINT'].astype('Int32')
    all_dataframe['RETIRED'] = all_dataframe['RETIRED'].astype('Int32')
    all_dataframe['YEARS_IN_TEAM'] = all_dataframe['YEARS_IN_TEAM'].astype('Int32')
    all_dataframe['WEIGHT'] = all_dataframe['WEIGHT'].astype('Int32')
    all_dataframe['HEIGHT'] = all_dataframe['HEIGHT'].astype('Int32')
    all_dataframe['AGE'] = all_dataframe['AGE'].astype('Int32')
    all_dataframe['WEEK_PLAYTIME'] = all_dataframe['WEEK_PLAYTIME'].astype('Int32')
    all_dataframe['IS_OUT'] = all_dataframe['IS_OUT'].astype('Int32')
    all_dataframe['DAYS_OUT'] = all_dataframe['DAYS_OUT'].astype('Int32')
    all_dataframe['IS_STARTING'] = all_dataframe['IS_STARTING'].astype('Int32')
    print('ALL_FINAL: (TEAM_INB,OPP_INB) set Int32 values...')
    all_dataframe['TEAM_CENTERS_IN'] = all_dataframe['TEAM_CENTERS_IN'].astype('Int32')
    all_dataframe['TEAM_FORWARDS_IN'] = all_dataframe['TEAM_FORWARDS_IN'].astype('Int32')
    all_dataframe['TEAM_GUARDS_IN'] = all_dataframe['TEAM_GUARDS_IN'].astype('Int32')
    all_dataframe['TEAM_CENTERS_IN_B'] = all_dataframe['TEAM_CENTERS_IN_B'].astype('Int32')
    all_dataframe['TEAM_FORWARDS_IN_B'] = all_dataframe['TEAM_FORWARDS_IN_B'].astype('Int32')
    all_dataframe['TEAM_GUARDS_IN_B'] = all_dataframe['TEAM_GUARDS_IN_B'].astype('Int32')
    all_dataframe['OPP_CENTERS_IN'] = all_dataframe['OPP_CENTERS_IN'].astype('Int32')
    all_dataframe['OPP_FORWARDS_IN'] = all_dataframe['OPP_FORWARDS_IN'].astype('Int32')
    all_dataframe['OPP_GUARDS_IN'] = all_dataframe['OPP_GUARDS_IN'].astype('Int32')
    all_dataframe['OPP_CENTERS_IN_B'] = all_dataframe['OPP_CENTERS_IN_B'].astype('Int32')
    all_dataframe['OPP_FORWARDS_IN_B'] = all_dataframe['OPP_FORWARDS_IN_B'].astype('Int32')
    all_dataframe['OPP_GUARDS_IN_B'] = all_dataframe['OPP_GUARDS_IN_B'].astype('Int32')
    print('ALL_FINAL: (OPP_PREV_PF,OPP_PREV_STL,OPP_PREV_BLK,OPP_PREV_PF_B,OPP_PREV_BLK_B) set Int32 values...')
    all_dataframe['OPP_PREV_PF'] = all_dataframe['OPP_PREV_PF'].astype('Int32')
    all_dataframe['OPP_PREV_STL'] = all_dataframe['OPP_PREV_STL'].astype('Int32')
    all_dataframe['OPP_PREV_BLK'] = all_dataframe['OPP_PREV_BLK'].astype('Int32')
    all_dataframe['OPP_PREV_PF_B'] = all_dataframe['OPP_PREV_PF_B'].astype('Int32')
    all_dataframe['OPP_PREV_STL_B'] = all_dataframe['OPP_PREV_STL_B'].astype('Int32')
    all_dataframe['OPP_PREV_BLK_B'] = all_dataframe['OPP_PREV_BLK_B'].astype('Int32')

    if 'TEAM' in all_dataframe.columns:
        print('Removing TEAM column using TEAM_NAME')
        all_dataframe = all_dataframe.drop('TEAM', axis=1)
    if all_dataframe['GAME_TIME'].astype(str).str.contains(':').any():
        print('Changing GAME_TIME replacing : convert to Int32')
        all_dataframe['GAME_TIME'] = all_dataframe['GAME_TIME'].str.replace(':', '', regex=False)
        all_dataframe['GAME_TIME'] = all_dataframe['GAME_TIME'].astype('Int32')
    all_dataframe.to_csv(gls.ALL_FINAL, index=False)

    print('Setting LAST_PTS/LAST_AST/LAST_REB')
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    all_dataframe.sort_values(by=['PLAYER_ID', 'GAME_DATE'], inplace=True)
    all_dataframe['LAST_PTS'] = all_dataframe.groupby(['PLAYER_ID', 'OPP_ID'])['PTS'].shift(1)
    all_dataframe['LAST_AST'] = all_dataframe.groupby(['PLAYER_ID', 'OPP_ID'])['AST'].shift(1)
    all_dataframe['LAST_REB'] = all_dataframe.groupby(['PLAYER_ID', 'OPP_ID'])['REB'].shift(1)
    all_dataframe.to_csv(gls.ALL_FINAL, index=False)
    print('Sorting LAST_PTS/LAST_AST/LAST_REB')
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    all_dataframe.sort_values(by=['GAME_DATE', 'GAME_ID'], ascending=True, inplace=True)
    all_dataframe.to_csv(gls.ALL_FINAL, index=False)
    print('Filling LAST_PTS/LAST_AST/LAST_REB')
    all_dataframe = pd.read_csv(gls.ALL_FINAL)
    columns_to_fill = ['LAST_PTS', 'LAST_AST', 'LAST_REB']
    all_dataframe[columns_to_fill] = all_dataframe[columns_to_fill].fillna(-1)
    all_dataframe['LAST_PTS'] = all_dataframe['LAST_PTS'].astype('Int32')
    all_dataframe['LAST_AST'] = all_dataframe['LAST_AST'].astype('Int32')
    all_dataframe['LAST_REB'] = all_dataframe['LAST_REB'].astype('Int32')
    all_dataframe.to_csv(gls.ALL_FINAL, index=False)

    print('....: All Final columns :....')
    print(all_dataframe.columns.tolist())
    print('....: All Final data :....')
    print(all_dataframe.head())
    print('final_parse completed...')

def build_all_files():
    print('build_all_files...')
    remove_previous_files()
    create_all_details()
    create_all_logs()
    if not os.path.exists(gls.ALL_GAMES):
        create_games_from_logs()
    build_combined_files()

def build_combined_files():
    print('build_combined_files...')
    logs_df = pd.read_csv(gls.ALL_LOGS)
    details_df = pd.read_csv(gls.ALL_DETAILS)
    logs_df['SEASON'] = logs_df['SEASON'].astype(int)
    logs_df['PLAYER_ID'] = logs_df['PLAYER_ID'].astype(int)
    logs_df['GAME_DATE'] = logs_df['GAME_DATE'].astype(str)
    logs_df['TEAM_NAME'] = logs_df['TEAM_NAME'].astype(str)
    details_df['PLAYER_ID'] = details_df['PLAYER_ID'].astype(int)
    details_df['SEASON'] = details_df['SEASON'].astype(int)
    details_df['TEAM'] = details_df['TEAM'].astype(str)
    details_df['AGE'] = details_df['AGE'].astype(int)
    details_df['WEIGHT'] = details_df['WEIGHT'].astype(int)
    details_df['HEIGHT'] = details_df['HEIGHT'].astype(int)

    print('Started Create Combine Logs...')
    if os.path.exists(gls.ALL_COMBINED):
        print(f'Loading ALL_COMBINED: {gls.ALL_COMBINED}')
        all_combined = pd.read_csv(gls.ALL_COMBINED, dtype=mu.get_col_types(gls.ALL_COMBINED))
    else:
        print(f'Generating ALL_COMBINED: {gls.ALL_COMBINED}')
        all_combined = pd.merge(logs_df, details_df, on=['PLAYER_ID', 'SEASON', 'TEAM_ID'], how='left',validate='many_to_many')
        all_combined['RETIRED'] = all_combined['RETIRED'].astype('Int32')
        all_combined['YEARS_IN_TEAM'] = all_combined['YEARS_IN_TEAM'].astype('Int32')
        all_combined['WEIGHT'] = all_combined['WEIGHT'].astype('Int32')
        all_combined['HEIGHT'] = all_combined['HEIGHT'].astype('Int32')
        all_combined['AGE'] = all_combined['AGE'].astype('Int32')
        all_combined.to_csv(gls.ALL_COMBINED, index=False)
    print(f'Loaded: {gls.ALL_COMBINED}')
    print(all_combined.head())

    clean_cfg_val = mu.read_from_config(gls.CLEAN_INT_SAVE)
    if clean_cfg_val == 0:
        print(f'Clean Load completed:{clean_cfg_val}')
        clean_cfg_val = 1
        mu.write_to_config(gls.CLEAN_INT_SAVE, clean_cfg_val)
        all_combined = pd.read_csv(gls.ALL_COMBINED, dtype=mu.get_col_types(gls.ALL_COMBINED))
        mu.print_memory_usage(all_combined, "All Combined")
        print('Cleaned Load removed top_10_preds')
        config = configparser.ConfigParser()
        config.read(gls.CFG_FILE)
        config['DEFAULT']['top_preds'] = str([])
        with open(gls.CFG_FILE, 'w', encoding='utf-8') as configfile:
            config.write(configfile)

    print("Final convert starting...")
    if os.path.exists(gls.ALL_FINAL):
        print(f'Loading ALL_FINAL: {gls.ALL_FINAL}')
        pd.read_csv(gls.ALL_FINAL, dtype=mu.get_col_types(gls.ALL_FINAL))
    else:
        print(f'Generating ALL_FINAL: {gls.ALL_FINAL}')
        final_df = pd.read_csv(gls.ALL_COMBINED, dtype=mu.get_col_types(gls.ALL_COMBINED))
        if 'PLAYER_NAME_y' in final_df.columns:
            print(f"final_df -> Removing: PLAYER_NAME_y")
            final_df = final_df.drop('PLAYER_NAME_y', axis=1)
        if 'PLAYER_NAME_x' in final_df.columns:
            print(f"final_df -> Converting: PLAYER_NAME_x to PLAYER_NAME")
            final_df = final_df.rename(columns={'PLAYER_NAME_x': 'PLAYER_NAME'})
        if 'POSITION_y' in final_df.columns:
            print(f"final_df -> Removing: POSITION_y")
            final_df = final_df.drop('POSITION_y', axis=1)
        if 'POSITION_x' in final_df.columns:
            print(f"final_df -> Converting: POSITION_x to POSITION")
            final_df = final_df.rename(columns={'POSITION_x': 'POSITION'})
        unique_positions = final_df['POSITION'].unique()
        print("Unique Positions:", unique_positions)
        unique_teams = final_df['TEAM_NAME'].unique()
        print("Unique Team:", unique_teams)
        unique_opps = final_df['OPP_NAME'].unique()
        print("Unique Opponent:", unique_opps)
        final_df.to_csv(gls.ALL_FINAL, index=False)
    print(f'Loaded: {gls.ALL_FINAL}')

    set_int_dates()
    remove_initial_games()
    set_game_active_positions()
    create_official_past_totals()
    final_parse()
    print('Finished...')

# create_all_details()
# build_combined_files()

print("Loaded.... ai_builder")