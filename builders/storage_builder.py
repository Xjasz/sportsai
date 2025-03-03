import os
import numpy as np
import pandas as pd
from globals import global_utils as mnu
import globals.global_settings as gls
import globals.run_settings as rns

mnu.debug_print("Loading.... storage_builder")

def generate_files(linked_df, eval_loss, target_col):
    mnu.debug_print("Running generate_files....",0)
    dfalter = linked_df.copy()
    percentage_diff = abs(dfalter[target_col] - dfalter[f'{target_col}_PRED']) / (dfalter[target_col] + 0.0001)
    absolute_diff = np.abs(dfalter[target_col] - dfalter[f'{target_col}_PRED'])
    dfalter['Prediction_Accuracy'] = np.where((percentage_diff <= 0.1) | ((dfalter[target_col] < 11) & (absolute_diff < 1)), 1, 0)
    dfalter = dfalter.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
    dfalter['RT_P'] = dfalter.groupby(['PLAYER_ID', 'SEASON'])['Prediction_Accuracy'].cumsum()
    dfalter['Game_Count'] = dfalter.groupby(['PLAYER_ID', 'SEASON']).cumcount() + 1
    dfalter['RT_PCT'] = (dfalter['RT_P'] / dfalter['Game_Count']).round(2)
    dfalter.to_csv(gls.dfaltersfile, index=False)
    mnu.debug_print("Creating dfalter ended...",0)
    df_rt3_alter = dfalter.copy()
    percentage_diff = abs(df_rt3_alter[target_col] - df_rt3_alter[f'{target_col}_PRED']) / (df_rt3_alter[target_col] + 0.0001)
    absolute_diff = np.abs(df_rt3_alter[target_col] - df_rt3_alter[f'{target_col}_PRED'])
    df_rt3_alter['RT3_Prediction_Accuracy'] = np.where((percentage_diff <= 0.1) | ((df_rt3_alter[target_col] < 11) & (absolute_diff < 1)), 1, 0)
    df_rt3_alter = df_rt3_alter.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
    df_rt3_alter['RT3G_Accuracy_Sum'] = df_rt3_alter.groupby(['PLAYER_ID', 'SEASON'])['RT3_Prediction_Accuracy'].rolling(window=3, min_periods=1).sum().reset_index(level=[0, 1], drop=True)
    df_rt3_alter['RT3G_Accuracy_Percentage'] = (df_rt3_alter['RT3G_Accuracy_Sum'] / 3).round(2)
    df_rt3_alter.to_csv(gls.df_rt3_alterfile, index=False)
    mnu.debug_print("Creating df_rt3_alterfile ended...",0)
    df_rt5_alter = df_rt3_alter.copy()
    percentage_diff = abs(df_rt5_alter[target_col] - df_rt5_alter[f'{target_col}_PRED']) / (df_rt5_alter[target_col] + 0.0001)
    absolute_diff = np.abs(df_rt5_alter[target_col] - df_rt5_alter[f'{target_col}_PRED'])
    df_rt5_alter['RT5_Prediction_Accuracy'] = np.where((percentage_diff <= 0.1) | ((df_rt5_alter[target_col] < 11) & (absolute_diff < 1)), 1, 0)
    df_rt5_alter = df_rt5_alter.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
    df_rt5_alter['RT5G_Accuracy_Sum'] = df_rt5_alter.groupby(['PLAYER_ID', 'SEASON'])['RT5_Prediction_Accuracy'].rolling(window=5, min_periods=1).sum().reset_index(level=[0, 1], drop=True)
    df_rt5_alter['RT5G_Accuracy_Percentage'] = (df_rt5_alter['RT5G_Accuracy_Sum'] / 5).round(2)
    df_rt5_alter.to_csv(gls.df_rt5_alterfile, index=False)
    mnu.debug_print("Creating df_rt5_alterfile ended...",0)
    df_rt9_alter = df_rt5_alter.copy()
    percentage_diff = abs(df_rt9_alter[target_col] - df_rt9_alter[f'{target_col}_PRED']) / (df_rt9_alter[target_col] + 0.0001)
    absolute_diff = np.abs(df_rt9_alter[target_col] - df_rt9_alter[f'{target_col}_PRED'])
    df_rt9_alter['RT9_Prediction_Accuracy'] = np.where((percentage_diff <= 0.1) | ((df_rt9_alter[target_col] < 11) & (absolute_diff < 1)), 1, 0)
    df_rt9_alter = df_rt9_alter.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
    df_rt9_alter['RT9G_Accuracy_Sum'] = df_rt9_alter.groupby(['PLAYER_ID', 'SEASON'])['RT9_Prediction_Accuracy'].rolling(window=9, min_periods=1).sum().reset_index(level=[0, 1], drop=True)
    df_rt9_alter['RT9G_Accuracy_Percentage'] = (df_rt9_alter['RT9G_Accuracy_Sum'] / 9).round(2)
    df_rt9_alter.to_csv(gls.df_rt9_alterfile, index=False)
    mnu.debug_print("Creating df_rt9_alterfile ended...",0)
    df = df_rt9_alter
    df['DIFF_AVG'] = abs(df[f'AVG_{target_col}'] - df[f'{target_col}_PRED'])
    df['DIFF_ACT'] = abs(df[target_col] - df[f'{target_col}_PRED'])
    percentage_diff = abs(df[target_col] - df[f'{target_col}_PRED']) / (df[target_col] + 0.0001)
    absolute_diff = np.abs(df[target_col] - df[f'{target_col}_PRED'])
    df['CORRECT'] = np.where((percentage_diff <= 0.1) | ((df[target_col] < 11) & (absolute_diff < 1)), 1, 0)
    dfs_sorted = df.sort_values(by=['DIFF_AVG', 'DIFF_ACT'], ascending=[False, True])
    if 'INDEX' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['INDEX'], axis=1)
    if 'Game_Count' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['Game_Count'], axis=1)
    if 'RT_P' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['RT_P'], axis=1)
    if 'Prediction_Accuracy' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['Prediction_Accuracy'], axis=1)
    if 'RT3_Prediction_Accuracy' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['RT3_Prediction_Accuracy'], axis=1)
    if 'RT5_Prediction_Accuracy' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['RT5_Prediction_Accuracy'], axis=1)
    if 'RT9_Prediction_Accuracy' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['RT9_Prediction_Accuracy'], axis=1)
    if 'RT3G_Accuracy_Sum' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['RT3G_Accuracy_Sum'], axis=1)
    if 'RT5G_Accuracy_Sum' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['RT5G_Accuracy_Sum'], axis=1)
    if 'RT9G_Accuracy_Sum' in dfs_sorted.columns:
        dfs_sorted = dfs_sorted.drop(['RT9G_Accuracy_Sum'], axis=1)
    if 'RT3G_Accuracy_Percentage'in dfs_sorted.columns:
        dfs_sorted.rename(columns={'RT3G_Accuracy_Percentage': 'RT3G_PCT'}, inplace=True)
    if 'RT5G_Accuracy_Percentage'in dfs_sorted.columns:
        dfs_sorted.rename(columns={'RT5G_Accuracy_Percentage': 'RT5G_PCT'}, inplace=True)
    if 'RT9G_Accuracy_Percentage'in dfs_sorted.columns:
        dfs_sorted.rename(columns={'RT9G_Accuracy_Percentage': 'RT9G_PCT'}, inplace=True)

    dfs_sorted = dfs_sorted.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
    dfs_sorted['OVER'] = (dfs_sorted[f'{target_col}_PRED'] > 1.1 * dfs_sorted[target_col]).astype(int)
    dfs_sorted['UNDER'] = (dfs_sorted[f'{target_col}_PRED'] < 0.9 * dfs_sorted[target_col]).astype(int)
    dfs_sorted['C_OVER'] = dfs_sorted.groupby(['PLAYER_ID', 'SEASON'])['OVER'].cumsum()
    dfs_sorted['C_UNDER'] = dfs_sorted.groupby(['PLAYER_ID', 'SEASON'])['UNDER'].cumsum()
    dfs_sorted['ROW_NUM'] = dfs_sorted.groupby(['PLAYER_ID', 'SEASON']).cumcount() + 1
    dfs_sorted['AVG_OVER'] = dfs_sorted['C_OVER'] / dfs_sorted['ROW_NUM']
    dfs_sorted['AVG_UNDER'] = dfs_sorted['C_UNDER'] / dfs_sorted['ROW_NUM']
    dfs_sorted = dfs_sorted.drop(columns=['ROW_NUM','C_OVER','C_UNDER'])
    dfs_sorted.to_csv(gls.dfothers_file, index=False)
    mnu.debug_print("Creating dfothers_file ended...",0)
    float_cols = [f'AVG_{target_col}', target_col, f'{target_col}_PRED', 'DIFF_AVG', 'DIFF_ACT','AVG_OVER','AVG_UNDER']
    for col in float_cols:
        if col in dfs_sorted.columns:
            dfs_sorted[col] = dfs_sorted[col].apply(mnu.format_float)
    dfs_sorted.to_csv(gls.dfs_sortedfile, index=False)
    mnu.debug_print(f"Creating dfs_sorted ended... size {len(dfs_sorted)}",0)
    column_spaces = [4, 10, 15, 15, 25]
    while len(column_spaces) != dfs_sorted.shape[1]:
        column_spaces.append(10)
    formatted_string = mnu.format_csv(dfs_sorted, column_spaces)
    with open(gls.dfs_sorted_cleanedfile, 'w', encoding='utf-8') as file:
        file.write(formatted_string)
    mnu.debug_print("Creating dfs_sorted ended...",0)
    fin_df = pd.read_csv(gls.dfs_sortedfile)
    fin_df = fin_df.sort_values(by=['GAME_DATE', 'GAME_ID'])

    mnu.debug_print("moving officials to end...",0)
    official_columns = [col for col in fin_df.columns if col.startswith("OFFICIAL1") or col.startswith("OFFICIAL2")]
    other_columns = [col for col in fin_df.columns if col not in official_columns]
    fin_df = fin_df[other_columns + official_columns]

    fin_df.to_csv(gls.fin_datafile, index=False)
    mnu.debug_print("Creating fin_data ended...",0)
    fin_df_parse = pd.read_csv(gls.fin_datafile)
    rtz_tgt_col = fin_df_parse.pop(f'RTZ_{target_col}')
    fin_df_parse.insert(6, f'RTZ_{target_col}', rtz_tgt_col)
    rt3_tgt_col = fin_df_parse.pop(f'RT3_{target_col}')
    fin_df_parse.insert(7, f'RT3_{target_col}', rt3_tgt_col)
    rt5_tgt_col = fin_df_parse.pop(f'RT5_{target_col}')
    fin_df_parse.insert(8, f'RT5_{target_col}', rt5_tgt_col)
    rt9_tgt_col = fin_df_parse.pop(f'RT9_{target_col}')
    fin_df_parse.insert(9, f'RT9_{target_col}', rt9_tgt_col)
    fin_df_parse = fin_df_parse.sort_values(by=['GAME_DATE', 'GAME_ID'])

    fin_df_parse.to_csv(gls.fin_data_parsefile, index=False)
    fin_df_parse['SEASON'] = fin_df_parse['SEASON'].astype(str).str.strip()
    mnu.debug_print("Creating fin_df_parse ended...",0)
    df_current_season = fin_df_parse[fin_df_parse['SEASON'] == rns.prediction_season]
    df_current_season.to_csv(gls.CURRENT_SEASON_PRED_FULLPATH, index=False)
    df_current_season.to_csv(gls.SYNC_SEASON_FILE, index=False)
    mnu.debug_print('Creating s_season ended...',0)
    os.makedirs(gls.PRED_OUTPUT_DIR, exist_ok=True)
    fin_df_parse = pd.read_csv(gls.fin_data_parsefile)
    fin_df_parse_group = fin_df_parse[fin_df_parse['GAME_DATE'] == rns.prediction_date]
    sorted_group = fin_df_parse_group.sort_values(by=['GAME_DATE', 'GAME_ID','RT_PCT'], ascending=[True,True,False])
    sorted_group.to_csv(gls.CURRENT_PREDICTION_FULLPATH, index=False)
    mnu.debug_print('Creating sorted_group ended...',0)
    backup_prediction = rns.prediction_date
    file_index = 2
    while mnu.check_files_for_string(gls.PREVIOUS_PRED_OUTPUT_DIR, backup_prediction):
        backup_prediction = f"{rns.prediction_date}-{file_index}"
        file_index += 1
    if eval_loss != '0.000':
        backup_prediction += f'-{eval_loss}{gls.DEFAULT_CSV_TYPENAME}'
    else:
        backup_prediction += f'{gls.DEFAULT_CSV_TYPENAME}'
    backup_prediction = gls.PREVIOUS_PRED_OUTPUT_DIR + backup_prediction
    df_current_season.to_csv(backup_prediction, index=False)
    mnu.debug_print('Creating backup_prediction ended...',0)
    return df_current_season

mnu.debug_print("Loaded.... storage_builder")