import os
import numpy as np
import pandas as pd
from globals import global_utils as mnu
import globals.global_settings as gls
import globals.run_settings as rns

mnu.debug_print("Loading.... storage_builder")

def accuracy_flag(df, target_col):
    percentage_diff = abs(df[target_col] - df[f'{target_col}_PRED']) / (df[target_col] + 0.0001)
    absolute_diff = np.abs(df[target_col] - df[f'{target_col}_PRED'])
    return np.where((percentage_diff <= 0.1) | ((df[target_col] < 11) & (absolute_diff < 1)), 1, 0)

def generate_files(linked_df, eval_loss, target_col):
    mnu.debug_print("Running generate_files....",0)
    for directory in (gls.PRED_OUTPUT_DIR, gls.PREVIOUS_PRED_OUTPUT_DIR, os.path.dirname(gls.SYNC_SEASON_FILE)):
        os.makedirs(directory, exist_ok=True)
    dfalter = linked_df.copy()
    dfalter['Prediction_Accuracy'] = accuracy_flag(dfalter, target_col)
    dfalter = dfalter.sort_values(by=['PLAYER_ID', 'GAME_DATE'])
    dfalter['RT_P'] = dfalter.groupby(['PLAYER_ID', 'SEASON'])['Prediction_Accuracy'].cumsum()
    dfalter['Game_Count'] = dfalter.groupby(['PLAYER_ID', 'SEASON']).cumcount() + 1
    dfalter['RT_PCT'] = (dfalter['RT_P'] / dfalter['Game_Count']).round(2)
    dfalter.to_csv(gls.dfaltersfile, index=False)
    mnu.debug_print("Creating dfalter ended...",0)
    df = dfalter
    for n, path in ((3, gls.df_rt3_alterfile), (5, gls.df_rt5_alterfile), (9, gls.df_rt9_alterfile)):
        df[f'RT{n}_Prediction_Accuracy'] = df['Prediction_Accuracy']
        df[f'RT{n}G_Accuracy_Sum'] = df.groupby(['PLAYER_ID', 'SEASON'])[f'RT{n}_Prediction_Accuracy'].rolling(window=n, min_periods=1).sum().reset_index(level=[0, 1], drop=True)
        df[f'RT{n}G_Accuracy_Percentage'] = (df[f'RT{n}G_Accuracy_Sum'] / n).round(2)
        df.to_csv(path, index=False)
        mnu.debug_print(f"Creating df_rt{n}_alterfile ended...",0)
    df['DIFF_AVG'] = abs(df[f'AVG_{target_col}'] - df[f'{target_col}_PRED'])
    df['DIFF_ACT'] = abs(df[target_col] - df[f'{target_col}_PRED'])
    df['CORRECT'] = df['Prediction_Accuracy']
    dfs_sorted = df.sort_values(by=['DIFF_AVG', 'DIFF_ACT'], ascending=[False, True])
    drop_cols = ['INDEX', 'Game_Count', 'RT_P', 'Prediction_Accuracy', 'RT3_Prediction_Accuracy',
                 'RT5_Prediction_Accuracy', 'RT9_Prediction_Accuracy', 'RT3G_Accuracy_Sum',
                 'RT5G_Accuracy_Sum', 'RT9G_Accuracy_Sum']
    dfs_sorted = dfs_sorted.drop(columns=drop_cols, errors='ignore')
    dfs_sorted = dfs_sorted.rename(columns={'RT3G_Accuracy_Percentage': 'RT3G_PCT', 'RT5G_Accuracy_Percentage': 'RT5G_PCT',
                                             'RT9G_Accuracy_Percentage': 'RT9G_PCT'})

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
    column_spaces = [4, 10, 15, 15, 25] + [10] * (dfs_sorted.shape[1] - 5)
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
    for pos, prefix in enumerate(('RTZ', 'RT3', 'RT5', 'RT9'), 6):
        fin_df_parse.insert(pos, f'{prefix}_{target_col}', fin_df_parse.pop(f'{prefix}_{target_col}'))
    fin_df_parse = fin_df_parse.sort_values(by=['GAME_DATE', 'GAME_ID'])

    fin_df_parse.to_csv(gls.fin_data_parsefile, index=False)
    fin_df_parse['SEASON'] = fin_df_parse['SEASON'].astype(str).str.strip()
    mnu.debug_print("Creating fin_df_parse ended...",0)
    df_current_season = fin_df_parse[fin_df_parse['SEASON'] == rns.prediction_season]
    df_current_season.to_csv(gls.CURRENT_SEASON_PRED_FULLPATH, index=False)
    df_current_season.to_csv(gls.SYNC_SEASON_FILE, index=False)
    mnu.debug_print('Creating s_season ended...',0)
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