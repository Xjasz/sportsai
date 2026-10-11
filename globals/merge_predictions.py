import os

import pandas as pd
import globals.global_settings as gls
import globals.global_utils as glu

def merge_predictions(merge_pred_file, pred_type):
    merged_df = pd.read_csv(merge_pred_file, nrows=0)
    columns = merged_df.columns
    merged_df = pd.DataFrame(columns=columns)
    merged_df['CORRECT'] = 0
    print(f'Loaded: {merge_pred_file}')
    merged_correct_sum = merged_df.groupby('PLAYER_ID')['CORRECT'].sum().to_dict()
    sorted_directory = sorted([d for d in os.listdir(gls.TOP_OUTPUT_DIR) if pred_type in d.upper() and os.path.isdir(f'{gls.TOP_OUTPUT_DIR}{d}')], reverse=True)
    print(f'Checking {len(sorted_directory)} files for Prediction Type:{pred_type}')
    merged_rows = {}
    for item in sorted_directory:
        check_dir_file = f'{gls.TOP_OUTPUT_DIR}{item}/season_file.csv'
        print(f'Importing players from file:{check_dir_file}')
        current_df = pd.read_csv(check_dir_file)
        for player_id, player_rows in current_df.groupby('PLAYER_ID'):
            correct_sum = player_rows['CORRECT'].sum()
            if player_id not in merged_correct_sum or correct_sum > merged_correct_sum[player_id]:
                print(f'Merging better prediction for {player_rows["PLAYER_NAME"].iloc[0]} (ID:{player_id}) with {correct_sum} correct predictions from file:{check_dir_file}')
                merged_correct_sum[player_id] = correct_sum
                merged_rows.pop(player_id, None)
                merged_rows[player_id] = player_rows
    merged_df = pd.concat([merged_df, *merged_rows.values()], ignore_index=True)
    merged_df = merged_df.sort_values(by=['GAME_DATE', 'GAME_ID'])
    merged_df.to_csv(merge_pred_file, index=False)
    merged_season_file = f'{gls.MERGED_PATH}{pred_type}/season_file.csv'
    directory = os.path.dirname(merged_season_file)
    if not os.path.exists(directory):
        os.makedirs(directory)
    merged_df.to_csv(merged_season_file, index=False)
    print(f'Saved: {merge_pred_file}')

def move_files_after_merge():
    glu.remove_old_directories(gls.MERGED_PREV_PATH, 12000)
    glu.move_dir_contents_recursively(gls.TOP_OUTPUT_DIR, gls.MERGED_PREV_PATH)
    pts_merged_folder = f'{gls.MERGED_PATH}PTS'
    ast_merged_folder = f'{gls.MERGED_PATH}AST'
    reb_merged_folder = f'{gls.MERGED_PATH}REB'
    glu.copy_folder(pts_merged_folder, gls.TOP_OUTPUT_DIR)
    glu.copy_folder(ast_merged_folder, gls.TOP_OUTPUT_DIR)
    glu.copy_folder(reb_merged_folder, gls.TOP_OUTPUT_DIR)
    glu.remove_folder_and_contents(pts_merged_folder)
    glu.remove_folder_and_contents(ast_merged_folder)
    glu.remove_folder_and_contents(reb_merged_folder)

def merge_all():
    merge_predictions(gls.MERGED_PTS_PRED_FILE, 'PTS')
    merge_predictions(gls.MERGED_REB_PRED_FILE, 'REB')
    merge_predictions(gls.MERGED_AST_PRED_FILE, 'AST')
    move_files_after_merge()

if __name__ == "__main__":
    print('Starting...')
    merge_all()
    print('Finished...')