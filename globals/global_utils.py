import math
import re
import os
import glob
import ast
import configparser
import inspect
import shutil
import time

import pandas as pd
import numpy as np
from datetime import datetime
from nba_api.stats.endpoints import boxscoretraditionalv2, boxscoresummaryv2
import globals.global_settings as gls

print("Loading.... global_utils")

def american_odds_to_decimal(odds):
    if odds > 0:
        return odds / 100 + 1
    else:
        return -100 / odds + 1

def american_odds_to_implied_probability(odds):
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return -odds / (-odds + 100)

def move_dir_contents_recursively(src_dir, dst_dir):
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    for item in os.listdir(src_dir):
        src_path = os.path.join(src_dir, item)
        dst_path = os.path.join(dst_dir, item)
        shutil.move(src_path, dst_path)

def copy_dir_contents_recursively(src_dir, dst_dir):
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    for item in os.listdir(src_dir):
        src_path = os.path.join(src_dir, item)
        dst_path = os.path.join(dst_dir, item)
        if os.path.isdir(src_path):
            shutil.copytree(src_path, dst_path)
        else:
            shutil.copy2(src_path, dst_path)

def copy_folder(src_folder, dst_folder):
    folder_name = os.path.basename(src_folder)
    final_dst_folder = os.path.join(dst_folder, folder_name)
    shutil.copytree(src_folder, final_dst_folder, dirs_exist_ok=True)

def remove_old_directories(directory, age_in_seconds=86400):
    if not os.path.exists(directory):
        print(f"Directory does not exist: {directory}")
        return
    current_time = time.time()
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isdir(item_path):
            item_mtime = os.path.getmtime(item_path)
            if current_time - item_mtime > age_in_seconds:
                shutil.rmtree(item_path)
                print(f"Removed: {item_path}")

def remove_folder_and_contents(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"Removed folder and all contents: {folder_path}")
    else:
        print(f"Folder does not exist: {folder_path}")

def format_csv(df, column_spaces):
    header = ','.join([f'{col:>{column_spaces[i]}}' for i, col in enumerate(df.columns)])
    formatted_data = '\n'.join([','.join([f'{str(value):>{column_spaces[i]}}' for i, value in enumerate(row)]) for row in df.values])
    return header + '\n' + formatted_data

def inverse_transform_onehot(data, onehot_encoder, categorical_columns):
    encoded_cols = data[categorical_columns]
    original_values = onehot_encoder.inverse_transform(encoded_cols)
    return original_values

def file_contains_value(file_path, value):
    with open(file_path, 'r', encoding='utf-8') as file:
        first_line = file.readline().strip()
        if value in first_line:
            return True
    return False

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    miles = 3956 * c
    return miles

def debug_print(print_value, print_lvl=gls.PRINT_LEVEL):
    if print_lvl >= gls.PRINT_LEVEL:
        print(print_value)

def print_detail(print_value):
    frame = inspect.stack()[1]
    file_name = frame.filename
    python_file = file_name.split('\\')[-1].replace('.py', '')
    caller = frame.function
    if caller == '<module>':
        caller = 'main'
    frame = inspect.currentframe().f_back
    var_name = None
    for name, value in frame.f_locals.items():
        if isinstance(value, type(print_value)) and id(value) == id(print_value):
            var_name = name
            break
    print(f"File:({python_file}) | Method:({caller}) | VariableName:({var_name}) | Value:({print_value})")

def format_float(value):
    return f"{value:.2f}"

def read_from_config(key, val_type=0):
    config = configparser.ConfigParser()
    config.read(gls.CFG_FILE)
    print_detail(key)
    if val_type == 0:
        return int(config['DEFAULT'].get(key, '0'))
    elif val_type == 1:
        return config['DEFAULT'].get(key, '0')
    else:
        return None

def write_to_config(key, val):
    config = configparser.ConfigParser()
    config.read(gls.CFG_FILE)
    config['DEFAULT'][key] = str(val)
    with open(gls.CFG_FILE, 'w', encoding='utf-8') as configfile:
        config.write(configfile)

def team_to_integer(team):
    return gls.TEAM_TOINT_MAPPER.get(team, -1)

def position_to_integer(position):
    return gls.POSITION_TOINT_MAPPER.get(position, -1)

def basic_filter(basic_group):
    players0 = basic_group['PLAYER_ID'] == 0
    allplayers0 = players0.all()
    return allplayers0

def convert_to_list(s):
    try:
        if pd.isna(s) or s == '':
            return []
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return []

def convert_dates_to_numeric(dataframe, dt_cols):
    reference_date = datetime(1950, 1, 1)
    for col in dt_cols:
        dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')
        dataframe[col] = dataframe[col].map(lambda x: (x - reference_date).days if pd.notna(x) else np.nan)
    return dataframe

def get_col_types(file_path):
    df_sample = pd.read_csv(file_path, nrows=1)
    col_types = {}
    for col in df_sample.columns:
        if pd.api.types.is_float_dtype(df_sample[col]):
            col_types[col] = 'float32'
        elif pd.api.types.is_integer_dtype(df_sample[col]):
            col_types[col] = 'Int32'
    return col_types

def print_memory_usage(df, df_name):
    memory = df.memory_usage(deep=True).sum()
    print(f"Memory usage of {df_name}: {memory} bytes")

def split_clean_name(name):
    cleaned_parts = list(dict.fromkeys(part.strip() for part in name.split(' / ') if part.strip()))
    return cleaned_parts
def contains_pattern(text, pattern):
    return re.search(pattern, text) is not None

def save_linestofile(lines, path):
    with open(path, 'w', encoding='utf-8') as file:
        for line in lines:
                if '\n' in line:
                    file.write(line)
                else:
                    file.write(line + gls.DEFAULT_LINEBREAK)

def find_files(directory, search_string):
    pattern = os.path.join(directory, '**', f'*{search_string}*.csv')
    files = glob.glob(pattern, recursive=True)
    if files:
        return True
    return False

def remove_suffixs(text):
    pattern_jr_sr = r"\s(Jr|Sr)\."
    pattern_iii_iv_ii = r"\s(III|IV|II)"
    replaced_text = re.sub(pattern_jr_sr, r"\1", text)
    replaced_text = re.sub(pattern_iii_iv_ii, r"\1", replaced_text)
    return replaced_text

def remove_other_suffixs(text):
    pattern = r"\s(?:Jr\.?|Sr\.?|III|IV|II)$"
    replaced_text = re.sub(pattern, '', text)
    return replaced_text

def remove_shortname_periods(text):
    pattern = r'([A-Z])\.([A-Z])\.'
    return re.sub(pattern, r'\1\2', text)

def remove_bloatline_bypatterns(text, patterns):
    for pattern in patterns:
        match_found = contains_pattern(text, pattern)
        if match_found:
            return None
    return text

def height_to_inches(height_str):
    try:
        feet, inches = height_str.split('-')
        return int(feet) * 12 + int(inches)
    except:
        return None

def ensure_correct_format(date_str):
    desired_format = '%Y-%m-%d'
    try:
        datetime.strptime(date_str, desired_format)
        return date_str
    except ValueError:
        other_formats = ['%d-%m-%Y', '%m/%d/%Y', '%b %d, %Y']
        for fmt in other_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime(desired_format)
            except ValueError:
                continue
        return "Unknown date format"

def strip_after_fourth_number(filename):
    parts = filename.split('-')
    stripped_filename = '-'.join(parts[:4])
    return stripped_filename


def check_files_for_string(directory_path, search_string):
    files = os.listdir(directory_path)
    matching_files = [file for file in files if search_string in file]
    if matching_files:
        # print(f"Files containing '{search_string}':")
        # for file in matching_files:
        #     print(file)
        return True
    else:
        # print(f"No files found containing '{search_string}'.")
        return False

def get_game_details(game_id):
    boxscorea = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
    player_statsa = boxscorea.player_stats.get_data_frame()
    team_statsa = boxscorea.team_stats.get_data_frame()
    boxscore_summarya = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    game_summarya = boxscore_summarya.game_summary.get_data_frame()
    other_statsa = boxscore_summarya.other_stats.get_data_frame()
    inactive_playersa = boxscore_summarya.inactive_players.get_data_frame()
    line_scorea = boxscore_summarya.line_score.get_data_frame()
    officialsa = boxscore_summarya.officials.get_data_frame()
    gameinfosa = boxscore_summarya.game_info.get_data_frame()
    return player_statsa, team_statsa, game_summarya, other_statsa, inactive_playersa, line_scorea, officialsa, gameinfosa

def determine_opponent(row):
    if 'vs.' in row['MATCHUP'] or '@' in row['MATCHUP']:
        return row['MATCHUP'][-3:]
    else:
        return None

def fix_invalid_teams(f_path):
    with open(f_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    with open(f_path, 'w', encoding='utf-8') as file:
        for line in lines:
            if "NJN" in line:
                line = line.replace("NJN", "BKN")
            file.write(line)

def playername_log_to_detail(df_names):
    print("playername_log_to_detail...")
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace('.', '', regex=False)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace(' ', '_', regex=False)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace(r'_(III|II|IV|V|Jr|Sr)$', r'\1', regex=True)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace('Nene', 'Nene Hilario', regex=False)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace('Sun_Sun', 'Sun Yue', regex=False)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace('Matt_WilliamsJr', 'Matt Williams', regex=False)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace('Craig_PorterJr', 'Craig Porter', regex=False)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace('Jakob Poltl', 'Jakob Poeltl', regex=False)
    df_names['PLAYER_NAME'] = df_names['PLAYER_NAME'].str.replace('Brandon Boston Jr', 'Brandon Boston', regex=False)


    return df_names

def special_namecheck(names_to_check):
    if any("Dennis Schr" in name_part for name_part in names_to_check):
        names_to_check = ['Dennis Schroder']
    elif any("Ha Seung" in name_part for name_part in names_to_check):
        names_to_check = ['Ha Seung-jin']
    elif "Linton JohnsonIII" in names_to_check:
        names_to_check = ['Linton Johnson']
    elif "Mike ConleyJr" in names_to_check:
        names_to_check = ['Mike Conley']
    elif "Gerald HendersonJr" in names_to_check:
        names_to_check = ['Gerald Henderson']
    elif "Stephen ZimmermanJr" in names_to_check:
        names_to_check = ['Stephen Zimmerman']
    elif "Tony WrotenJr" in names_to_check:
        names_to_check = ['Tony Wroten']
    elif "Glen RiceJr" in names_to_check:
        names_to_check = ['Glen Rice']
    elif "Wayne SeldenJr" in names_to_check:
        names_to_check = ['Wayne Selden']
    elif "Mike DunleavyJr" in names_to_check:
        names_to_check = ['Mike Dunleavy']
    elif "Ray McCallumJr" in names_to_check:
        names_to_check = ['Ray McCallum']
    elif "DJ StewartJr" in names_to_check:
        names_to_check = ['DJ Stewart']
    elif "Gary ClarkJr" in names_to_check:
        names_to_check = ['Gary Clark']
    elif "Walker RussellJr" in names_to_check:
        names_to_check = ['Walker Russell']
    elif "BJ BostonJr" in names_to_check:
        names_to_check = ['Brandon BostonJr']
    elif "Patrick EwingJr" in names_to_check:
        names_to_check = ['Patrick Ewing']
    elif "Craig PorterJr" in names_to_check:
        names_to_check = ['Craig Porter']
    elif "DJ Augustine" in names_to_check:
        names_to_check = ['DJ Augustin']
    elif "Marcus Morris" in names_to_check:
        names_to_check = ['Marcus MorrisSr']
    elif "Reggie Bullock" in names_to_check:
        names_to_check = ['Reggie BullockJr']
    elif "Frank Mason" in names_to_check:
        names_to_check = ['Frank MasonIII']
    elif "Stanislav Medvedenko" in names_to_check:
        names_to_check = ['Slava Medvedenko']
    elif "Jermaine Samuels" in names_to_check:
        names_to_check = ['Jermaine SamuelsJr']
    elif "LucMbaha Moute" in names_to_check:
        names_to_check = ['Luc Mbah a Moute']
    elif "MettaWorld Peace" in names_to_check:
        names_to_check = ['Metta World Peace']
    elif "Jakob Poltl" in names_to_check:
        names_to_check = ['Jakob Poeltl']
    elif "Brandon BostonJr" in names_to_check:
        names_to_check = ['Brandon Boston']
    elif "Anfernee Hardaway222 Test" in names_to_check:
        print(f"testing here: {names_to_check}")
    return names_to_check
print("Loaded.... global_utils")