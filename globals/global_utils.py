import math
import re
import os
import configparser
import shutil
import time

import pandas as pd
import numpy as np
from datetime import datetime
from nba_api.stats.endpoints import boxscoretraditionalv3, boxscoresummaryv2, leaguegamelog
from nba_api.live.nba.endpoints import boxscore as live_boxscore
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

def format_float(value):
    return f"{value:.2f}"

def read_from_config(key, val_type=0):
    config = configparser.ConfigParser()
    config.read(gls.CFG_FILE)
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

def height_to_inches(height_str):
    try:
        feet, inches = height_str.split('-')
        return int(feet) * 12 + int(inches)
    except (ValueError, AttributeError):
        return None

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

def season_for_date(date_str: datetime) -> str:
    start = date_str.year if date_str.month >= 8 else date_str.year - 1
    string_season = f"{start}-{str((start + 1) % 100).zfill(2)}"
    return string_season

def get_season_data(date):
    season = season_for_date(date)
    season_df = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="T"
    ).get_data_frames()[0]
    season_df["GAME_DATE"] = pd.to_datetime(season_df["GAME_DATE"])
    season_df = season_df.sort_values(["SEASON_ID", "TEAM_ID", "GAME_DATE", "GAME_ID"], kind="mergesort")
    season_df["WIN"] = (season_df["WL"] == "W").astype(int)
    season_df["LOSS"] = (season_df["WL"] == "L").astype(int)
    season_df["WINS"] = season_df.groupby(["SEASON_ID", "TEAM_ID"])["WIN"].cumsum()
    season_df["LOSSES"] = season_df.groupby(["SEASON_ID", "TEAM_ID"])["LOSS"].cumsum()
    return season_df

def camel_to_upper_snake(df):
    df.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', c).upper() for c in df.columns]
    return df

rename_map = {
    'TEAM_TRICODE': 'TEAM_ABBREVIATION',
    'MINUTES': 'MIN',
    'FIELD_GOALS_MADE': 'FGM',
    'FIELD_GOALS_ATTEMPTED': 'FGA',
    'FIELD_GOALS_PERCENTAGE': 'FG_PCT',
    'THREE_POINTERS_MADE': 'FG3M',
    'THREE_POINTERS_ATTEMPTED': 'FG3A',
    'THREE_POINTERS_PERCENTAGE': 'FG3_PCT',
    'FREE_THROWS_MADE': 'FTM',
    'FREE_THROWS_ATTEMPTED': 'FTA',
    'FREE_THROWS_PERCENTAGE': 'FT_PCT',
    'REBOUNDS_OFFENSIVE': 'OREB',
    'REBOUNDS_DEFENSIVE': 'DREB',
    'REBOUNDS_TOTAL': 'REB',
    'ASSISTS': 'AST',
    'STEALS': 'STL',
    'BLOCKS': 'BLK',
    'TURNOVERS': 'TO',
    'FOULS_PERSONAL': 'PF',
    'POINTS': 'PTS',
    'PLUS_MINUS_POINTS': 'PLUS_MINUS',
    'PERSON_ID': 'PLAYER_ID',
}

def get_game_details(game_id):
    boxscorea = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
    player_statsa = boxscorea.player_stats.get_data_frame()
    player_statsa = camel_to_upper_snake(player_statsa)
    player_statsa.rename(columns=rename_map, inplace=True)
    player_statsa['PLAYER_NAME'] = (player_statsa['FIRST_NAME'].fillna('') + ' ' + player_statsa['FAMILY_NAME'].fillna('')).str.strip()
    player_statsa = player_statsa.rename(columns={"FIRST_NAME": "NICKNAME"})

    team_statsa = boxscorea.team_stats.get_data_frame()
    team_statsa = camel_to_upper_snake(team_statsa)

    boxscore_summarya = boxscoresummaryv2.BoxScoreSummaryV2(game_id=game_id)
    game_summarya = boxscore_summarya.game_summary.get_data_frame()
    line_scorea = boxscore_summarya.line_score.get_data_frame()

    live_data = live_boxscore.BoxScore(game_id).game.get_dict()
    live_officials = live_data.get("officials", [])
    df_officials = pd.json_normalize(live_officials)
    df_officials = camel_to_upper_snake(df_officials)
    players = [
        {**p, "teamId": t.get("teamId")}
        for t in [live_data.get("homeTeam", {}), live_data.get("awayTeam", {})]
        for p in t.get("players", [])
    ]
    df_players = pd.json_normalize(players)
    df_players = camel_to_upper_snake(df_players)
    df_inactive = df_players[df_players["STATUS"] == "INACTIVE"].reset_index(drop=True)
    df_inactive['GAME_ID'] = game_id
    df_inactive = df_inactive.rename(columns={"NAME": "PLAYER_NAME"})
    df_inactive = df_inactive.rename(columns={"PERSON_ID": "PLAYER_ID"})
    df_inactive['NICKNAME'] = df_inactive['FIRST_NAME']
    df_inactive['START_POSITION'] = ''
    df_inactive['MIN'] = ''
    df_inactive["COMMENT"] = df_inactive["NOT_PLAYING_DESCRIPTION"].apply(
        lambda x: "OUT - Inactive Player" if pd.isna(x) or str(x).strip() == "" else f"OUT - {x}"
    )
    df_inactive = df_inactive.merge(player_statsa[["TEAM_ID", "TEAM_CITY", "TEAM_ABBREVIATION"]].drop_duplicates(), on="TEAM_ID", how="left")

    player_statsa.drop(['NAME_I'], axis=1, inplace=True)
    player_statsa.drop(['TEAM_NAME'], axis=1, inplace=True)
    player_statsa.drop(['TEAM_SLUG'], axis=1, inplace=True)
    player_statsa.drop(['FAMILY_NAME'], axis=1, inplace=True)
    player_statsa.drop(['PLAYER_SLUG'], axis=1, inplace=True)
    player_statsa.drop(['JERSEY_NUM'], axis=1, inplace=True)
    player_statsa = player_statsa.rename(columns={"POSITION": "START_POSITION"})

    df_inactive.drop(['NAME_I'], axis=1, inplace=True)
    df_officials.drop(['NAME_I'], axis=1, inplace=True)
    return player_statsa, team_statsa, game_summarya, line_scorea, df_inactive, df_officials

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

print("Loaded.... global_utils")