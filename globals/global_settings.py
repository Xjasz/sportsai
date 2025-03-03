import os

#ENV VARIABLES
BROWSER_PROFILE_DIR = os.getenv('BROWSER_PROFILE_DIR', '')
BROWSER_EXE_LOC = os.getenv('BROWSER_EXE_LOC', '')
GECKO_EXE_LOC = os.getenv('GECKO_EXE_LOC', '')
SPORTSAI_DBSERVER = os.getenv('SPORTSAI_DBSERVER', '')
SPORTSAI_DBNAME = os.getenv('SPORTSAI_DBNAME', '')
SPORTSAI_DBUSER = os.getenv('SPORTSAI_DBUSER', '')
SPORTSAI_DBPASS = os.getenv('SPORTSAI_DBPASS', '')
SETTINGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SETTINGS_DIR)

#GLOBAL VARIABLES
PRINT_LEVEL = 0
CLEAN_INT_SAVE = 'cleanintsave'
NBAAPI_GAMEID_INIT = '00'
NBAAPI_LEAGUE_ID = '00'
AQUIRE_COL_NAME = 'AQUIRE'
RELINQUISH_COL_NAME = 'RELINQUISH'
PLAYERNAME_COL_NAME = 'PLAYER_NAME'
UNKNOWN_TEAM_NAME = 'UKN'
BROWSER_TYPE = 'FIREFOX'

#MAPPERS
TEAM_TOSHORT_MAPPER = {"Hawks": "ATL","Celtics": "BOS","Nets": "BKN","Hornets": "CHA","Bobcats": "CHA","Bulls": "CHI", "Cavaliers": "CLE","Mavericks": "DAL","Nuggets": "DEN","Pistons": "DET","Warriors": "GSW","Rockets": "HOU","Pacers": "IND","Clippers": "LAC","Lakers": "LAL","Grizzlies": "MEM","Heat": "MIA","Bucks": "MIL","Timberwolves": "MIN","Pelicans": "NOP","Knicks": "NYK","Thunder": "OKC","Sonics": "OKC","Magic": "ORL","76ers": "PHI","Suns": "PHX","Trail Blazers": "POR","Blazers": "POR","Kings": "SAC","Spurs": "SAS","Raptors": "TOR","Jazz": "UTA","Wizards": "WAS"}
POSITION_TOINT_MAPPER = {'Guard': 0,'Guard-Forward': 1,'Forward-Guard': 2,'Forward': 3,'Forward-Center': 4,'Center-Forward': 5,'Center': 6}
TEAM_TOINT_MAPPER = {'ATL': 0, 'BKN': 1, 'BOS': 2, 'CHA': 3, 'CHI': 4, 'CLE': 5, 'DAL': 6, 'DEN': 7, 'DET': 8, 'GSW': 9,'HOU': 10, 'IND': 11, 'LAC': 12, 'LAL': 13, 'MEM': 14, 'MIA': 15, 'MIL': 16, 'MIN': 17, 'NOH': 18,'NOK': 19, 'NOP': 20, 'NYK': 21, 'OKC': 22, 'ORL': 23, 'PHI': 24, 'PHX': 25, 'POR': 26, 'SAC': 27,'SAS': 28, 'SEA': 29, 'TOR': 30, 'UTA': 31, 'WAS': 32}
POSITION_TOSIZE_MAPPER = {'Guard': {'avg_height': "6-3", 'avg_weight': 185},'Forward': {'avg_height': "6-8", 'avg_weight': 220},'Center':  {'avg_height': "6-11", 'avg_weight': 240}}
BLOAT_TODTYPE_MAPPER = {'EVENT_TYPE': str, 'PLAYER_ID': str}

#GLOBAL ARRAYS
BLOAT_KEYWORDS = ['fired', 'hired', 'owner', 'promote', 'coach \(date approximate\)', 're-signed as', 'resign', 'reassign', 're-assigned','administrative leave', 'leave of abs', 'retired as', 'replaced as', 'coach fined', 'head coach suf', 'suspended as','re-sign as head', 'coach had surgery', 'replaced as', 'fined as', 'on head coach', 'director of player programs', 'demoted', 'coach missed','coach underwent', 'coach diagnosed', 'returned as', 'as head coach', 'retained as', 'as head of', 'assistant', 'manager', 'scout', 'president']
BLOAT_ALTKEYWORDS = [r',,,trade|,,,3-team|,,,4-team|,,,sent|,,,received|,,,team|,,,activated|,,,placed|,,,2018|,,,assigned|,,,col|,CHI,,,|,CLE,,,']
BLOAT_ALTKEYWORDS2 = [r',,']

#AI VARIABLES
PREDICTION_OUTPUT_COLUMNS = ['SEASON', 'GAME_ID', 'PLAYER_ID', 'G', 'P', 'T', 'AVG_PTS', 'RTZ_PTS', 'RT3_PTS', 'RT5_PTS', 'RT9_PTS','PREV_PTS','PREV_FGA','PREV_FGM','AVG_FGM','AVG_FGA','RT3_FGM','RT3_FGA','RT5_FGM','RT5_FGA']
TARGET_COLUMNS = ['PTS', 'REB', 'AST', 'STL', 'BLK']
IGNORE_MAIN_COLUMNS = ['G', 'P', 'T', 'WL',
                       'MIN', 'PTS', 'REB', 'AST', 'STL', 'BLK', 'OREB', 'DREB','PF', 'TOV',
                       'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT',
                       'P0', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7', 'P8', 'P9']
FEATURE_IGNORE_COLUMNS = IGNORE_MAIN_COLUMNS
FEATURE_COLUMNS_TEST = ['SEASON','GAME_DATE','PLAYER_ID',
'TEAM', 'OPPONENT', 'WIN_TOT', 'IS_HOME',
'PREV_PTS', 'PREV_AST', 'PREV_REB', 'PREV_STL', 'PREV_BLK', 'PREV_MIN', 'PREV_FGA', 'PREV_FGM', 'PREV_FTA', 'PREV_FTM', 'PREV_FG3A', 'PREV_FG3M',
'MAX_PTS', 'MAX_AST', 'MAX_REB', 'MAX_STL', 'MAX_BLK', 'MAX_MIN', 'MAX_FGA', 'MAX_FGM', 'MAX_FTA', 'MAX_FTM', 'MAX_FG3A', 'MAX_FG3M',
'RTZ_MIN', 'RTZ_PTS', 'RTZ_REB', 'RTZ_AST', 'RTZ_STL', 'RTZ_BLK','RTZ_FGM', 'RTZ_FGA', 'RTZ_FG3M', 'RTZ_FG3A', 'RTZ_FTM', 'RTZ_FTA', 'RTZ_OREB', 'RTZ_DREB', 'RTZ_PF',
'AVG_MIN', 'AVG_PTS', 'AVG_REB', 'AVG_AST', 'AVG_STL', 'AVG_BLK', 'AVG_FGM', 'AVG_FGA', 'AVG_FG3M','AVG_FG3A', 'AVG_FTM', 'AVG_FTA', 'AVG_OREB','AVG_DREB', 'AVG_PF',
'RT3_MIN', 'RT5_MIN', 'RT9_MIN', 'RT3_PTS', 'RT5_PTS', 'RT9_PTS', 'RT3_FGM', 'RT3_FGA', 'RT3_FTM', 'RT3_FTA',
'HEIGHT', 'WEIGHT', 'YEARS_IN_TEAM', 'POSITION',
'TEAM_1', 'TEAM_2', 'TEAM_3', 'TEAM_4', 'TEAM_5', 'TEAM_6', 'TEAM_7',
'OPP_1', 'OPP_2', 'OPP_3', 'OPP_4', 'OPP_5', 'OPP_6', 'OPP_7']
TARGET_SINGLE_COLUMN = 'PTS'

#GLOBAL DEFAULT NAMES
DEFAULT_PLAYER_DETAIL_NAME = 'player_detail_'
DEFAULT_CSV_TYPENAME = '.csv'
DEFAULT_LINEBREAK ='\n'

#FILES
CFG_FILE = f'{PROJECT_DIR}/globals/config.ini'
TEAM_LOCATIONS_JSON = f'{PROJECT_DIR}/data/other/team_locations.json'

MERGED_PREV_PATH = f'{PROJECT_DIR}/data/ai/merged/MERGED_GAMES/'
MERGED_PATH = f'{PROJECT_DIR}/data/ai/merged/MERGE_'

MERGED_PTS_PRED_FILE = f'{PROJECT_DIR}/data/ai/merged/merged_pts_preds.csv'
MERGED_REB_PRED_FILE = f'{PROJECT_DIR}/data/ai/merged/merged_reb_preds.csv'
MERGED_AST_PRED_FILE = f'{PROJECT_DIR}/data/ai/merged/merged_ast_preds.csv'

#SPORTSBOOK
SPORTSBOOK_SOURCE_HTML = f'{PROJECT_DIR}/data/odds/sportsbook_source.html'
INJURY_SOURCE_HTML = f'{PROJECT_DIR}/data/odds/injury_source.html'

#SYNC FILES
SYNC_ODDS_PREDICT_FILE = f'{PROJECT_DIR}/data/sync/s_odds.csv'
SYNC_PREDICTION_FILE = f'{PROJECT_DIR}/data/sync/s_predict.csv'
SYNC_SEASON_FILE = f'{PROJECT_DIR}/data/sync/s_season.csv'

#DATAFRAME FILES
CURRENT_SEASON_PRED_FULLPATH = f'{PROJECT_DIR}/data/ai/predictions/current_season.csv'
CURRENT_PREDICTION_FULLPATH = f'{PROJECT_DIR}/data/ai/predictions/current.csv'

ALL_COMBINED = f'{PROJECT_DIR}/data/all_combined.csv'
ALL_DETAILS = f'{PROJECT_DIR}/data/all_details.csv'
ALL_FINAL = f'{PROJECT_DIR}/data/all_final.csv'
ALL_LOGS = f'{PROJECT_DIR}/data/all_logs.csv'
ALL_PARTIALS = f'{PROJECT_DIR}/data/all_partials.csv'

UNKNOWN_EVENTS_FULLPATH = f'{PROJECT_DIR}/data/other/unknown_events.csv'
LOCATION_DISTANCES = f'{PROJECT_DIR}/data/other/location_distances.csv'
VALID_SKIP_DATES = f'{PROJECT_DIR}/data/other/valid_skip_dates.csv'
BLOAT_DATA_FULLPATH = f'{PROJECT_DIR}/data/other/bloat_data.csv'
EVENTS_FULLPATH = f'{PROJECT_DIR}/data/other/event_data.csv'
ALL_GAMES = f'{PROJECT_DIR}/data/other/all_games.csv'
UNIQUE_OFFICIALS = f'{PROJECT_DIR}/data/other/unique_officials.csv'
OFFICIALS_TODAY = f'{PROJECT_DIR}/data/other/officials_today.csv'

CURRENT_INJURY_FULLPATH = f'{PROJECT_DIR}/data/odds/current_injurys.csv'
CURRENT_EVENTS_FULLPATH = f'{PROJECT_DIR}/data/odds/current_events.csv'
CURRENT_ODDS_FULLPATH = f'{PROJECT_DIR}/data/odds/current_odds.csv'

#DIRECTORIES
PREVIOUS_PRED_OUTPUT_DIR = f'{PROJECT_DIR}/data/ai/predictions/previous/'
MODE_STATE_OUTPUT_DIR = f'{PROJECT_DIR}/data/ai/states/'
DATAFRAME_AI_DIR = f'{PROJECT_DIR}/data/ai/dataframes/'
PRED_OUTPUT_DIR = f'{PROJECT_DIR}/data/ai/predictions/'
TOP_OUTPUT_DIR = f'{PROJECT_DIR}/data/ai/top/'
PREVIOUS_ODDS_DATA_DIR = f'{PROJECT_DIR}/data/odds/previous/'
ODDS_DATA_DIR = f'{PROJECT_DIR}/data/odds/'
PLAYER_DETAIL_DIR = f'{PROJECT_DIR}/data/player/'
SEASON_DATA_DIR = f'{PROJECT_DIR}/data/season/'
GAMES_DATA_DIR = f'{PROJECT_DIR}/data/game/'
DATA_DIR = f'{PROJECT_DIR}/data/'

#AI MODEL DATA
MODEL_PATH = f'{PROJECT_DIR}/data/ai/states/nba_prediction_model.h5'
EVAL_STATE_PATH = f'{PROJECT_DIR}/data/ai/states/evaluated_data.csv'
ENVIR_STATE_PATH = f'{PROJECT_DIR}/data/ai/states/env_state.pkl'
REPLAY_PATH = f'{PROJECT_DIR}/data/ai/states/replay_memory.pkl'
SCALER_PATH = f'{PROJECT_DIR}/data/ai/states/scaler.pkl'

#AI MODEL AND DATAFRAME FILES
fin_df_parse_cleanedfile = f'{PROJECT_DIR}/data/ai/dataframes/fin_df_parse_cleaned.csv'
dfs_correct_datafile = f'{PROJECT_DIR}/data/ai/dataframes/dfs_correct_datafile.csv'
dfs_sorted_cleanedfile= f'{PROJECT_DIR}/data/ai/dataframes/dfs_sorted_cleaned.csv'
val_err_results_path = f'{PROJECT_DIR}/data/ai/dataframes/val_err_results.csv'
fin_data_parsefile = f'{PROJECT_DIR}/data/ai/dataframes/fin_data_parse.csv'
results_df_file = f'{PROJECT_DIR}/data/ai/dataframes/results_df_file.csv'
df_rt3_alterfile = f'{PROJECT_DIR}/data/ai/dataframes/df_rt3_alter.csv'
df_rt5_alterfile = f'{PROJECT_DIR}/data/ai/dataframes/df_rt5_alter.csv'
df_rt9_alterfile = f'{PROJECT_DIR}/data/ai/dataframes/df_rt9_alter.csv'
dfothers_file = f'{PROJECT_DIR}/data/ai/dataframes/dfothers_file.csv'
linked_df_file = f'{PROJECT_DIR}/data/ai/dataframes/linked_df.csv'
dfs_sortedfile= f'{PROJECT_DIR}/data/ai/dataframes/dfs_sorted.csv'
check_df_file = f'{PROJECT_DIR}/data/ai/dataframes/check_df.csv'
dfaltersfile = f'{PROJECT_DIR}/data/ai/dataframes/dfalters.csv'
fin_datafile = f'{PROJECT_DIR}/data/ai/dataframes/fin_data.csv'

#AI MODEL STATES
model_weights_file = f'{PROJECT_DIR}/data/ai/states/model_weights_file.tf'
scaler_state_file = f'{PROJECT_DIR}/data/ai/states/scaler_state_file.pkl'
model_state_file = f'{PROJECT_DIR}/data/ai/states/model_state_file.h5'
best_model_file = f'{PROJECT_DIR}/data/ai/states/best/best_model.h5'

#URLS
SPORTSBOOK_API_URL = 'https://sbapi.in.sportsbook.fanduel.com/api/'
ESPN_NBA_INJURY_URL = 'https://www.espn.com/nba/injuries'

#SQL TABLES
SPORTSBOOK_ODDS_TABLE = 'sportsbook_odds'
NBA_PREDICTIONS_TABLE = 'nba_predictions'
NBA_STATS_TABLE = 'nba_stats'

##### THIS HAS BEEN UPDATE AND IS NO LONGER ACCURATE
#########################################################################################################################################
#######################################################    FINAL DATA STRUCTURE     #####################################################
#########################################################################################################################################
# 'PLAYER_ID', 'SEASON', 'GAME_ID', 'GAME_DATE', 'GAME_TIME', 'TEAM_ID', 'TEAM_NAME', 'TEAM_CITY', 'IS_HOME', 'IS_WIN', 'IS_STARTING'
# 'WINS', 'LOSSES', 'PLAYER_NAME', 'OFFICIAL1', 'OFFICIAL2','START_POSITION', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A',
# 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS',
# 'IS_PREDICTOR', 'TEAM_DNP', 'TEAM_OUT', 'OPP_DNP', 'OPP_OUT', 'POSITION', 'DISTANCE', 'OPP_DISTANCE', 'ALTITUDE', 'GAMES_IN',
# 'GAMES_OUT', 'GAMES_CONT', 'GAMES_START', 'GAMES_BENCH','OPP_NAME', 'OPP_ID', 'OPP_WINS', 'OPP_LOSSES', 'MATCHEDONENT',
# 'OPP_PLAYER_ID', 'DEF_PTS', 'DEF_AST', 'DEF_REB', 'TEAM_OUT_START', 'TEAM_OUT_BENCH', 'OPP_OUT_START', 'OPP_OUT_BENCH','TWIN',
# 'TLOSS', 'OWIN', 'OLOSS', 'IS_OUT', 'DAYS_OUT', 'AVG_MIN', 'RT3_MIN', 'RT5_MIN', 'RT9_MIN', 'AVG_PTS', 'RT3_PTS', 'RT5_PTS',
# 'RT9_PTS', 'AVG_REB', 'RT3_REB', 'RT5_REB', 'RT9_REB','AVG_AST', 'RT3_AST', 'RT5_AST', 'RT9_AST', 'AVG_STL', 'RT3_STL',
# 'RT5_STL', 'RT9_STL', 'AVG_BLK', 'RT3_BLK', 'RT5_BLK', 'RT9_BLK', 'AVG_FGM', 'RT3_FGM', 'RT5_FGM', 'RT9_FGM', 'AVG_FGA',
# 'RT3_FGA', 'RT5_FGA', 'RT9_FGA', 'AVG_FG3M', 'RT3_FG3M', 'RT5_FG3M', 'RT9_FG3M', 'AVG_FG3A', 'RT3_FG3A', 'RT5_FG3A',
# 'RT9_FG3A', 'AVG_FTM', 'RT3_FTM', 'RT5_FTM', 'RT9_FTM', 'AVG_FTA','RT3_FTA', 'RT5_FTA', 'RT9_FTA', 'AVG_OREB', 'RT3_OREB',
# 'RT5_OREB', 'RT9_OREB', 'AVG_DREB', 'RT3_DREB', 'RT5_DREB', 'RT9_DREB', 'AVG_PF', 'RT3_PF', 'RT5_PF', 'RT9_PF', 'RTZ_MIN',
# 'RTZ_PTS', 'RTZ_REB', 'RTZ_AST', 'RTZ_STL', 'RTZ_BLK', 'RTZ_FGM', 'RTZ_FGA', 'RTZ_FG3M', 'RTZ_FG3A', 'RTZ_FTM', 'RTZ_FTA',
# 'RTZ_OREB', 'RTZ_DREB', 'RTZ_PF', 'PREV_PTS', 'PREV_AST','PREV_REB', 'PREV_STL', 'PREV_BLK', 'PREV_MIN', 'PREV_FGA', 'PREV_PF',
# 'PREV_FGM', 'PREV_FTA', 'PREV_FTM', 'PREV_FG3A', 'PREV_FG3M', 'MAX_PTS', 'MAX_AST', 'MAX_REB', 'MAX_STL', 'MAX_BLK',
# 'MAX_MIN','MAX_FGA', 'MAX_FGM', 'MAX_FTA', 'MAX_FTM', 'MAX_FG3A', 'MAX_FG3M', 'GAME_DAY', 'LAST_GAME_DAYS', 'BACKTOBACKGAME',
# 'WEEK_PLAYTIME', 'OPP_LAST_GAME_DAYS', 'OPP_BACKTOBACKGAME','OPP_WEEK_PLAYTIME', 'OPP_DEF_PTS1', 'OPP_DEF_PTS3',
# 'OPP_DEF_PTS5', 'OPP_DEF_PTS9', 'OPP_DEF_PTSAVG', 'OPP_DEF_REB1', 'OPP_DEF_REB3', 'OPP_DEF_REB5', 'OPP_DEF_REB9',
# 'OPP_DEF_REBAVG','OPP_DEF_AST1', 'OPP_DEF_AST3', 'OPP_DEF_AST5', 'OPP_DEF_AST9', 'OPP_DEF_ASTAVG', 'AGE', 'BIRTHDATE',
# 'SCHOOL', 'COUNTRY', 'HEIGHT', 'WEIGHT', 'YEARS_IN_TEAM', 'RETIRED', 'GAMEDATEINT','BIRTHDATEINT', 'TEAM_CENTER',
# 'TEAM_CENTER-FORWARD', 'TEAM_FORWARD', 'TEAM_FORWARD-CENTER', 'TEAM_FORWARD-GUARD', 'TEAM_GUARD', 'TEAM_GUARD-FORWARD',
# 'OPP_CENTER', 'OPP_CENTER-FORWARD','OPP_FORWARD', 'OPP_FORWARD-CENTER', 'OPP_FORWARD-GUARD', 'OPP_GUARD', 'OPP_GUARD-FORWARD',
# 'ALTITUDE_B', 'LOSSES_B', 'TEAM_DNP_B', 'TEAM_OUT_B', 'DISTANCE_B', 'GAMES_OUT_B', 'OPP_WINS_B','TLOSS_B', 'OWIN_B',
# 'DAYS_OUT_B', 'BACKTOBACKGAME_B', 'WEEK_PLAYTIME_B', 'OPP_LAST_GAME_DAYS_B', 'TEAM_ALLOUT', 'OPP_ALLOUT',,'TEAM_CENTERS_IN',
# 'TEAM_FORWARDS_IN','TEAM_GUARDS_IN','OPP_CENTERS_IN','OPP_FORWARDS_IN','OPP_GUARDS_IN','TEAM_CENTERS_IN_B',
# 'TEAM_FORWARDS_IN_B','TEAM_GUARDS_IN_B','OPP_CENTERS_IN_B','OPP_FORWARDS_IN_B','OPP_GUARDS_IN_B','OPP_PLAYER_NAME',
# 'OPP_PREV_PF','OPP_RT3_PF','OPP_AVG_PF','OPP_PREV_STL','OPP_RT3_STL','OPP_AVG_STL','OPP_PREV_BLK','OPP_RT3_BLK','OPP_AVG_BLK'
# 'OPP_DEF_PREVRANK','OPP_DEF_RT5RANK','OPP_DEF_AVGRANK','OPP_DEF_PREVRANK_B','OPP_DEF_RT5RANK_B','OPP_DEF_AVGRANK_B'
#########################################################################################################################################
#########################################################################################################################################
