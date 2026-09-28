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
BROWSER_TYPE = 'FIREFOX'

#MAPPERS
TEAM_TOSHORT_MAPPER = {"Hawks": "ATL","Celtics": "BOS","Nets": "BKN","Hornets": "CHA","Bobcats": "CHA","Bulls": "CHI", "Cavaliers": "CLE","Mavericks": "DAL","Nuggets": "DEN","Pistons": "DET","Warriors": "GSW","Rockets": "HOU","Pacers": "IND","Clippers": "LAC","Lakers": "LAL","Grizzlies": "MEM","Heat": "MIA","Bucks": "MIL","Timberwolves": "MIN","Pelicans": "NOP","Knicks": "NYK","Thunder": "OKC","Sonics": "OKC","Magic": "ORL","76ers": "PHI","Suns": "PHX","Trail Blazers": "POR","Blazers": "POR","Kings": "SAC","Spurs": "SAS","Raptors": "TOR","Jazz": "UTA","Wizards": "WAS"}
POSITION_TOINT_MAPPER = {'Guard': 0,'Guard-Forward': 1,'Forward-Guard': 2,'Forward': 3,'Forward-Center': 4,'Center-Forward': 5,'Center': 6}
POSITION_TOSIZE_MAPPER = {'Guard': {'avg_height': "6-3", 'avg_weight': 185},'Forward': {'avg_height': "6-8", 'avg_weight': 220},'Center':  {'avg_height': "6-11", 'avg_weight': 240}}

#AI VARIABLES
PREDICTION_OUTPUT_COLUMNS = ['SEASON', 'GAME_ID', 'PLAYER_ID', 'G', 'P', 'T', 'AVG_PTS', 'RTZ_PTS', 'RT3_PTS', 'RT5_PTS', 'RT9_PTS','PREV_PTS','PREV_FGA','PREV_FGM','AVG_FGM','AVG_FGA','RT3_FGM','RT3_FGA','RT5_FGM','RT5_FGA']
TARGET_SINGLE_COLUMN = 'PTS'

#GLOBAL DEFAULT NAMES
DEFAULT_PLAYER_DETAIL_NAME = 'player_detail_'
DEFAULT_CSV_TYPENAME = '.csv'

#FILES
CFG_FILE = f'{PROJECT_DIR}/globals/config.ini'
TEAM_LOCATIONS_JSON = f'{PROJECT_DIR}/data/other/team_locations.json'

MERGED_PREV_PATH = f'{PROJECT_DIR}/data/ai/merged/MERGED_GAMES/'
MERGED_PATH = f'{PROJECT_DIR}/data/ai/merged/MERGE_'

MERGED_PTS_PRED_FILE = f'{PROJECT_DIR}/data/ai/merged/merged_pts_preds.csv'
MERGED_REB_PRED_FILE = f'{PROJECT_DIR}/data/ai/merged/merged_reb_preds.csv'
MERGED_AST_PRED_FILE = f'{PROJECT_DIR}/data/ai/merged/merged_ast_preds.csv'

#SPORTSBOOK
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

LOCATION_DISTANCES = f'{PROJECT_DIR}/data/other/location_distances.csv'
VALID_SKIP_DATES = f'{PROJECT_DIR}/data/other/valid_skip_dates.csv'
ALL_GAMES = f'{PROJECT_DIR}/data/other/all_games.csv'
UNIQUE_OFFICIALS = f'{PROJECT_DIR}/data/other/unique_officials.csv'
OFFICIALS_TODAY = f'{PROJECT_DIR}/data/other/officials_today.csv'

CURRENT_INJURY_FULLPATH = f'{PROJECT_DIR}/data/odds/current_injurys.csv'
CURRENT_ODDS_FULLPATH = f'{PROJECT_DIR}/data/odds/current_odds.csv'

#DIRECTORIES
PREVIOUS_PRED_OUTPUT_DIR = f'{PROJECT_DIR}/data/ai/predictions/previous/'
DATAFRAME_AI_DIR = f'{PROJECT_DIR}/data/ai/dataframes/'
PRED_OUTPUT_DIR = f'{PROJECT_DIR}/data/ai/predictions/'
TOP_OUTPUT_DIR = f'{PROJECT_DIR}/data/ai/top/'
PREVIOUS_ODDS_DATA_DIR = f'{PROJECT_DIR}/data/odds/previous/'
ODDS_DATA_DIR = f'{PROJECT_DIR}/data/odds/'
PLAYER_DETAIL_DIR = f'{PROJECT_DIR}/data/player/'
SEASON_DATA_DIR = f'{PROJECT_DIR}/data/season/'
GAMES_DATA_DIR = f'{PROJECT_DIR}/data/game/'

#AI MODEL AND DATAFRAME FILES
dfs_sorted_cleanedfile= f'{PROJECT_DIR}/data/ai/dataframes/dfs_sorted_cleaned.csv'
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
