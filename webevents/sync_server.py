import configparser
import os
import pandas as pd
import sqlalchemy
from globals import global_utils as mutils
import globals.global_settings as gls
import globals.run_settings as rns
from sqlalchemy import create_engine

print('Starting...')

if not os.path.exists(gls.SYNC_PREDICTION_FILE):
    print(f'Prediction file not found : {gls.SYNC_PREDICTION_FILE}')
    exit(0)
if not os.path.exists(gls.SYNC_ODDS_PREDICT_FILE):
    print(f'Odds file not found : {gls.SYNC_ODDS_PREDICT_FILE}')
    exit(0)

odds_df = pd.read_csv(gls.SYNC_ODDS_PREDICT_FILE)

print('preds ready to match...')

def split_event(row):
    date = rns.sync_odds_date
    overval = row['SP_VAL']
    underval = row['SP_VAL']
    return pd.Series([date, overval, underval])

odds_df[['PredictDate', 'OverValue', 'UnderValue']] = odds_df.apply(split_event, axis=1)
print(f'odds_df = {odds_df}')
new_df = odds_df[['PredictDate','EventName','PLAYER','BetType','OverValue','UnderValue','OverOdds','UnderOdds','PTS_PRED','RT_PCT','RT3G_PCT','RT5G_PCT','RT9G_PCT']].copy()
new_df.rename(columns={'PTS_PRED': 'Prediction'}, inplace=True)
new_df.rename(columns={'PLAYER': 'PlayerName'}, inplace=True)
new_df.rename(columns={'RT_PCT': 'RTPercent'}, inplace=True)
new_df.rename(columns={'RT3G_PCT': 'RT3Percent'}, inplace=True)
new_df.rename(columns={'RT5G_PCT': 'RT5Percent'}, inplace=True)
new_df.rename(columns={'RT9G_PCT': 'RT9Percent'}, inplace=True)
new_df['Points'] = 0
new_df['AveragePoints'] = 0
new_df['PlayerId'] = 0
mv_col = new_df.pop('Prediction')
new_df.insert(8, 'Prediction', mv_col)
mvpl_col = new_df.pop('PlayerId')
new_df.insert(2, 'PlayerId', mvpl_col)


final_df = pd.read_csv(gls.ALL_FINAL)
txt_df = final_df[final_df['G'] == rns.sync_odds_date]
all_zero = (txt_df[gls.TARGET_SINGLE_COLUMN] == 0).all()

# if all_zero:
#     print(f'Updated data not found: {gls.ALL_FINAL}')
#     exit(0)

print("Matching Points...")
for index, row in txt_df.iterrows():
    player_name = row['P']
    player_id = row['PLAYER_ID']
    pts = row[gls.TARGET_SINGLE_COLUMN]
    avg_pts = row['AVG_PTS']
    matching_row = new_df[new_df['PlayerName'] == player_name]
    new_df['AveragePoints'] = new_df['AveragePoints'].astype(float)
    if not matching_row.empty:
        new_df.loc[matching_row.index, 'PlayerId'] = player_id
        new_df.loc[matching_row.index, 'Points'] = pts
        new_df.loc[matching_row.index, 'AveragePoints'] = avg_pts
new_df.fillna(0, inplace=True)
new_df['AveragePoints'] = new_df['AveragePoints'].apply(mutils.format_float)
new_df['Prediction'] = new_df['Prediction'].apply(mutils.format_float)
new_df['RTPercent'] = new_df['RTPercent'].apply(mutils.format_float)
new_df['RT3Percent'] = new_df['RT3Percent'].apply(mutils.format_float)
new_df['RT5Percent'] = new_df['RT5Percent'].apply(mutils.format_float)
new_df['RT9Percent'] = new_df['RT9Percent'].apply(mutils.format_float)

print("Matching Confidence...")
preds_df = pd.read_csv(gls.SYNC_PREDICTION_FILE)
preds_df.columns = preds_df.columns.str.strip()
new_df['Confidence'] = ''
for index, row in preds_df.iterrows():
    player_id = row['PLAYER_ID']
    conf = row['CONF']
    matching_row = new_df[new_df['PlayerId'] == player_id]
    if not matching_row.empty:
        new_df.loc[matching_row.index, 'Confidence'] = conf


removed_players = new_df[new_df['PlayerId'] == 0]
unique_players = removed_players['PlayerName'].unique()
for player_name in unique_players:
    print(f"Removed player: {player_name}")


print('Parsing data completed....')
new_df = new_df[new_df['PlayerId'] != 0]
outbound_df = new_df.copy()
outbound_count = len(outbound_df)

print(f'Ready to sync {outbound_count} predictions...')

if outbound_count > 0:
    print(f'Syncing with Table: {gls.NBA_PREDICTIONS_TABLE}')
    mys_sv = gls.SPORTSAI_DBSERVER
    mys_us = gls.SPORTSAI_DBUSER
    mys_ps = gls.SPORTSAI_DBPASS
    engine = create_engine(f'mysql+mysqlconnector://{mys_us}:{mys_ps}@{mys_sv}')
    column_types = {'Confidence': sqlalchemy.types.DECIMAL(15, 10)}
    outbound_df.to_sql(gls.NBA_PREDICTIONS_TABLE, con=engine, if_exists='append', index=False, dtype=column_types)
    print(f'Synced {gls.NBA_PREDICTIONS_TABLE} with {outbound_count} records...')
    engine.dispose()

print('Removed odds sync file....')
os.remove(gls.SYNC_ODDS_PREDICT_FILE)
print('Removed predict sync file....')
os.remove(gls.SYNC_PREDICTION_FILE)

if not os.path.exists(gls.SYNC_SEASON_FILE):
    print(f'season stats file not found : {gls.SYNC_SEASON_FILE}')
    exit(0)

print(f'Syncing with nba stats Table: {gls.NBA_STATS_TABLE}')
season_df = pd.read_csv(gls.SYNC_SEASON_FILE)
outbound_df = season_df.copy()
outbound_count = len(outbound_df)
if outbound_count > 0:
    print(f'Syncing with Table: {gls.NBA_STATS_TABLE}')
    config = configparser.ConfigParser()
    config.read(gls.CFG_FILE)
    mys_sv = gls.SPORTSAI_DBSERVER
    mys_us = gls.SPORTSAI_DBUSER
    mys_ps = gls.SPORTSAI_DBPASS
    engine = create_engine(f'mysql+mysqlconnector://{mys_us}:{mys_ps}@{mys_sv}')
    col_types = {'CONF': sqlalchemy.types.DECIMAL(15, 10)}
    # noinspection PyTypeChecker
    outbound_df.to_sql(gls.NBA_STATS_TABLE, con=engine, if_exists='append', index=False, dtype=col_types)
    print(f'Synced {gls.NBA_STATS_TABLE} with {outbound_count} records...')
    engine.dispose()

print('Removed stats sync file....')
os.remove(gls.SYNC_SEASON_FILE)

print('Finished...')