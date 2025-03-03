import configparser
import globals.global_settings as gls
from sqlalchemy import create_engine, text

print('Starting...')

config = configparser.ConfigParser()
config.read(gls.CFG_FILE)
mys_sv = gls.SPORTSAI_DBSERVER
mys_us = gls.SPORTSAI_DBUSER
mys_ps = gls.SPORTSAI_DBPASS
engine = create_engine(f'mysql+mysqlconnector://{mys_us}:{mys_ps}@{mys_sv}')

def create_sportsbook_odds(_engine):
    print('Creating Sportsbook Odds Table...')
    CREATE_SQL_SCRIPT = """
    CREATE TABLE {} (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        EventName VARCHAR(100),
        OpenDate DATETIME,
        DateChecked DATETIME,
        BetName VARCHAR(100),
        BetType VARCHAR(100),
        OverValue DECIMAL(10, 2),
        OverOdds INT,
        UnderValue DECIMAL(10, 2),
        UnderOdds INT
    );
    """.format(gls.SPORTSBOOK_ODDS_TABLE)
    with _engine.connect() as connection:
        connection.execute(text(CREATE_SQL_SCRIPT))
    _engine.dispose()
    print('Created Sportsbook Odds Table...')

def create_nba_predicitons(_engine):
    print('Creating NBA Predictions Table...')
    CREATE_SQL_SCRIPT = '''
    CREATE TABLE {} (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        PredictDate DATE,
        EventName VARCHAR(100),
        PlayerId INT,
        PlayerName VARCHAR(75),
        BetType VARCHAR(100),
        OverValue DECIMAL(10, 2),
        OverOdds INT,
        UnderValue DECIMAL(10, 2),
        UnderOdds INT,
        Points INT,
        Prediction DECIMAL(10, 2),
        Confidence DECIMAL(15, 10),
        AveragePoints DECIMAL(10, 2),
        RTPercent DECIMAL(10, 2),
        RT3Percent DECIMAL(10, 2),
        RT5Percent DECIMAL(10, 2),
        RT9Percent DECIMAL(10, 2)
    );
    '''.format(gls.NBA_PREDICTIONS_TABLE)

    with _engine.connect() as connection:
        connection.execute(text(CREATE_SQL_SCRIPT))
    _engine.dispose()
    print('Created NBA Predictions Table...')


def create_nba_stats(_engine):
    print('Creating NBA Stats Table...')
    CREATE_SQL_SCRIPT = '''
    CREATE TABLE {} (
        ID INT AUTO_INCREMENT PRIMARY KEY,
        SEASON INT,
        GAME_ID INT,
        PLAYER_ID INT,
        G DATE,
        P VARCHAR(255),
        RTZ_PTS DECIMAL(10, 2),
        RT3_PTS DECIMAL(10, 2),
        RT5_PTS DECIMAL(10, 2),
        RT9_PTS DECIMAL(10, 2),
        T VARCHAR(10),
        AVG_PTS DECIMAL(10, 2),
        PREV_PTS INT,
        PREV_FGA INT,
        PREV_FGM INT,
        AVG_FGM DECIMAL(10, 2),
        AVG_FGA DECIMAL(10, 2),
        RT3_FGM DECIMAL(10, 2),
        RT3_FGA DECIMAL(10, 2),
        RT5_FGM DECIMAL(10, 2),
        RT5_FGA DECIMAL(10, 2),
        PTS DECIMAL(10, 2),
        PTS_Predict DECIMAL(10, 2),
        CONF DECIMAL(15, 10),
        RT_PCT DECIMAL(10, 2),
        RT3G_PCT DECIMAL(10, 2),
        RT5G_PCT DECIMAL(10, 2),
        RT9G_PCT DECIMAL(10, 2),
        DIFF_AVG DECIMAL(10, 2),
        DIFF_ACT DECIMAL(10, 2)
    );
    '''.format(gls.NBA_STATS_TABLE)

    with _engine.connect() as connection:
        connection.execute(text(CREATE_SQL_SCRIPT))
    _engine.dispose()
    print('Created NBA Stats Table...')

create_nba_stats(engine)

print('Finished...')