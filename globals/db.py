from sqlalchemy import create_engine
import globals.global_settings as gls

def engine():
    return create_engine(f'mysql+mysqlconnector://{gls.SPORTSAI_DBUSER}:{gls.SPORTSAI_DBPASS}@{gls.SPORTSAI_DBSERVER}')
