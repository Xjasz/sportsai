from datetime import date
##########################################
######### RUN SETTINGS FOR DATA PREDICTION
run_from_start_to_finish = True
current_season_only = False
use_today = True
use_seasons = False
merge_predictions = True
use_database = False
prediction_season = '2024'
prediction_date = '2025-01-27'
odds_date = '2025-01-27'
##########################################
##########################################
################# RUN SETTINGS FOR SYNCING
will_sync_server = False
sync_odds_date = '2025-01-27'
##########################################
if use_today:
    prediction_date = date.today().isoformat()
    odds_date = date.today().isoformat()
    sync_odds_date = date.today().isoformat()
