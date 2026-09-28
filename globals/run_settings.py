from datetime import date
##########################################
######### RUN SETTINGS FOR DATA PREDICTION
update_active_players_team = False
run_from_start_to_finish = True
current_season_only = True
use_today = True
use_seasons = False
merge_predictions = True
use_database = True
prediction_season = str(date.today().year if date.today().month >= 8 else date.today().year - 1)
prediction_date = '2025-12-01'
odds_date = '2025-12-01'
##########################################
##########################################
################# RUN SETTINGS FOR SYNCING
will_sync_server = False
sync_odds_date = '2025-12-01'
##########################################
if use_today:
    prediction_date = date.today().isoformat()
    odds_date = date.today().isoformat()
    sync_odds_date = date.today().isoformat()
