import argparse
import configparser
import datetime
import logging
import os
import pathlib
import shutil
import sys
import time
import traceback

import pandas as pd

import globals.global_settings as gls

STAGE_ORDER = ['env', 'api', 'data', 'features', 'scrape']
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRATCH_DIR = os.path.join(SCRIPT_DIR, '.scratch')

def require_main_guard(path):
    if "if __name__ == '__main__':" not in pathlib.Path(path).read_text(encoding='utf-8'):
        raise SystemExit(f'{path} has no __main__ guard; merge feature/data-pipeline and feature/ai-output first')
logger = logging.getLogger('smoke_test')


def ascii_only(text):
    return str(text).encode('ascii', 'replace').decode('ascii')


def info(msg): logger.info(ascii_only(msg))
def warn(msg): logger.warning(ascii_only(msg))
def error(msg): logger.error(ascii_only(msg))


def timed(func, *a, **kw):
    t0 = time.time()
    result = func(*a, **kw)
    return result, time.time() - t0


class Tee:
    def __init__(self, stream, fh):
        self.stream, self.fh = stream, fh

    def write(self, data):
        data = ascii_only(data)
        self.stream.write(data)
        try:
            self.fh.write(data)
        except Exception:
            pass
        return len(data)

    def flush(self):
        self.stream.flush()
        self.fh.flush()


def list_seasons(base_dir):
    return sorted(d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))) if os.path.isdir(base_dir) else []


def list_csv(dir_path):
    return sorted(f for f in os.listdir(dir_path) if f.endswith('.csv')) if os.path.isdir(dir_path) else []


def log_file_info(label, path):
    if os.path.exists(path):
        info(f'{label}: {path} ({os.path.getsize(path)} bytes)')
    else:
        info(f'{label} missing: {path}')


def log_exception(prefix):
    error(f'{prefix} EXCEPTION:')
    for line in traceback.format_exc().splitlines():
        error(line)


def stage_env(args):
    import globals.run_settings as rns
    info(f'python: {sys.version.split()[0]}')
    versions = {}
    for modname in ['nba_api', 'pandas', 'numpy', 'selenium', 'sqlalchemy', 'openpyxl', 'tensorflow', 'torch']:
        try:
            versions[modname] = getattr(__import__(modname), '__version__', 'unknown')
        except ImportError:
            versions[modname] = 'not installed'
    info(f'versions: {versions}')
    info(f'env vars set: {sorted(k for k in os.environ if k.startswith(("SPORTSAI_", "BROWSER_", "GECKO_")))}')
    rns_keys = ['update_active_players_team', 'run_from_start_to_finish', 'current_season_only', 'use_today',
                'use_seasons', 'merge_predictions', 'use_database', 'prediction_season', 'prediction_date', 'odds_date']
    info(f'rns settings: { {k: getattr(rns, k, None) for k in rns_keys} }')
    if os.path.exists(gls.CFG_FILE):
        cfg = configparser.ConfigParser()
        cfg.read(gls.CFG_FILE)
        info(f'config.ini: {dict(cfg["DEFAULT"])}')
    else:
        info(f'config.ini missing at {gls.CFG_FILE}')
    seasons = list_seasons(gls.GAMES_DATA_DIR)
    counts = {s: len(list_csv(os.path.join(gls.GAMES_DATA_DIR, s))) for s in seasons}
    info(f'GAMES_DATA_DIR={gls.GAMES_DATA_DIR} game file counts by season: {counts}')
    newest_files = list_csv(os.path.join(gls.GAMES_DATA_DIR, seasons[-1])) if seasons else []
    info(f'newest game file: {newest_files[-1] if newest_files else None}')
    info(f'PLAYER_DETAIL_DIR={gls.PLAYER_DETAIL_DIR} player detail files: {len(list_csv(gls.PLAYER_DETAIL_DIR))}')
    log_file_info('season csv', f'{gls.SEASON_DATA_DIR}{rns.prediction_season}.csv')
    log_file_info('all_final.csv', gls.ALL_FINAL)


def stage_api(args):
    import nba_api
    from globals import global_utils as mu
    from nba_api.stats.endpoints import commonallplayers, commonplayerinfo, playercareerstats
    from builders import gamelog_builder as glb
    info(f'nba_api: {nba_api.__version__}')
    ids, dt = timed(glb.fetch_game_ids, args.run_date)
    info(f'fetch_game_ids({args.run_date}) took {dt:.2f}s -> {len(ids)} ids: {ids}')
    if not ids:
        warn('No game ids returned for date, skipping remaining api checks')
        return
    raw_id = ids[0]
    norm_id = raw_id[2:] if raw_id.startswith('00') else raw_id
    full_id = f'00{norm_id}'
    frames, dt = timed(mu.get_game_details, full_id)
    info(f'get_game_details({full_id}) took {dt:.2f}s')
    names = ['player_stats', 'team_stats', 'game_summary', 'line_score', 'inactive_players', 'officials']
    for name, frame in zip(names, frames):
        info(f'{name}: shape={frame.shape} columns={list(frame.columns)}')
    player_stats, team_stats, game_summary, line_score, inactive_players, officials = frames
    info(f'player_stats COMMENT value_counts: {player_stats["COMMENT"].value_counts(dropna=False).to_dict()}')
    info(f'player_stats START_POSITION value_counts: {player_stats["START_POSITION"].value_counts(dropna=False).to_dict()}')
    info(f'player_stats sample MIN values: {player_stats["MIN"].head(3).tolist()}')
    season_df, dt = timed(mu.get_season_data, args.run_date)
    info(f'get_season_data({args.run_date}) took {dt:.2f}s -> shape={season_df.shape}')
    game_id_str = str(player_stats['GAME_ID'].iloc[0])
    for team_id in player_stats['TEAM_ID'].unique().tolist():
        match = season_df[(season_df['TEAM_ID'] == team_id) & (season_df['GAME_ID'] == game_id_str)]
        if not match.empty:
            info(f'team {team_id} WINS={match.iloc[0]["WINS"]} LOSSES={match.iloc[0]["LOSSES"]}')
        else:
            warn(f'team {team_id} not found in season_data for game {game_id_str}')
    all_players, dt = timed(lambda: commonallplayers.CommonAllPlayers(is_only_current_season='1', league_id='00').get_data_frames()[0])
    info(f'commonallplayers took {dt:.2f}s -> rows={len(all_players)}')
    first_player_id = player_stats['PLAYER_ID'].iloc[0]
    info_df, dt = timed(lambda: commonplayerinfo.CommonPlayerInfo(player_id=first_player_id, league_id_nullable='00').get_data_frames()[0])
    info(f'commonplayerinfo({first_player_id}) took {dt:.2f}s -> shape={info_df.shape}')
    career_df, dt = timed(lambda: playercareerstats.PlayerCareerStats(player_id=first_player_id, league_id_nullable='00').get_data_frames()[0])
    info(f'playercareerstats({first_player_id}) took {dt:.2f}s -> shape={career_df.shape}')


def stage_data(args):
    from globals import global_utils as mu
    from builders import gamelog_builder as glb
    raw_dir = os.path.join(SCRATCH_DIR, 'game_raw') + os.sep
    os.makedirs(raw_dir, exist_ok=True)
    ids = glb.fetch_game_ids(args.run_date)
    info(f'fetch_game_ids({args.run_date}) -> {len(ids)} ids, using up to {args.games}')
    seasondf = mu.get_season_data(args.run_date)
    stat_cols = ['FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS', 'PLUS_MINUS']
    for game_id in ids[:args.games]:
        info(f'create_game_data({game_id}) starting (sleeps 2-6s after save)')
        df, dt = timed(glb.create_game_data, game_id, seasondf, alt_directory=raw_dir)
        info(f'create_game_data({game_id}) took {dt:.2f}s -> rows={len(df)} columns={list(df.columns)}')
        info(f'COMMENT value_counts: {df["COMMENT"].value_counts(dropna=False).to_dict()}')
        pos_counts = df['START_POSITION'].value_counts(dropna=False).to_dict()
        info(f'START_POSITION value_counts: {pos_counts}')
        for pos, expected in [('C', 2), ('G', 4), ('F', 4)]:
            if pos_counts.get(pos, 0) != expected:
                warn(f"START_POSITION '{pos}' count is {pos_counts.get(pos, 0)}, expected {expected} (feature code raises on this)")
        for team_id, group in df.groupby('TEAM_ID'):
            row = group.iloc[0]
            info(f'team {team_id}: WINS={row["WINS"]} LOSSES={row["LOSSES"]} OFFICIAL1={row["OFFICIAL1"]} OFFICIAL2={row["OFFICIAL2"]}')
        info(f'GAME_DATE={df["GAME_DATE"].iloc[0]} SEASON={df["SEASON"].iloc[0]}')
        info(f'NaN counts per stat column: { {c: int(df[c].isna().sum()) for c in stat_cols if c in df.columns} }')


def copy_features_input(season, num_files):
    feat_root = os.path.join(SCRATCH_DIR, 'features')
    if os.path.exists(feat_root):
        shutil.rmtree(feat_root)
    game_dst = os.path.join(feat_root, 'game', season)
    player_dst = os.path.join(feat_root, 'player')
    season_dst = os.path.join(feat_root, 'season')
    for d in (game_dst, player_dst, season_dst):
        os.makedirs(d, exist_ok=True)
    real_season_dir = os.path.join(gls.GAMES_DATA_DIR, season)
    game_files = list_csv(real_season_dir)
    picked = game_files[-num_files:]
    info(f'Copying {len(picked)} of {len(game_files)} game files from {real_season_dir}')
    player_ids = set()
    for fname in picked:
        shutil.copy2(os.path.join(real_season_dir, fname), os.path.join(game_dst, fname))
        df = pd.read_csv(os.path.join(game_dst, fname), usecols=['PLAYER_ID'])
        player_ids.update(df['PLAYER_ID'].astype(str).tolist())
    real_player_files = os.listdir(gls.PLAYER_DETAIL_DIR) if os.path.isdir(gls.PLAYER_DETAIL_DIR) else []
    matched = [f for f in real_player_files if any(f'_{pid}_' in f for pid in player_ids)]
    info(f'Copying {len(matched)} matching player detail files of {len(real_player_files)} total')
    for fname in matched:
        shutil.copy2(os.path.join(gls.PLAYER_DETAIL_DIR, fname), os.path.join(player_dst, fname))
    cfg_dst = os.path.join(feat_root, 'config.ini')
    if os.path.exists(gls.CFG_FILE):
        shutil.copy2(gls.CFG_FILE, cfg_dst)
    else:
        with open(cfg_dst, 'w', encoding='utf-8') as f:
            f.write('[DEFAULT]\n')
    info(f'Copied config.ini to {cfg_dst}')
    return feat_root, season_dst


def stage_features(args):
    import globals.run_settings as rns
    from builders import ai_builder as aib, playerdetail_builder as pld
    require_main_guard(os.path.join(SCRIPT_DIR, 'generate_data.py'))
    import generate_data as gd
    seasons = list_seasons(gls.GAMES_DATA_DIR)
    if not seasons:
        raise RuntimeError(f'No season directories found under {gls.GAMES_DATA_DIR}')
    season = seasons[-1]
    info(f'Using season {season} for features stage')
    feat_root, season_dst = copy_features_input(season, args.files)

    for attr, fname in [('ALL_LOGS', 'all_logs.csv'), ('ALL_DETAILS', 'all_details.csv'), ('ALL_COMBINED', 'all_combined.csv'),
                         ('ALL_FINAL', 'all_final.csv'), ('ALL_PARTIALS', 'all_partials.csv'), ('ALL_GAMES', 'all_games.csv')]:
        setattr(gls, attr, os.path.join(feat_root, fname))
    gls.GAMES_DATA_DIR = os.path.join(feat_root, 'game') + os.sep
    gls.SEASON_DATA_DIR = season_dst + os.sep
    gls.PLAYER_DETAIL_DIR = os.path.join(feat_root, 'player') + os.sep
    gls.CFG_FILE = os.path.join(feat_root, 'config.ini')
    gls.PRED_OUTPUT_DIR = os.path.join(feat_root, 'predictions') + os.sep
    gls.DATAFRAME_AI_DIR = os.path.join(feat_root, 'dataframes') + os.sep
    rns.current_season_only = True
    rns.prediction_season = season
    rns.run_from_start_to_finish = True

    steps = [
        ('clear_calculations', gd.clear_calculations),
        ('delete_invalid_player_details', pld.delete_invalid_player_details),
        ('set_invalid_players', gd.set_invalid_players),
        ('set_positions_and_cleanup', gd.set_positions_and_cleanup),
        ('set_distance_altitude', gd.set_distance_altitude),
        ('track_player_out_events', gd.track_player_out_events),
        ('set_opponents', gd.set_opponents),
        ('set_player_opponents', gd.set_player_opponents),
        ('set_out_totals', gd.set_out_totals),
        ('set_current_wins', gd.set_current_wins),
        ('fix_prediction_values', gd.fix_prediction_values),
        ('combine_games_to_season', gd.combine_games_to_season),
        ('setup_season_values', gd.setup_season_values),
        ('build_all_files', aib.build_all_files),
    ]
    for step_name, func in steps:
        _, dt = timed(func)
        info(f'{step_name} took {dt:.2f}s')

    season_csv = os.path.join(season_dst, f'{season}.csv')
    if os.path.exists(season_csv):
        sdf = pd.read_csv(season_csv)
        info(f'season csv shape={sdf.shape} columns={len(sdf.columns)} (expected 203)')
    else:
        warn(f'season csv not found at {season_csv}')
    if os.path.exists(gls.ALL_FINAL):
        fdf = pd.read_csv(gls.ALL_FINAL)
        nan_count = int(fdf.isna().sum().sum())
        dup_count = int(fdf.duplicated(subset=['PLAYER_ID', 'GAME_ID']).sum())
        info(f'all_final.csv shape={fdf.shape} NaN={nan_count} duplicates(PLAYER_ID,GAME_ID)={dup_count}')
    else:
        warn(f'all_final.csv not found at {gls.ALL_FINAL}')


def stage_scrape(args):
    import webevents.get_latest_injurys as gli
    import webevents.get_latest_events as gle
    scratch_dir = os.path.join(SCRATCH_DIR, 'scrape')
    os.makedirs(scratch_dir, exist_ok=True)
    gls.CURRENT_INJURY_FULLPATH = os.path.join(scratch_dir, 'current_injurys.csv')
    gls.INJURY_SOURCE_HTML = os.path.join(scratch_dir, 'injury_source.html')
    gls.OFFICIALS_TODAY = os.path.join(scratch_dir, 'officials_today.csv')
    calls = [
        ('find_injury_news', gli.find_injury_news),
        ('scrape_game_officials', gle.scrape_game_officials),
        ('find_todays_nba_lineups', gle.find_todays_nba_lineups),
        ('find_sportsbook_games', gle.find_sportsbook_games),
    ]
    any_failed = False
    for name, func in calls:
        try:
            result, dt = timed(func)
            count = len(result)
            first = (result.iloc[0].to_dict() if isinstance(result, pd.DataFrame) else result[0]) if count else None
            info(f'{name} took {dt:.2f}s -> count={count} first={first}')
        except Exception:
            any_failed = True
            log_exception(name)
    if any_failed:
        raise RuntimeError('one or more scrape calls failed, see log for tracebacks')


STAGE_FUNCS = {'env': stage_env, 'api': stage_api, 'data': stage_data, 'features': stage_features, 'scrape': stage_scrape}


def run_stage(name, args):
    info(f'=== STAGE {name} START ===')
    t0 = time.time()
    ok = True
    try:
        STAGE_FUNCS[name](args)
    except Exception:
        ok = False
        log_exception(f'STAGE {name}')
    info(f'=== STAGE {name} END elapsed={time.time() - t0:.2f}s ok={ok} ===')
    return ok


def main():
    parser = argparse.ArgumentParser(description='Local smoke test for the sportsai data pipeline')
    parser.add_argument('--stage', dest='stages', action='append', choices=STAGE_ORDER)
    parser.add_argument('--date', default=None)
    parser.add_argument('--games', type=int, default=2)
    parser.add_argument('--files', type=int, default=30)
    args = parser.parse_args()
    selected_raw = args.stages or ['env', 'api', 'data', 'features']
    selected = [s for s in STAGE_ORDER if s in selected_raw]

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = os.path.join(SCRATCH_DIR, f'smoke_{ts}.log')
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    tee_fh = open(log_path, 'a', encoding='utf-8', buffering=1)
    sys.stdout = Tee(sys.stdout, tee_fh)
    sys.stderr = Tee(sys.stderr, tee_fh)

    date_str = args.date
    if not date_str:
        seasons = list_seasons(gls.GAMES_DATA_DIR)
        files = list_csv(os.path.join(gls.GAMES_DATA_DIR, seasons[-1])) if seasons else []
        date_str = files[-1][:10] if files else '2025-01-27'
    args.run_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    info(f'Stages: {selected}')
    info(f'Date: {args.run_date} Games: {args.games} Files: {args.files}')

    results = {name: run_stage(name, args) for name in selected}

    info('=== SUMMARY ===')
    for name in selected:
        info(f'{name}: {"PASS" if results[name] else "FAIL"}')
    print(f'Log file: {os.path.abspath(log_path)}')
    sys.exit(0 if all(results.values()) else 1)


if __name__ == '__main__':
    main()
