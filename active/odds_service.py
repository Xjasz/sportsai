import os
import json
import time
import logging
import datetime

import pytz
import pandas as pd
import requests
from sqlalchemy import create_engine
from typing import Optional, Dict, List, Any

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',filename='sportsbook_odds.log')

logger = logging.getLogger()
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

logger.info('Starting...')
SPORTSBOOK_API_URL = 'https://sbapi.in.sportsbook.fanduel.com/api/'
MAIN_DB_CREDS = '{DB_CRED_JSON_LOCATION}'
SPORTSBOOK_ODDS_TABLE = 'sportsbook_odds'
TEAM_TOSHORT_MAPPER = {"Hawks": "ATL","Celtics": "BOS","Nets": "BKN","Hornets": "CHA","Bobcats": "CHA","Bulls": "CHI", "Cavaliers": "CLE","Mavericks": "DAL","Nuggets": "DEN","Pistons": "DET","Warriors": "GSW","Rockets": "HOU","Pacers": "IND","Clippers": "LAC","Lakers": "LAL","Grizzlies": "MEM","Heat": "MIA","Bucks": "MIL","Timberwolves": "MIN","Pelicans": "NOP","Knicks": "NYK","Thunder": "OKC","Sonics": "OKC","Magic": "ORL","76ers": "PHI","Suns": "PHX","Trail Blazers": "POR","Blazers": "POR","Kings": "SAC","Spurs": "SAS","Raptors": "TOR","Jazz": "UTA","Wizards": "WAS"}

with open(MAIN_DB_CREDS, 'r') as json_file:
    settings = json.load(json_file)
mys_sv = eventc_value = settings.get('my_sql_s')
mys_us = eventc_value = settings.get('my_sql_u')
mys_ps = eventc_value = settings.get('my_sql_p')

event_type = 'nba'
outbound_odds = 'odds_service_data.csv'
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_time = datetime.datetime.now()
utc_timezone = pytz.timezone('UTC')
est_timezone = pytz.timezone('US/Eastern')


def create_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) "
            "Gecko/20100101 Firefox/144.0"
        ),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Cookie": (
            "pxcts={PXCTS}; "
            "_pxvid={PXVID}"
        ),
    })
    return s

session = create_session()

def get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        resp = session.get(url, params=params, timeout=15)
    except requests.RequestException as e:
        logger.info("Request error for %s: %s", url, e)
        return None

    if not resp.ok:
        logger.info("HTTP %s for %s", resp.status_code, url)
        return None

    ctype = resp.headers.get("Content-Type", "")
    if "json" not in ctype:
        logger.info("Non-JSON response for %s (possible captcha/interstitial)", url)
        return None

    try:
        return resp.json()
    except json.JSONDecodeError:
        logger.info("Failed to parse JSON for %s", url)
        return None

# ------------------------------------------------------------
# Fetch upcoming games
# ------------------------------------------------------------

def find_upcoming_games() -> List[Dict[str, Any]]:
    params = {
        "currencyCode": "USD",
        "exchangeLocale": "en_US",
        "includePrices": "true",
        "language": "en",
        "regionCode": "NAMERICA",
        "timezone": "America/New_York",
        "_ak": "FhMFpcPWXMeyZxOx",
        "page": "CUSTOM",
        "customPageId": EVENT_TYPE,
    }
    event_type = 'nba'
    url = SPORTSBOOK_API_URL + "content-managed-page"
    logger.info("GET upcoming games: %s", url)
    data = get_json(url, params=params)
    if not data:
        logger.info("No data returned for upcoming games.")
        return []

    events = data.get("attachments", {}).get("events", {})
    extracted = []

    for ev in events.values():
        name = ev.get("name")
        if not name:
            continue

        if " @ " not in name or name.startswith("West @ East"):
            continue

        parts = name.split(" @ ")
        away_team = parts[0].strip().split()[-1]
        home_team = parts[1].strip().split()[-1]
        away_short = TEAM_TOSHORT_MAPPER.get(away_team)
        home_short = TEAM_TOSHORT_MAPPER.get(home_team)

        open_str = ev.get("openDate")
        if not open_str:
            continue

        dt = None
        try:
            dt = datetime.datetime.strptime(open_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            try:
                dt = datetime.datetime.strptime(open_str, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                pass
        if dt is None:
            continue

        dt_utc = utc_tz.localize(dt)
        dt_est = dt_utc.astimezone(est_tz).replace(tzinfo=None)

        extracted.append({
            "eventId": ev.get("eventId"),
            "name": name,
            "eventTypeId": ev.get("eventTypeId"),
            "competitionId": ev.get("competitionId"),
            "home_team": [home_team, home_short],
            "away_team": [away_team, away_short],
            "game_id": ev.get("eventId"),
            "open_date": dt_est
        })

    logger.info("Got %d upcoming events", len(extracted))
    return extracted

# ------------------------------------------------------------
# Fetch event markets
# ------------------------------------------------------------

def get_event_page(event_id: Any, tab: str) -> Optional[Dict[str, Any]]:
    params = {
        "_ak": "FhMFpcPWXMeyZxOx",
        "eventId": str(event_id),
        "tab": tab,
        "useCombinedTouchdownsVirtualMarket": "true"
    }
    url = SPORTSBOOK_API_URL + "event-page"
    logger.info("GET event-page: eventId=%s, tab=%s", event_id, tab)
    return get_json(url, params=params)

def extract_markets_for_event(ev: Dict[str, Any],tab: str,market_suffix: str) -> List[Dict[str, Any]]:
    data = get_event_page(ev["eventId"], tab)
    if not data:
        return []

    markets = data.get("attachments", {}).get("markets", {})
    results: List[Dict[str, Any]] = []

    for m in markets.values():
        mtype = m.get("marketType", "")
        if not (mtype.endswith(market_suffix) and "_ALT_" not in mtype):
            continue

        mname = m.get("marketName", "")
        if "-" not in mname:
            continue

        name_part, type_part = [x.strip() for x in mname.rsplit("-", 1)]

        base = {
            "EventName": ev["name"],
            "OpenDate": ev["open_date"],
            "DateChecked": formatted_now,
            "BetName": name_part,
            "BetType": type_part
        }

        over_val = None
        over_odds = None
        under_val = None
        under_odds = None

        for r in m.get("runners", []):
            res_type = r.get("result", {}).get("type")
            hcap = r.get("handicap")
            odds = (r.get("winRunnerOdds", {}).get("americanDisplayOdds", {}).get("americanOddsInt"))
            if res_type == "OVER":
                over_val = hcap
                over_odds = odds
            elif res_type == "UNDER":
                under_val = hcap
                under_odds = odds

        if over_val is not None or under_val is not None:
            rec = dict(base)
            if over_val is not None:
                rec["OverValue"] = over_val
                rec["OverOdds"] = over_odds
            if under_val is not None:
                rec["UnderValue"] = under_val
                rec["UnderOdds"] = under_odds
            results.append(rec)

    return results

def fetch_and_process_event_data(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for ev in events:
        event_id = ev.get("eventId")
        name = ev.get("name")
        open_date = ev.get("open_date")
        if not event_id or not name or not open_date:
            continue
        if open_date < current_time:
            logger.info("Skipping event %s %s startDate: %s (GAME IS LIVE)",event_id, name, open_date)
            continue
        logger.info("Getting data for event %s %s startDate: %s (GAME IS UPCOMING)", event_id, name, open_date)
        all_rows.extend(extract_markets_for_event(ev, "player-points", "_TOTAL_POINTS"))
        time.sleep(3)
        all_rows.extend(extract_markets_for_event(ev, "player-rebounds", "_TOTAL_REBOUNDS"))
        time.sleep(3)
        all_rows.extend(extract_markets_for_event(ev, "player-assists", "_TOTAL_ASSISTS"))
        time.sleep(3)
    logger.info("Total extracted markets: %d", len(all_rows))
    return all_rows

def main() -> None:
    events = find_upcoming_games()
    if not events:
        logger.info("No events found, stopping.")
        return
    all_extracted = fetch_and_process_event_data(events)
    if not all_extracted:
        logger.info("No new data to check, stopping.")
        return

    logger.info(f'Parsed upcoming events and saved: {outbound_odds}')
    df_events = pd.DataFrame(all_extracted_data)
    df_events.to_csv(outbound_odds, index=False)
    uniquedates = df_events['OpenDate'].unique()
    formatted_dates = [date.strftime('%Y-%m-%d %H:%M:%S') for date in uniquedates]
    formatted_dates_str = ','.join([f"'{date}'" for date in formatted_dates])
    engine = create_engine(f'mysql+mysqlconnector://{mys_us}:{mys_ps}@{mys_sv}')
    sql_query = f"""SELECT * FROM {SPORTSBOOK_ODDS_TABLE} WHERE OpenDate IN ({formatted_dates_str})"""
    database_df = pd.read_sql_query(sql_query, engine)
    if len(database_df) > 0:
        merge_columns = ['EventName', 'BetName', 'BetType', 'OverValue', 'UnderValue']
        check_df = pd.merge(df_events, database_df[merge_columns], on=merge_columns, how='left', indicator=True)
        merged_df = check_df[check_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    else:
        merged_df = df_events.copy()

    new_record_count = len(merged_df)
    if new_record_count > 0:
        logger.info(f'Parsed data {new_record_count} new records')
        merged_df.to_csv(outbound_odds, index=False)
        merged_df.to_sql(SPORTSBOOK_ODDS_TABLE, con=engine, if_exists='append', index=False)
        logger.info(f'Synced sportsbook_odds {new_record_count} records...')

    engine.dispose()
    logger.info('Cleanup sync files...')
    if os.path.exists(outbound_odds):
        os.remove(outbound_odds)

    logger.info('Finished...')
    logger.info('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')






if __name__ == "__main__":
    main()