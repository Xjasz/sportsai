from nba_api.stats.endpoints import commonallplayers
import pandas as pd
from pathlib import Path
from builders import playerdetail_builder as pdb

directory = Path("data/game/2025")
totalcount = 0
players = set()
print('Starting...')

if directory.exists():
    for file_path in directory.glob("*.csv"):
        df = pd.read_csv(file_path, low_memory=False)
        mask = df["COMMENT"].isna() & (df["POSITION"].isna() | (df["POSITION"].astype(str).str.strip() == ""))
        for _, row in df.loc[mask, ["PLAYER_ID", "PLAYER_NAME"]].dropna().iterrows():
            players.add((str(row["PLAYER_ID"]), str(row["PLAYER_NAME"])))
            print(f'File: {file_path} ---> Unknown PlayerId {str(row["PLAYER_ID"])} Player {str(row["PLAYER_NAME"])}')
            totalcount +=1
else:
    print(f"Directory not found: {directory.resolve()}")
    exit()

print(f'Players to fix {totalcount}')
current_all_players = commonallplayers.CommonAllPlayers(is_only_current_season='1', league_id='00').get_data_frames()[0]

for player_id, player_name in sorted(players):
    pdb.fetch_and_save_player_detail((player_id, player_name), current_all_players, force_update=True)


print('Finished...')