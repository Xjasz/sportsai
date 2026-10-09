
from builders import playerdetail_builder as pdb  # noqa: F401
from nba_api.stats.endpoints import commonallplayers  # noqa: F401
import pandas as pd
from pathlib import Path

# PLAYER ONLY
# player_id = '1628368'
# player_name = 'DeAaron Fox'
# current_all_players = commonallplayers.CommonAllPlayers(is_only_current_season='0', league_id='00').get_data_frames()[0]
# pdb.fetch_and_save_player_detail((player_id, player_name), current_all_players)
# print('end')

players = [
    ("1630585", "Marcus_Garrett", "GUARD"),
    ("1627746", "Skal_Labissiere", "FORWARD-CENTER"),
    ("1626158", "Richaun_Holmes", "FORWARD"),
    ("1627732", "Ben_Simmons", "GUARD-FORWARD"),
    ("1628365", "Markelle_Fultz", "GUARD"),
    ("1628964", "Mo_Bamba", "CENTER"),
    ("1628970", "Miles_Bridges", "FORWARD"),
    ("1628995", "Kevin_Knox_II", "FORWARD"),
    ("1629022", "Lonnie_Walker_IV", "GUARD-FORWARD"),
    ("1629052", "Oshae_Brissett", "FORWARD-GUARD"),
    ("1629056", "Terence_Davis", "GUARD"),
    ("1629650", "Moses_Brown", "CENTER"),
    ("1630165", "Killian_Hayes", "GUARD"),
    ("1630201", "Malachi_Flynn", "GUARD"),
    ("1630205", "Lamar_Stevens", "FORWARD"),
    ("1630231", "KJ_Martin", "FORWARD"),
    ("1630264", "Anthony_Gill", "FORWARD"),
    ("1630296", "Braxton_Key", "FORWARD"),
    ("1630531", "Jaden_Springer", "GUARD"),
    ("1630539", "Kai_Jones", "CENTER-FORWARD"),
    ("1630542", "Marcus_Bagley", "FORWARD"),
    ("1630600", "Isaiah_Mobley", "FORWARD"),
    ("1630658", "Colin_Castleton", "CENTER"),
    ("1630678", "Terry_Taylor", "FORWARD"),
    ("1630762", "Phillip_Wheeler", "FORWARD"),
    ("1631111", "Wendell_Moore_Jr", "GUARD"),
    ("1631116", "Patrick_Baldwin_Jr", "FORWARD"),
    ("1631123", "Jamaree_Bouyea", "GUARD"),
    ("1631166", "Drew_Timme", "FORWARD"),
    ("1631197", "Jared_Rhoden", "GUARD"),
    ("1631223", "David_Roddy", "FORWARD"),
    ("1631250", "Pete_Nance", "FORWARD"),
    ("1631301", "Jaylen_Sims", "GUARD"),
    ("1631306", "Cole_Swider", "FORWARD"),
    ("1631311", "Lester_Quinones", "GUARD"),
    ("1641720", "Jalen_Hood-Schifino", "GUARD"),
    ("1641732", "Colby_Jones", "GUARD"),
    ("1641755", "Kevin_McCullar_Jr", "GUARD"),
    ("1641774", "Tristan_Vukcevic", "FORWARD"),
    ("1641801", "Emanuel_Miller", "FORWARD"),
    ("1641817", "Anton_Watson", "FORWARD"),
    ("1641879", "Yuri_Collins", "GUARD"),
    ("1641936", "Miles_Norris", "FORWARD"),
    ("1642024", "Alex_Reese", "FORWARD-CENTER"),
    ("1642267", "Bub_Carrington", "GUARD"),
    ("1642275", "Tidjane_Salaun", "FORWARD"),
    ("200782", "PJ_Tucker", "FORWARD"),
    ("202699", "Tobias_Harris", "FORWARD"),
    ("203458", "Alex_Len", "CENTER"),
    ("203995", "Vasilije_Micic", "GUARD"),
]



# player_id = '1628470'
directory = Path("data/game/2024")  # relative path from script location
total_count = 0

for player_id, player_name, position in players:
    if directory.exists():
        fix_count = 0
        for file_path in directory.glob("*.csv"):
            if file_path.read_text(errors="ignore").find(player_id) != -1:
                df = pd.read_csv(file_path, low_memory=False)
                invalid_mask = df["COMMENT"].isna() & (df["POSITION"].isna() | (df["POSITION"].astype(str).str.strip() == ""))
                if invalid_mask.any():
                    mask = df["PLAYER_ID"].astype(str) == player_id
                    if mask.any():
                        fix_count = fix_count + 1
                        total_count = total_count + 1
                        df.loc[mask, "POSITION"] = position
                        df.to_csv(file_path, index=False)
    print(f'Fixed {player_name} count = {fix_count}')
print(f'Total Fixes {total_count}')

# if directory.exists():
#     for file_path in directory.glob("*.csv"):
#         if file_path.read_text(errors="ignore").find(player_id) != -1:
#             df = pd.read_csv(file_path, low_memory=False)
#             mask = df["PLAYER_ID"].astype(str) == player_id
#             if mask.any():
#                 df.loc[mask, "POSITION"] = "FORWARD"
#                 df.to_csv(file_path, index=False)
# else:
#     print(f"Directory not found: {directory.resolve()}")

print('end')