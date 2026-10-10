# sportsai - rules for Claude sessions

## Output
- Be direct. No preamble, no filler, no restating the ask. Lead with the result.
- PR titles, PR bodies and commit messages: short bullets, one line per change.
- Code comments only when the logic is non-obvious, max 2 lines. No docstrings on obvious functions. No TODOs.

## Code
- Python 3.12 compatible. Lines up to 150 chars.
- Clean and short. No speculative abstractions, no helper for one-time use, no future-proofing, no feature flags.
- Validate at boundaries (nba_api, scraped pages, DB, files). Trust internal code after that.
- Assume large data. No needless full-file reloads. Prefer vectorized pandas over iterrows when output stays identical.
- Config lives in `globals/` (`global_settings.py`, `run_settings.py`, `config.ini`). Never hardcode paths or seasons elsewhere.
- Parameterized SQL only. No compatibility shims.
- Do not remove the owner's code (functions, constants, flags, commented blocks, config keys, trace prints) without asking, even with
  zero callers. Allowed on your own: debug leftovers keyed on literal IDs, no-op statements, exact-duplicate consolidation, and
  intermediate rewrites of a file whose final content is unchanged (say so in the PR).
- Model and training logic (`generate_ai.py` ranges, features, splits, scoring thresholds, `LOADED_STATES`) is the owner's: report findings
  as questions, implement only as separate opt-in changes the owner approved first.
- `active/odds_service.py` is standalone by design; leave its cookie header and constants alone.
- Keep line endings as found: `generate_view.py`, `globals/global_utils.py`, `webevents/get_latest_events.py`, `requirements.txt` and
  `player_add.py` are CRLF; edit them byte-wise.

## Git / PRs
- Never commit to master. Branches start with `feature/`.
- One branch, one commit, one PR per session; amend and force-push that branch for follow-ups. Open questions go in the chat reply, not in extra PRs.
- Stage files by name, never `git add -A`.
- Before commit: `ruff check --select F,E9 .` clean and `python -m py_compile` on touched files.
- PR body: `What` / `Why` / `Verified` as bullets. Say "lint clean" unless a runtime artifact proves "tested".

## Season state
- `rns.prediction_season` is the newest `data/game/<year>` folder, fixed at import: the run that writes the first game file of a new
  season still processes the previous season; the next run picks up the new folder.
- At a season start the owner sets the season by hand (`rns.prediction_season`, `globals/config.ini` `latest_gamedate`). Game files come only
  from nba_api and are never copied or hand-edited. Tonight's prediction rows are built from each team's last game file in the
  prediction-season folder (`prediction_builder.process_directory`), then injuries, lineups and officials are applied on top.
- Never assume the current season has data, that `data/season/<year>.csv` exists, or that `data/all_partials.csv` is fresh; ask before
  touching season handling, roster assembly or the START_POSITION count checks.
- Early-season paths that raise today and are left as-is: `prediction_builder.process_directory` on an empty folder (`max(files)` is None) or when
  a team has no game in that folder yet; `generate_data.combine_games_to_season` on an empty folder (`KeyError 'WINS'`).
- `ai_builder.create_all_logs` rebuilds `all_logs.csv` from every season file on each run and does not write `all_partials.csv` until
  `data/season/<year>.csv` exists; `remove_initial_games` drops the first 500 rows of each season.
- Player detail files gain a row for the new season only with `update_active_players_team=True` (refetch) or on a team change; without it
  the detail merge leaves POSITION NaN and `set_player_opponents` raises "Invalid Position Found".
- `player_fix.py` refetches details for players whose game rows have COMMENT NaN and POSITION NaN; `player_add.py` patches POSITION in
  game files from a hand-kept list. Both are part of the owner's manual routine; do not fold them into the pipeline unasked.
- Verify offline: extract `data/load/load_data.zip` into the scratchpad (never into `data/`), redirect `gls.*` paths like `smoke_test.py` does,
  never write under `data/` or to `globals/config.ini`. nba.com, ESPN, Rotowire and FanDuel are unreachable from cloud sessions.

## Agents
- Max 5 agents per session. Each gets a role, a file scope and hard rules. Workers run on Sonnet.
