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
- Parameterized SQL only. Delete dead code cleanly, no compatibility shims.

## Git / PRs
- Never commit to master. One branch per task, one PR per concern.
- Few substantial commits, not many tiny ones. Stage files by name, never `git add -A`.
- Before commit: `ruff check --select F,E9 .` clean and `python -m py_compile` on touched files.
- PR body: `What` / `Why` / `Verified` as bullets. Say "lint clean" unless a runtime artifact proves "tested".

## Agents
- Max 5 agents per session. Each gets a role, a file scope and hard rules. Workers run on Sonnet.
