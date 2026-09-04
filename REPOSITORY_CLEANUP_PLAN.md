# Repository Cleanup Plan

## 1. Scope

Repository cleanup/maintenance pass only. No application code changes,
no logic refactoring, no concurrency/config/secret changes.

Working directory: `D:\Bot-master-main` (branch `p1-final-architecture-pass`).

## 2. Inventory summary

### A. Protected runtime modules (DO NOT DELETE / DO NOT MODIFY)
```
main_bot.py
config.py
database.py
proxy_manager.py
session_manager.py
account_lease_manager.py
exception_classifier.py
adder.py
dmsender.py
scraper.py
videochat.py
web_console.py
session_migration.py
```
All 13 confirmed present.

### B. Protected tests (DO NOT DELETE / DO NOT MODIFY)
```
test_p0_lifecycle.py
test_p0_closeout.py
test_p1_security.py
```
All 3 confirmed present.

### C. Other tracked, required files
- `requirements.txt`        — deployment dependencies (KEEP)
- `web_view/index.html`, `web_view/script.js`, `web_view/style.css` — web_console frontend assets (runtime; KEEP)
- `start_bot.sh`            — deployment/run script (KEEP)
- `.gitignore`              — VCS hygiene (KEEP)
- `silent.mp3`              — runtime WebRTC voice-chat audio asset, referenced by
                               `videochat.py:646` and `main_bot.py:2231` (KEEP)
- `master_control_suite.session` (28672 bytes) — Telethon session file
                               (account/session data; KEEP, PROTECTED)

### D. Runtime config / account data (KEEP, PROTECTED — referenced by code)
- `proxies.txt`  — empty proxy pool file; default proxy source in
                    `proxy_manager.py:178,194,599,698` (KEEP)
- `vars.txt`     — empty account vars file; parsed by `database.py:1066`
                    and `session_migration.py:4,39,80` + referenced in
                    `main_bot.py:1707` and `database.py:1046,1144` (KEEP)
- `.env`         — environment secrets; gitignored, untracked (KEEP, PROTECTED)

### E. Generated artifacts (DELETE)
- `__pycache__/` — Python bytecode cache
- `.pytest_cache/` — pytest cache

### F. Junk / temporary files (DELETE)
- `=2.4.0` — contains stray pip stdout
              ("Requirement already satisfied: python-socks[asyncio] ... (2.8.2)");
              filename is a shell/pip artifact; referenced by NO code (DELETE)

### G. Historical audit / baseline reports (ARCHIVE to `docs/archive/`)
- `AUDIT_BEFORE_FIX.md`
- `CONFIRMED_VS_SUSPECTED.md`
- `P0_CLOSEOUT_REPORT.md`
- `P0_FIX_REPORT.md`
- `PRODUCTION_BASELINE.md`
- `STAGING_BASELINE.md`

### H. Final operational documents (KEEP at repo root)
- `FINAL_ARCHITECTURE.md`
- `FINAL_RESOURCE_INVARIANTS.md`
- `FINAL_TEST_REPORT.md`
- `FINAL_CHANGELOG.md`
- `PRODUCTION_READINESS.md`
- `FINAL_CANDIDATE_FREEZE.md`  (final-state doc; kept)

### I. Duplicate / old-version source files
A repository-wide search found NO `*_old.py`, `*_backup.py`, `*-final.py`,
`*-new.py`, `*(1).py` variants for any runtime module. None to archive.

## 3. Reference scan performed

Searched every `.py` source for references to: `=2.4.0`, `vars.txt`,
`proxies.txt`, `silent.mp3`, `master_control_suite.session`, and each
historical/final `.md` document.

- Only the runtime-config/data files noted above are referenced.
- No source references any `.md` document, so archiving docs is safe.
- `=2.4.0` has zero references — confirmed junk.

## 4. Deletion / archival plan

| Path | Category | Reason | Safe to delete? | Used by runtime? |
| ---- | -------- | ------ | --------------- | ---------------- |
| `__pycache__/` | F (generated) | Python bytecode cache | DELETE | No |
| `.pytest_cache/` | F (generated) | pytest cache (not in root .gitignore) | DELETE | No |
| `=2.4.0` | G (junk) | stray pip stdout misfiled as a filename; 0 refs | DELETE | No |
| `AUDIT_BEFORE_FIX.md` | Archive | historical audit | move→`docs/archive/` | No |
| `CONFIRMED_VS_SUSPECTED.md` | Archive | historical audit | move→`docs/archive/` | No |
| `P0_CLOSEOUT_REPORT.md` | Archive | historical report | move→`docs/archive/` | No |
| `P0_FIX_REPORT.md` | Archive | historical report | move→`docs/archive/` | No |
| `PRODUCTION_BASELINE.md` | Archive | historical baseline | move→`docs/archive/` | No |
| `STAGING_BASELINE.md` | Archive | historical baseline | move→`docs/archive/` | No |

## 5. Git safety

- Checkpoint tag `before-repository-cleanup` will be created at HEAD before any change.
- No history rewrite, no amend, no push.
- No commit will be made unless explicitly requested.
- `.gitignore` will gain `.pytest_cache/` (hygiene only; section 6 requirement).
- Generated/ignored files (`__pycache__`, `.env`) are NOT deleted merely for being
  "generated-looking": `.env` is secrets (keep), `__pycache__`/`.pytest_cache` are
  confirmed pure caches (delete).

## 6. Verification steps (run after cleanup)

1. `python -m compileall .`
2. `python -c "import main_bot; print('IMPORT_OK')"`
3. `pytest -q`
4. Re-confirm the 13 runtime modules + 3 tests still exist.

## 7. Uncertain items (none expected)

If any file cannot be confidently classified, it is KEPT and listed in
`FINAL_REPOSITORY_CLEANUP.md` rather than deleted.
