# PRODUCTION_BASELINE

Freeze of the repository state BEFORE the final architecture pass begins. No source was
modified during this phase.

## Git

- Repository: `D:\Bot-master-main`
- Branch created for this pass: `p1-final-architecture-pass`
- Baseline commit (from `main` HEAD): `6c678ba43bd5d8fcbfa4d9542dda2db97cd80b2a` ("kilo code")
- Working tree at freeze: modified vs HEAD (all prior P0/P0-closeout work uncommitted):
  `adder.py`, `database.py`, `dmsender.py`, `main_bot.py`, `proxy_manager.py`, `scraper.py`,
  `session_manager.py`, `videochat.py`, `web_console.py` (tracked, modified); untracked:
  `AUDIT_BEFORE_FIX.md`, `CONFIRMED_VS_SUSPECTED.md`, `P0_CLOSEOUT_REPORT.md`, `P0_FIX_REPORT.md`,
  `session_migration.py`, `test_p0_closeout.py`, `test_p0_lifecycle.py`.

## Runtime / dependencies

- Python: **3.11.9**
- Key packages:
  - Telethon 1.44.0
  - pymongo 4.17.0
  - fastapi 0.141.1
  - uvicorn 0.52.4
  - aiohttp 3.14.3
  - pydantic 2.13.5
  - pytest 9.1.1
  - pytest-asyncio 1.4.0
  - python-dotenv 1.2.3

(No `motor` async Mongo driver detected; synchronous PyMongo is used — relevant to Phase 20.)

## Filenames & SHA-256 (at freeze)

| File | SHA-256 |
|------|---------|
| account_lease_manager.py | 5A7889EF65501B214C62D35E59BFCFD2A274620627DD10A9EE52F53C2D3DDE78 |
| adder.py | 82D6B086EF7A32D01703D4379C9F9FBD084F0B3BF0748B9FDA38B2CE0391DE3B |
| config.py | B953C1A6654DC0E572D021AFFF5F5BD736B1B02828C5BFFF94CE650339430278 |
| database.py | D4A0E85B183E4FF9568DEDE66AABF461A86973871F95EC5B48FCCC7498BB485D |
| dmsender.py | 10069B5D45797B61634DBBCD3987187C2E7B11E8EBBA609D8035AF602ACA5DB9 |
| exception_classifier.py | 715C58240402996E87E9055E127DF688F746D18F72CCA62136D2225C2894E327 |
| main_bot.py | 8690AD0D774B944C82A6D97974B074473D0C56A8C3A0A569D1635C853E2D8111 |
| proxy_manager.py | 61A8BA9BDE5DDE410689D24ADD0819597EF60B5C10D1124560933465EACA0C34 |
| scraper.py | E7F33E48B95190A67844ACD5F147E24A06DFA8E281936399E7E4C0A2DCB7F20D |
| session_manager.py | A5AB601F551EFBE9A5FD0C91920103E6AD2DFA6E9CAE1849432701DE471AD1E8 |
| session_migration.py | 058A7104AFD5EB4B52F3F2D43351DD259E5B3C953999BA74C94C6C3EB4C07269 |
| test_p0_closeout.py | A582A2524302D1FD8C955BD10450A468338D9D584200DDABF7B5E017856BEA69 |
| test_p0_lifecycle.py | 0BFE29B2F75ACF22BBC361FD5E952E366CBEE2D587B9A67C623C1AB6429BA95A |
| videochat.py | 199F235651E85C43BF1E8F0F35BC06DDFACD91C25A277416C9C229EE113BA44F |
| web_console.py | 7D9CD89A3E63DD0065EC61FD15FCFEECD74198ACD2E007673FE0653D33179D9D |

## Preserved prior reports

- `AUDIT_BEFORE_FIX.md` (preserved, per Phase 0 requirement)
- `CONFIRMED_VS_SUSPECTED.md`
- `P0_FIX_REPORT.md`
- `P0_CLOSEOUT_REPORT.md`

## Note on filename references

The task instructions refer to suffixed filenames (e.g. `main_bot(5).py`). Those are historical
download artifacts; the authoritative in-repo files are the unsuffixed versions listed above.
All work in this pass is performed on the unsuffixed files.

## Test baseline (at freeze)

```
55 passed, 21 warnings in 8.40s
if info and info.client and info.client.is_connected():   (RuntimeWarning: AsyncMock not awaited — test mocks)
```
