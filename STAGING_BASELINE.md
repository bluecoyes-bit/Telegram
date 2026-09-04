# STAGING_BASELINE

Freeze/checkpoint for the FINAL STAGING VALIDATION pass.
Created: 2026-09-04 | Branch: `p1-final-architecture-pass`

## Tree cleanliness — HONEST RECORD

The working tree is **NOT clean**. It contains the uncommitted cumulative work from the prior
architecture pass (all deliverables present as untracked files, and several runtime files modified
vs HEAD). This is expected and was already the case at the start of validation. The `git tag`
below therefore points to the last commit, not the working tree.

| Item | Value |
|------|-------|
| git tag | `staging-candidate-1` |
| tag points to commit | `6c678ba43bd5d8fcbfa4d9542dda2db97cd80b2a` ("kilo code") |
| branch | `p1-final-architecture-pass` |
| working-tree entries (changed+untracked) | 33 |
| Note | candidate code = tagged commit + uncommitted working-tree changes (Sections 2+). |

## Runtime environment

- Python: **3.11.9**
- OS: **Windows 10** (win32, Microsoft Windows NT 10.0)
- Git: POSIX bash **not** available (PowerShell only).

### Installed package versions
| Package | Version |
|---------|---------|
| telethon | 1.44.0 |
| pymongo | 4.17.0 |
| fastapi | 0.141.1 |
| uvicorn | 0.52.4 |
| aiohttp | 3.14.3 |
| pydantic | 2.13.5 |
| pytest | 9.1.1 |
| pytest-asyncio | 1.4.0 |
| python-dotenv | 1.2.3 |
| pytgcalls | 3.0.0.dev24 |

## SHA-256 — runtime Python files
| File | SHA-256 |
|------|---------|
| account_lease_manager.py | 7A8258A403B6C6A6345812F86BC275FD2D49C1E2CF3200ED2F8AA39E71210DCE |
| adder.py | 1568DBE9787DA65D9015E4854A628FF88EF0B7911A5C9C8A18E5CDF9F7232C4D |
| config.py | 10E65270BCCB149A981EFF97BAD0D4F7BE3FF70699710DBC7DB2C5CD14EABDD7 |
| database.py | D4A0E85B183E4FF9568DEDE66AABF461A86973871F95EC5B48FCCC7498BB485D |
| dmsender.py | 67621387F29A5841EB1759682F0BF8E7C2DE33A3E7775FC90FADCF188669364F |
| exception_classifier.py | 715C58240402996E87E9055E127DF688F746D18F72CCA62136D2225C2894E327 |
| main_bot.py | 37BB8B16146B54B0FD3D0AE223F5740D249233E27E0D98BB8D794389918B32CC |
| proxy_manager.py | 61A8BA9BDE5DDE410689D24ADD0819597EF60B5C10D1124560933465EACA0C34 |
| scraper.py | E7F33E48B95190A67844ACD5F147E24A06DFA8E281936399E7E4C0A2DCB7F20D |
| session_manager.py | BF1E31335819F14074BEE15832529775E29BAA134196ECE0E0F0DD1AE6AAAB04 |
| session_migration.py | 058A7104AFD5EB4B52F3F2D43351DD259E5B3C953999BA74C94C6C3EB4C07269 |
| videochat.py | 8D1BC79CFFCECD67765FC4496872565D1B610C0AB05CE9B1837D4C8D5259674A |
| web_console.py | 306A3CBF666C9AD0152B9F84125400CFA0F97AF3A31E4EA4A7C26C774525D3B8 |

## SHA-256 — test files
| File | SHA-256 |
|------|---------|
| test_p0_lifecycle.py | 260EB4FB91B687D0D16942FBEEC748338B545E161EA783A545A0F3C6FDCCABCF |
| test_p0_closeout.py | 7A368E8F6DC81AD10520F8EF1C50B4E6F6668D891753A71F2DFB09108C25B8FD |
| test_p1_security.py | 5E49494B56816209AB9DE06C95215014F331FCD74BB1299F1F92376A83EB8201 |

## Environment constraints (UNVERIFIED — LIVE EXTERNAL DEPENDENCY)
A headless sandbox: **no live Telegram session, no live MongoDB, no live proxy egress, no real
PyTgCalls**, and **no `.env`/credentials configured**. Therefore live-dependent gates (real Mongo
contention, real Telethon connect/auth, real proxy egress identity, real voice stream) cannot be
executed here and are explicitly marked `UNVERIFIED — LIVE EXTERNAL DEPENDENCY` in this validation.
All applicable in-process tests use the existing unit/integration mocks and never touch
production data.
