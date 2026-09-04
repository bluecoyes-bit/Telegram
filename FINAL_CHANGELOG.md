# FINAL_CHANGELOG

Chronological change log for the final architecture pass (`p1-final-architecture-pass`), from the
`PRODUCTION_BASELINE.md` freeze (`6c678ba` "kilo code") to the current working tree. No commit was
made (none requested); all changes are uncommitted on the branch.

## Phase 0 — Baseline freeze
- Created branch `p1-final-architecture-pass` from `main` at `6c678ba`.
- Wrote `PRODUCTION_BASELINE.md` (package versions, 15-file SHA-256 table, 55-test baseline,
  preserved prior reports note).

## Phases 2–19 (carried into closeout, preserved)
- `SessionManager` established as the single session/client/lease owner; terminal-status gating as
  the first gate; `AuthKeyDuplicated` terminal policy; login/OTP/2FA unified ownership; proxy lease
  manager with single-owner keyword-style release; DM/adder resource-aware scheduling.
- **Phase 3.4:** added public `SessionManager.release_lease(lease)`; all feature modules switched
  from private `_release_lease` to the public method (`adder.py`, `web_console.py`, `videochat.py`,
  `main_bot.py`).
- **Phase 13:** removed hardcoded `num_workers = min(accounts, 20)`; added
  `compute_dm_worker_capacity()` in `dmsender.py`; effective_capacity launch + accurate
  `worker_started` event.
- **Phase 15:** adder worker pool made proxy-capacity-aware (no hardcoded 10).

## Phase 18 — Web console security (this pass)
- `config.py`: added `WEB_API_TOKEN` (env, empty default, Admin section).
- `web_console.py`:
  - Fixed **critical pre-existing import bug** (`asynccontextmanager` missing ⇒ app could not
    import/start). Added `from contextlib import asynccontextmanager`.
  - Added `_rate_limited()` (per-IP sliding window, 60 req/60 s, bounded bucket dict), public
    `ensure_api_token()` dependency, and `_background_tasks` registry + `shutdown_background_tasks()`.
  - Attached `Depends(ensure_api_token)` to `console_router` ⇒ every `/console/api/*` route requires
    `Authorization: Bearer` or `X-API-Token`; unset token ⇒ 503.
- `main_bot.py`: import `shutdown_background_tasks()`; call it in lifespan shutdown.

## Phase 20 — Executor offload for hot-path DB reads
- `session_manager.py` `acquire()` + `reserve_login()`: `get_session_by_phone()` →
  `await get_session_by_phone_async()`.
- `account_lease_manager.py` `is_eligible()` + `acquire()`: same async-variant routing.
- Test `FakeDB` mocks extended with `async def get_session_by_phone_async(...)` (lifecycle + closeout).

## Phase 22 — Task lifecycle
- `web_console.py` `POST /api/console/mass-execute`: `asyncio.create_task(...)` now tracked in
  `_background_tasks` with `done_callback` discard; cancelled/awaited on shutdown. Videochat
  teardown helpers reviewed — short self-terminating routines, acceptable fire-and-forget.

## Phase 23 — Exception policy
- Removed all 7 bare `except:` (narrowed to `except Exception`) — `videochat.py` (4) and
  `web_console.py` (3). `KeyboardInterrupt`/`SystemExit` no longer masked. Verified count = 0.

## Phase 28/29 — Tests & static checks
- New `test_p1_security.py` (8 tests) covering the auth gate, rate limiting, and the import
  regression guard for `web_console.py`.
- `compileall` exit 0 on all files; `import main_bot` verified; bare-except scan = 0;
  `compute_dm_worker_capacity` O(1) micro-bench recorded.
- Final suite: 74 passed, 0 failed.

## Deliverables (this pass)
`PRODUCTION_READINESS.md` (verdict + sections A–M), `FINAL_ARCHITECTURE.md`,
`FINAL_RESOURCE_INVARIANTS.md`, `FINAL_TEST_REPORT.md`, `FINAL_CHANGELOG.md`.

## Files changed this pass (delta vs baseline freeze)
`config.py`, `web_console.py`, `main_bot.py`, `session_manager.py`, `account_lease_manager.py`,
`videochat.py`, `test_p0_lifecycle.py`, `test_p0_closeout.py`, and new `test_p1_security.py`
(+ 5 deliverable `.md` files).
