# CORE PATCH #8 — MAIN_BOT ORCHESTRATION: SESSIONMANAGER-ONLY LIFECYCLE

## OBJECTIVE

Harden `main_bot.py` orchestration on top of the already-validated
`SessionManager` / `ProxyLeaseManager` lifecycle: auditor, auto-recovery,
login/OTP/2FA, background-task ownership, global login state, and shutdown all
consume the public lifecycle **only** — no direct client construction, no
`release_lease()`/`release_proxy()` outside a manage context, no second client
pool, no new locks.

Only `main_bot.py` was modified. `session_manager.py`, `proxy_manager.py`,
`adder.py`, `dmsender.py`, `videochat.py`, `web_console.py` and
`database.py` are **not** modified. No redesign, no second client pool, no new
lock.

---

## AUDIT FINDINGS (pre-change)

| # | Issue | Resolution |
|---|-------|-----------|
| 1 | Auditor ran an unbounded pass per account, sorting with an unconditional `shuffle` (non-deterministic LRU) | Deterministic `sort(key=_normalize_check_time)`; bounded pass via `_auditor_run_pass` with an `asyncio.Semaphore`, human-like delay retained |
| 2 | Auditor could fabricate one client per DB account regardless of proxy capacity | `_auditor_effective_capacity = min(eligible, network, configured)`; `_auditor_network_capacity` returns `0` on any pool error → cycle skipped, zero clients created |
| 3 | Recovery path had no ownership/eligibility guards and lived outside `SessionManager` | Busy/owned/terminal guards + `async with managed_client(acc)`; `SessionAlreadyOwnedError` at acquire → skip, never classified as account failure |
| 4 | `shared_login_process` constructed/touched clients outside SessionManager | Strict loop over `build_login_client()` under the caller's login reservation; `release_login()` + `reserve_login()` on each failed attempt; 4-attempt strict proxy policy, no real-IP fallback, no client reconnect |
| 5 | Login/OTP clients could be disconnected directly (bypassing the lease) | All 6 production call sites now use `managed_client()`; `managed_client` only acquires + yields `lease.client`, never disconnects |
| 6 | Background engines spawned as anonymous fire-and-forget tasks | Every engine runs inside `_run_with_bounded_restarts` (registered in `GLOBAL.background_tasks`, restart-limited, cancellation-propagating); `_cancel_tracked_background_tasks` awaited during shutdown |
| 7 | Shutdown ordering had no single deterministic path; double-shutdown unguarded | Ordered + guarded shutdown in `lifespan`: `disconnect_all` → proxy stop → account stop → db close → bot disconnect, with `_shutdown_done` idempotence guard |
| 8 | `GlobalState` replaced an auth state without releasing the old login reservation | `cleanup_stale_auth_states` releases stale login reservations via `session_manager.release_login`; `set_auth_state` replaces in place without ever disconnecting the old client |
| 9 | 2FA handler could persist the plaintext password and disconnect/`finally` the lease client | Verified: password passed as `two_fa_password=None`; no `client.disconnect(` / `finally:` in the 2FA body |

---

## CHANGES IN `main_bot.py`

### Auditor (`2000s` block, `continuous_session_auditor` at 2305)
- `should_start_auditor()` (2210) gates on `CONFIG.AUDITOR_ENABLED`, default **on**.
- `_normalize_check_time` (2220) parses `last_checked_time`/`last_updated`
  (int/float/str, malformed→0) so the LRU ordering is deterministic.
- `_auditor_network_capacity` (2235): `proxy_lease_manager.get_available_count()`;
  any exception → `0` (failsafe, no client ever fabricated).
- `_auditor_effective_capacity` (2247): `min(≈eligible, ≈network, configured)`.
- `_auditor_run_pass` (2267): capacity `<= 0` → `(0, 0, len(accounts))` with
  zero clients; otherwise bounded `asyncio.Semaphore(max(1, capacity))`, each
  worker sleeps `uniform(10.0, 20.0)` (anti-spam) and classifies
  `SessionAlreadyOwnedError` → busy (skipped), other exceptions → failed.
- Batch loop `sort`s LRU-authoritative (no shuffle) and derives capacity once
  per cycle.

### Auto-recovery (`_recover_failed_accounts` at 2773, loop at 2835)
- `should_start_recovery()` (2215) gates on `CONFIG.ENABLE_AUTO_RECOVERY`,
  default **on**.
- Guards, in order: missing session → skip; terminal status (`TERMINAL_DB_STATUSES`)
  → skip forever; `account_lease_manager.is_busy` → skip; `session_manager.is_owned`
  → skip.
- Recovery runs entirely inside `async with managed_client(acc)` (→
  `session_manager.acquire(phone, module="managed_client", worker_id="managed_client", auto_release=True)`).
  `SessionAlreadyOwnedError` at acquire → documented skip (never an account failure);
  any other exception → logged, no escalation.
- Never constructs or disconnects a client directly.

### Login / OTP / 2FA
- `shared_login_process` (543): per-attempt `build_login_client(clean_phone, owner,
  session_str=new StringSession, api_id, api_hash, device, proxy=None)`; a `None`
  build → release + reserve + retry; proxy-request failure → `release_login` +
  re-`reserve_login` + rotate; returns `code_sent` with `code_hash`, `client`,
  `proxy_used` (`_login_proxy_label`, 529); exhaustion → strict-policy exception.
  Never reconnects a failed client (a fresh lease replaces it); never disconnects.
- `ensure_otp_listener` (498): idempotent via `_otp_registered` key —
  exactly one handler per phone.
- `fetch_past_otps` (514): logs each message, tolerates errors silently.
- 2FA handler (`verify_2fa_handler`): password passed to
  `save_authorized_session(..., two_fa_password=None)`; no store of plaintext;
  the client is a `managed_client()` lease (no `disconnect`, no `finally`).

### Background-task ownership
- `_cancel_tracked_background_tasks` (2600): cancels everything in
  `GLOBAL.background_tasks`, drains with suppression, clears the set.
- `_run_with_bounded_restarts` (2612): runs a factory inside the tracked set,
  restarts on exception up to `max_restarts` with backoff, re-raises on
  `CancelledError` so the tracked task can finish, drops the tracking entry on exit.
- Registrar at startup (2673/2684): auditor + recovery each run inside
  `_run_with_bounded_restarts`; updater (2061) also registered. Pre-existing
  gathered fan-out tasks (1077 `_ui_scan_worker`, 1798 `scan_and_recover`) stay
  `gather`-ed — never fire-and-forget.
- `bot = BotProxy()` (353) is the single Telegram bot client (unchanged).

### Global login state (class `AuthState` 132 / `GlobalState` 147)
- `set_auth_state` replaces the entry in place; the displaced client is never
  disconnected here (disconnection belongs to the originating lease).
- `cleanup_stale_auth_states` releases stale login reservations through
  `session_manager.release_login(phone, "login:<phone>")` — the displaced client
  is left untouched (its lease owns cleanup).

### Shutdown (`lifespan` at 2651)
- Startup: reset `_shutdown_done`, seed engine config, initialize bot, start the
  three ordered lifecycle managers; auditor/recovery run only when their gates
  are enabled (default on).
- Shutdown order: (1) `GLOBAL.shutdown_flag` + `_cancel_tracked_background_tasks`,
  (2) `session_manager.disconnect_all()`, (3) `proxy_manager.stop_background_testing`
  + `proxy_lease_manager.stop()`, (4) `account_lease_manager.stop()`,
  (5) `db.close()`, (6) bot `disconnect()`, all `await`-ed, exceptions contained.
- Idempotence: `if _shutdown_done: return` guard + early `yield` plus
  `_shutdown_done = True` — a completed shutdown is safe to re-enter.

---

## NEW TEST FILE `test_main_bot_lifecycle.py` (34 tests)

| Area | Tests | Coverage |
|------|-------|----------|
| Auditor | 9 | startup default-on / disabled; `_auditor_effective_capacity` bounded by eligible / network / configured / zero; network failsafe→0; `_normalize_check_time` mixed types; `_auditor_run_pass` skip-on-zero-capacity + bounded concurrency with mixed ok/failed outcomes |
| Recovery | 7 | startup default-on / disabled; terminal / busy / owned skips; recovery via `managed_client` (acquire delegation, ACTIVE status, zero direct disconnect); owned-at-acquire skip |
| Login lifecycle | 8 | `managed_client` raises on no lease; yields lease client; `shared_login_process` code-sent path (code_hash, owner, proxy label, pending row, no release); proxy rotation on failure; exhaustion release/max-4 rule; OTP listener idempotent; OTP handler capture; past-OTP log + error tolerance |
| Background/Lifespan | 5 | bounded restarts on error; stop-at-limit; cancel propagation; tracked-task cancellation; ordered + guarded shutdown |
| GlobalState | 2 | in-place replacement without disconnect; stale cleanup via `release_login` without disconnect |
| Source/Auth | 2 | static audit (no pool/no private tokens/2FA hygiene); `check_session_authorization` status matrix (authorized / unauthorized / revoked / timeout / connection_error / unknown / disconnected) |
| Import | 1 | imports without a live MongoDB |

---

## STATIC + RUNTIME VERIFICATION

```text
Application logic is inline (no patch)                    -> OK (verified 5 of 6 fixtures)
python -m pytest test_main_bot_lifecycle.py -q -W error::RuntimeWarning
                                                          -> 34 passed, 0 warnings
python -m compileall -q main_bot.py                       -> COMPILE_OK
neutralized import (SuiteDatabase._init_mongo patched)    -> MAINBOT_IMPORT_OK
```

Static token scan (Select-String / source read on `main_bot.py`):

```text
use_pool / client_pool / ClientPoolEntry / _pool_lock /
session_manager._ / release_lease( / create_authenticated_client /
_no_proxy / proxy_lease_manager.release_proxy(             -> 0 matches
asyncio.create_task(                                       -> 6 sites, all owned:
                                                           353 BotProxy bot (single bot client)
                                                           1077 gathered fan-out (UI scan)
                                                           1798 gathered fan-out (scan_and_recover)
                                                           2061 updater   (registered)
                                                           2673 auditor   (registered via _run_with_bounded_restarts)
                                                           2684 recovery  (registered via _run_with_bounded_restarts)
session_manager.acquire in managed_client (482-485)       -> auto_release=True,
                                                           module="managed_client",
                                                           worker_id="managed_client"
managed_client(...) call sites                             -> 6, all "async with":
                                                           1066, 1623, 1779, 1994, 2488, 2812
```

### Suite matrix (all expected 0 failures)

```text
test_p0_closeout.py          43 passed
test_p0_lifecycle.py         34 passed
test_proxy_lease.py           9 passed
test_adder_lifecycle.py      15 passed
test_dmsender_lifecycle.py   22 passed
test_videochat_lifecycle.py  13 passed
test_main_bot_lifecycle.py   34 passed
------------------------------------------------
TOTAL                       170 passed
```

`test_p0_closeout.py` is additionally green under
`-W error::RuntimeWarning` (43 passed, 0 warnings).

---

## OBSERVATIONS / DECISIONS DURING VERIFICATION

- **`_auditor_run_pass`, `_recover_failed_accounts`, `shared_login_process`,
  `lifespan`, `set_auth_state`, `cleanup_stale_auth_states` are all `async`.**
  The new tests drive them with `@pytest.mark.asyncio` (strict mode); no
  `asyncio.run` is used inside async tests.
- **Login exhaustion exception text** is
  `"Proxy Connection Failed! …"` (strict 4-attempt policy, no real-IP leak);
  the audit doc/tests assert on that user-facing string plus the internal
  `log.error` "Strict Proxy Policy" marker.
- **Shared `random` module**: `_auditor_run_pass` uses `random.uniform` from the
  module namespace, so the bounded test monkeypatches `main_bot.random.uniform`
  to `0.0` to neutralize the human-like delay without touching application code.
- **Lifespan idempotence guard semantics**: `_shutdown_done` is reset at each
  startup (module-global), so the guard protects a *completed* shutdown from
  re-running side effects; the test arms the flag and verifies zero extra
  side effects (no second `disconnect_all` / `db.close`).
- **`GlobalState` never disconnects a client** — lifecycle cleanup stays with the
  originating SessionManager lease. The old client from a replaced/stale auth
  state is released via `release_login`, never killed directly.
- No live-network smoke test was performed. The repository is **not** hereby
  declared production-ready.

## RELATED FILES

- `main_bot.py` — modified (only PATCH #8 target).
- `test_main_bot_lifecycle.py` — new (34 tests).
- Baselines in `test_p0_closeout.py`, `test_p0_lifecycle.py`,
  `test_proxy_lease.py`, `test_adder_lifecycle.py`,
  `test_dmsender_lifecycle.py`, `test_videochat_lifecycle.py` — unchanged, green.

## PATCH STATUS: COMPLETE