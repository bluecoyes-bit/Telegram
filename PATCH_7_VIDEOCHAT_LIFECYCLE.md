# CORE PATCH #7 — VIDEOCHAT LIFECYCLE: OWNERSHIP-ONLY STREAMING

## OBJECTIVE

Refactor `videochat.py` so `CloudVoiceChatEngine` consumes the validated
`SessionManager` / `ProxyLeaseManager` lifecycle **only** — every voice
operation runs inside `async with session_manager.acquire(...)`, with
deterministic task ownership and termination, explicit voice states, and a
strict static-ownership audit. No second `TelegramClient` pool, no proxy pool,
no account-lock mechanism. `session_manager.py`, `proxy_manager.py`,
`adder.py`, `dmsender.py`, `main_bot.py`, `web_console.py` and `database.py`
are **not** modified.

---

## AUDIT FINDINGS (pre-change)

| # | Issue | Location |
|---|-------|----------|
| 1 | Manual `client.__aenter__()` / `__aexit__()` instead of the acquire context | old `_run_stream` enter/exit |
| 2 | Manual `release_lease()` calls outside the acquire context | old teardown paths (x2) |
| 3 | Fire-and-forget `asyncio.create_task(_graceful_teardown())` — untracked, non-awaitable cleanup | old terminate path |
| 4 | `SessionManager.disconnect_all()` invoked from terminate — could kill other modules' sessions | old `terminate_voice_cluster` |
| 5 | No explicit voice state machine with timeouts (single boolean `is_running`) | engine-wide |
| 6 | Duplicate `self.scraper_helper = MemberScraper(...)` constructor line | `__init__` |
| 7 | `_active_calls` modeled as a list keyed by nothing; tasks not strongly tracked for teardown | engine-wide |
| 8 | PyTgCalls-missing fallback stub could fake production success | old stub branch |

---

## CHANGES IN `videochat.py`

### Ownership model
- **Long voice call** = the ENTIRE stream inside ONE
  `async with session_manager.acquire(phone, module="videochat_stream", worker_id=f"vc:{phone}", auto_release=True, timeout=VC_ACQUIRE_TIMEOUT)`.
  Session + client + proxy live only inside that context; `SessionManager`
  releases all of them deterministically on success, failure, or cancellation.
- **Short ops** (audit cleanup, migration) keep their `async with`, now with
  `worker_id=...` and `timeout=VC_BATCH_TIMEOUT`.
- **No** `__aenter__`/`__aexit__`, no manual `release_lease`/`release_proxy`,
  no `TelegramClient(...)` creation, no DB lock calls, no `client.disconnect()`.
- `terminate_voice_cluster` cancels every registered stream task and awaits
  `asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True),
  VC_TEARDOWN_TIMEOUT)`. It does **not** call `SessionManager.disconnect_all()`
  (other modules' sessions must stay untouched).

### Explicit voice state machine
`VoiceClusterState`: `IDLE / RESERVING / CONNECTING / READY / STARTING_CALL /
ACTIVE / STOPPING / STOPPED / FAILED / TERMINAL / VIDEO_ENGINE_UNAVAILABLE`.
- `RESERVING` before acquire, `CONNECTING/READY/STARTING_CALL/ACTIVE` inside
  the lease, `STOPPED` on clean exit, `STOPPING` only as a transient on cancel,
  `FAILED` on retryable failures (with replacement spawn), `TERMINAL` on
  terminal session errors (quarantine via `SessionManager` + `mark_account_failed`,
  **no** replacement spawn), `VIDEO_ENGINE_UNAVAILABLE` when PyTgCalls is missing.
- Cancellation result in a final `STOPPED` (the `_run_stream` `finally` never
  overwrites `FAILED`/`TERMINAL`/`VIDEO_ENGINE_UNAVAILABLE`).

### Task ownership
- Every `asyncio.create_task` is owned: migration tasks are `gather`-ed; the
  replacement spawn and launch spawns go through `_register_voice_task(phone, task)`
  (strong `_cluster_tasks` dict + `_active_tasks` set + done-callbacks that pop
  the phone key). Takeover-guard handler only `set()`s an `asyncio.Event`.
- New engine-level guard in `_execute_single_stream`: if the phone already has a
  registered cluster task that is a *different* live task, the concurrent start
  is rejected **before** touching any session/proxy resource (no second client
  can ever be fabricated; session-manager rollback can never disturb the
  running lease). A task's own registered entry passes.

### Bounded timeouts
`VC_ACQUIRE_TIMEOUT 30` · `VC_CONNECT_TIMEOUT 20` · `VC_SETUP_TIMEOUT 40` ·
`VC_ENGINE_START_TIMEOUT 30` · `VC_ENGINE_PLAY_TIMEOUT 30` ·
`VC_ENGINE_STOP_TIMEOUT 15` · `VC_TEARDOWN_TIMEOUT 20` · `VC_BATCH_TIMEOUT 60`.

### Error classification
`classify_exception(exc)` decides: terminal+quarantinable
(`AUTH_KEY_DUPLICATED`, `SESSION_REVOKED`, `AUTH_KEY_UNREGISTERED`,
`ACCOUNT_BANNED`, `UNAUTHORIZED`) -> `TERMINAL`, normalized-phone
`db.mark_account_failed`, no retry/replacement. `AuthKeyDuplicatedError` is
quarantined centrally by `SessionManager` inside `acquire()`. Anything else ->
`FAILED` + replacement spawn from the backup queue (if `is_running`).

### Test seams (defaults = production)
`_voice_engine_available = PYTGCALLS_AVAILABLE`, `_app_factory = PyTgCalls`,
`_sleep_scale = 1.0`, `_launch_stagger = (3.0, 5.0)`, `_probe_interval = 10.0`,
`_voice_chat_probe_retries = 4`, `_initial_stabilize_sleep = 0.3`,
`_post_play_cooldown = 2.0`, `_stream_rejoin_sleep = 5.0`,
`_keepalive_max_cycles = None`, `_keepalive_delays = ((20,20),(30,45),(40,60))`,
`_keepalive_delay_cycle_bands = (3,10)`.

---

## NEW TEST FILE `test_videochat_lifecycle.py` (13 tests)

| Class | Tests | Coverage |
|-------|-------|----------|
| `TestStaticOwnershipAudit` | 3 | No `TelegramClient(`, `.__aenter__(`, `.__aexit__(`, `release_lease(`, `_release_lease(`, `release_proxy(`, `.disconnect()`, `acquire_lock`, `release_lock`, `is_locked`; no fire-and-forget `_graceful_teardown` / `disconnect_all()`; exactly 3 owned `create_task` sites (1 gathered migration, 2 registered spawns) |
| `TestAcquireDiscipline` | 1 | Every acquire site is `async with`; runtime acquire wrapper asserts `auto_release=True`, `module="videochat_stream"`, `worker_id` |
| `TestLeaseLifetime` | 1 | Session owned exactly during the active call, released after |
| `TestCancellation` | 1 | Cancel mid-stream -> `STOPPED`, client/session/proxy/account all released |
| `TestPyTgCallsFailures` | 2 | Start failure -> `FAILED`, no leak; engine-unavailable -> explicit `VIDEO_ENGINE_UNAVAILABLE`, zero acquire |
| `TestTermination` | 1 | `terminate_voice_cluster` awaits cleanup, `_cluster_tasks == {}`, all counters back to 0 |
| `TestRepeatedCycles` | 1 | Repeated start/stop returns to baseline |
| `TestConcurrency` | 1 | Concurrent start of same account rejected, exactly 1 client fabricated, running stream survives |
| `TestAuthKeyDuplicated` | 1 | `AuthKeyDuplicatedError` mid-session -> `TERMINAL`, session `QUARANTINED`, `db.failed` recorded, queued backup **not** consumed, no retry |
| `TestMemoryTaskCycles` | 1 | 10 sequential start/stop cycles: no task/`_cluster_tasks`/counter growth after **every** cycle |

---

## STATIC + RUNTIME VERIFICATION

```text
python -m compileall videochat.py                         -> clean
python -c "import videochat; ..."                          -> VIDEOCHAT_IMPORT_OK True
grep (Select-String) videochat.py:
  TelegramClient( / __aenter__( / __aexit__( / release_lease( /
  _release_lease( / release_proxy( / .disconnect() /
  acquire_lock / release_lock / is_locked                  -> 0 matches
  asyncio.create_task(                                     -> 3 (443 migration, 846 replacement, 889 launch)
  session_manager.acquire                                  -> 3, all "async with"
  disconnect_all                                           -> docstring only (not called)
```

### Suite matrix (all expected 0 failures)
```text
test_p0_closeout.py         42 passed
test_p0_lifecycle.py        34 passed
test_proxy_lease.py          9 passed
test_adder_lifecycle.py     15 passed
test_dmsender_lifecycle.py  22 passed
test_videochat_lifecycle.py 13 passed
------------------------------------------------
TOTAL                       135 passed
```

---

## OBSERVATIONS / DECISIONS DURING VERIFICATION

- **`running_voice_clusters()` counts only `ACTIVE`**, and `_cluster_state` is
  keyed by phone (one entry per phone). A rejected concurrent start for the
  same phone legitimately rewrites that phone's state on its own path, so the
  concurrency test asserts on client-fabrication count and stream survival
  rather than the shared cluster-state map.
- **SessionManager rollback edge (pre-existing, out of scope):** if two
  acquires for the *same* phone with the *same* owner key ever overlap, the
  failing attempt's `_rollback_acquire_failure` can clear the live lease's
  `SessionInfo` and decrement `_active_count`. The engine now makes that
  impossible at its layer via the `_cluster_tasks` guard (rejects before the
  second acquire), so the edge is unreachable through `CloudVoiceChatEngine`.
  `session_manager.py` was intentionally **not** modified (PATCH #7 scope).
- No live-network smoke test was performed. The repository is **not** hereby
  declared production-ready.

## PATCH STATUS: COMPLETE