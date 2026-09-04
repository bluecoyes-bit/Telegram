# FINAL_ARCHITECTURE

Target state of the Telegram multi-account suite after the final architecture pass.
All changes are within `p1-final-architecture-pass` (uncommitted working tree on top of
`PRODUCTION_BASELINE.md` freeze at `6c678ba`).

## 1. Ownership model (the core invariant)

> ONE SESSION = ONE ACTIVE CLIENT = ONE ACTIVE OWNER = ONE ACTIVE NETWORK ROUTE
> ONE PROXY LEASE = ONE ACTIVE OWNER
> TERMINAL ACCOUNT ⇒ ZERO client = ZERO proxy = ZERO worker = ZERO retry

- **`SessionManager`** (`session_manager.py`) is the **single owner** of active Telethon
  clients/sessions and of the per-session lease. It enforces `_active_count <= _max_active_clients`
  under an `asyncio.Lock`. Public lifecycle API: `acquire()` (async context manager) and
  `release_lease(lease)`; `_release_lease` is internal-only.
- **`AccountLeaseManager`** (`account_lease_manager.py`) is a thin eligibility+lease-expiry layer
  over the DB statuses; it does NOT create clients. Its reaper reclaims expired reservations.
- **`ProxyLeaseManager` / `ProxyManager`** own proxy nodes; `ProxyLeaseManager` auto-reclaims
  leases/cooldowns. One lease → one owner.
- **Feature modules** (`adder.py`, `dmsender.py`, `videochat.py`, `scraper.py`, `web_console.py`)
  consume sessions only via `acquire()` / `release_lease()` and never call private internals.

## 2. Session lifecycle states

`available → reserved → connected → idle → available | terminal`, plus `quarantined`.
Terminal set: `revoked`, `banned`, `deactivated`, `invalid`, `auth_key_duplicated`,
`permanently_failed`, `quarantined`. Terminal ⇒ never yields a client/lease/worker.

## 3. Account-status gating order

1. DB status check (terminal ⇒ skip) — first gate.
2. `AccountLeaseManager.is_eligible()` (DB statuses in eligible set).
3. `SessionManager.acquire()` (terminal re-check + capacity + fingerprint).
4. Per-module worker capacity (`compute_dm_worker_capacity`, adder proxy-aware pool).

## 4. Resource-aware scheduling

- **DM** (`dmsender.py`): `compute_dm_worker_capacity(num_accounts, available_proxies,
  session_capacity, configured_limit) = max(1, min(...))` when accounts exist, `0` when none.
  O(1); emits `worker_started` with `effective_capacity`.
- **Adder** (`adder.py`): pool = `min(len(active_accounts), ADDER_MAX_WORKER_SESSIONS,
  available_proxy_count)` (≥1), no hardcoded sessions.

## 5. Error/terminal classification

`exception_classifier.py` centralizes `ErrorCategory` mapping. `AuthKeyDuplicated` ⇒
`auth_key_duplicated` terminal ⇒ session removed permanently, proxy/worker released, no retry.

## 6. Session event journal & collision diagnostics

`_log_lifecycle` emits structured lifecycle events (`SESSION_...`), including phone + session
fingerprint + module + worker. Combined with the fingerprint hash, collisions are attributable to
a source record even when a different worker later observes the phone. (Historical cross-restart
correlation is documentable only — see readiness report §H.)

## 7. Task lifecycle (Phase 22)

- Auditor + auto-recovery: registered in `GLOBAL.background_tasks`, cancelled in lifespan shutdown.
- Proxy/account lease reapers: stored + cancelled on `stop()`.
- DM workers/reporter & adder/videochat workers: gathered / stored in tracked structures.
- **Web-console mass-join worker (fixed):** now tracked in `_background_tasks` with a
  done-callback discard, cancelled+awaited by `shutdown_background_tasks()` on shutdown.
- `videochat._active_tasks` uses a `WeakSet`. Teardown helpers (`force_exit_routine`,
  `_graceful_teardown`) are short self-terminating routines.

## 8. Synchronous-driver policy (Phase 20)

Synchronous PyMongo is used (no motor). Hot async paths must not block the loop: they use
`get_session_by_phone_async` / `_run_sync` (executor). Fixed in `SessionManager.acquire()`,
`reserve_login()`, `AccountLeaseManager.is_eligible()`, `acquire()`. Caching
(`_session_cache`, `TTLCache`) mitigates remaining sync reads.

## 9. Web API security (Phase 18)

`console_router` is gated by `Depends(ensure_api_token)` on **every** route:
- Token from `WEB_API_TOKEN` (`config.py`), read at request time.
- Accepts `Authorization: Bearer <token>` or `X-API-Token: <token>`.
- Secure by default: unset token ⇒ 503 (refused), never open.
- Per-IP sliding-window rate limiter (60 req / 60 s) ⇒ 429; bounded bucket dict.
Verified via HTTP TestClient (401/503/200/429) in `test_p1_security.py`.

## 10. Ordered shutdown

lifespan shutdown order: cancel auditor+recovery → cancel web console background tasks →
stop proxy/account lease managers → `session_manager.disconnect_all()` → `db.close()` →
`real_bot.disconnect()`.

## 11. Caches / memory safety

`TTLCache` session/stats caches (bounded), `_status_bar_cache` TTL 30 s, `auth_states` TTL-pruned,
`deque(maxlen=100)` console logs, `lifecycle_events` capped at 500, `_sessions` bounded by
`_max_active_clients`, `_active_tasks` WeakSet, lease reapers prune stale entries. All bounded.
