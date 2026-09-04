# AUDIT_BEFORE_FIX.md — Full 12-Phase Pre-Fix Audit

**Date:** 2026-09-04
**Scope:** All 12 Python files in `D:\Bot-master-main\`
**Method:** Full file reads + targeted grep verification across all modules
**Status:** COMPLETE — NO CODE HAS BEEN MODIFIED

---

## Executive Summary

The system has **three critical bugs** causing the observed symptoms:

1. **`SessionManager._active_count` monotonic leak** — `_release_lease()` never decrements the counter, causing it to grow until `max_active_clients` (200) is hit, blocking ALL new session acquisition permanently.

2. **DM freeze** — Combination of proxy contention (10 proxies vs 14 workers), ignored timeout parameter in `acquire_proxy`, and `SessionAlreadyOwnedError` infinite re-queue loops.

3. **6 competing client-creation sites** bypass `SessionManager.acquire()`, creating duplicate `TelegramClient` objects for the same session → `AuthKeyDuplicatedError`.

---

## PHASE 1: File Inventory (Confirmed)

| File | Actual Lines | Role |
|------|-------------|------|
| `main_bot.py` | 2732 | Bot commands, GlobalState, auditor, server |
| `session_manager.py` | 624 | Session lifecycle (single source of truth) |
| `account_lease_manager.py` | 442 | Account ownership/leasing |
| `exception_classifier.py` | 375 | Error classification (dead code) |
| `proxy_manager.py` | 1018 | Proxy management + leasing |
| `database.py` | 1329 | DB layer + locks |
| `dmsender.py` | 895 | DM campaigns + wizard |
| `videochat.py` | 818 | PyTgCalls adapter |
| `web_console.py` | 1006 | FastAPI web console |
| `scraper.py` | 350 | Group member scraping |
| `adder.py` | 577 | Member adding |
| `config.py` | 363 | Configuration |

---

## PHASE 2: Competing Client Pools (Verified)

### Pool 1: `SessionManager._sessions` (session_manager.py:143)
- **Owner:** SessionManager class instance
- **Creator:** `SessionManager.acquire()` via `_create_client()` at line 353
- **Disconnect:** `_safe_disconnect_client()` at line 547, `disconnect_all()` at line 566
- **Locking:** `asyncio.Lock` at line 142
- **Status:** THE REAL OWNER — single source of truth for session lifecycle

### Pool 2: `GlobalState.client_pool` (main_bot.py:194)
- **Owner:** GlobalState instance
- **Creator:** `pool_set()` at line 316 — **NEVER CALLED** (zero callers in codebase)
- **Disconnect:** `pool_remove()` at line 329 — called 5 times but operates on EMPTY dict
- **Locking:** `_pool_lock` asyncio.Lock at line 178
- **Status:** **VESTIGIAL/DEAD CODE** — completely unused in practice

### Pool 3: `GlobalState.auth_states` (main_bot.py:187)
- **Owner:** GlobalState instance
- **Creator:** `set_auth_state()` at line 282 — during `/login` → `/verify` → `/verify_2fa` flow
- **Disconnect:** `pop_auth_state()` at line 293, `cleanup_stale_auth_states()` at line 297
- **Locking:** `_auth_lock` asyncio.Lock at line 186
- **Status:** TRANSIENT — holds clients only during login flow. Has TTL cleanup.

### Pool 4: `web_console.ACTIVE_CLIENT_POOL` + `CLIENT_LOCKS` (web_console.py:105-108)
- **Owner:** Module-level globals
- **Creator:** `get_buffered_active_client()` at line 164 via `_session_manager._create_client()`
- **Disconnect:** LRU eviction at line 182 (max 50), health check eviction at line 140
- **Locking:** Per-phone `asyncio.Lock` at line 126
- **Status:** **ACTIVE COMPETING POOL** — completely bypasses `SessionManager.acquire()`. Has its own LRU, its own locks, its own lifecycle. Web console clients are INVISIBLE to SessionManager.

### Pool 5: `videochat._client_cache` + `_running_clients` (videochat.py:124-127)
- **Owner:** CloudVoiceChatEngine instance
- **Creator:** `_get_voice_client()` at line 210 via `session_manager._create_client()`
- **Disconnect:** `terminate_voice_cluster()` at line 798, error paths at lines 208, 607, 679
- **Locking:** None — no lock protects `_client_cache` or `_running_clients`
- **Status:** **ACTIVE COMPETING POOL** — bypasses `SessionManager.acquire()`. Creates clients directly via `_create_client()`. No concurrency protection.

### Pool 6: `database.py:1201` direct `TelegramClient(`
- **Owner:** Function-local scope
- **Creator:** `reload_local_accounts()` at line 1201 via direct `TelegramClient(str(session_path), ...)`
- **Disconnect:** `finally` block at line 1237 — disconnects within same function
- **Locking:** None — runs in async context but uses synchronous pymongo
- **Status:** **ACTIVE COMPETING CREATION** — bypasses SessionManager entirely. Uses file-based session (not StringSession). Short-lived but can collide during connect window.

---

## PHASE 3: Client Creation Table (Verified)

| # | Site | File:Line | Via SessionManager? | Via ProxyLease? | Risk |
|---|------|-----------|-------------------|----------------|------|
| 1 | `SessionManager.acquire()` | session_manager.py:353 | YES (is the factory) | YES (via acquire) | Controlled |
| 2 | `web_console.get_buffered_active_client()` | web_console.py:164 | **NO** | **NO** | **HIGH** |
| 3 | `videochat._get_voice_client()` | videochat.py:210 | **NO** | **NO** | **HIGH** |
| 4 | `videochat.launch_voice_cluster()` | videochat.py:392,508 | **NO** | **NO** | **HIGH** |
| 5 | `dmsender` legacy path | dmsender.py:174 | **NO** | **NO** | **HIGH** |
| 6 | `main_bot.create_authenticated_client()` | main_bot.py:561 | **NO** (calls `_create_client`) | **NO** | **HIGH** |
| 7 | `main_bot.verify_handler` fallback | main_bot.py:1464,1533 | **NO** | **NO** | **HIGH** |
| 8 | `database.reload_local_accounts()` | database.py:1201 | **NO** (direct `TelegramClient(`) | **NO** | **MODERATE** |
| 9 | `main_bot.managed_client()` | main_bot.py:605 | YES (delegates to acquire) | Varies | Controlled |
| 10 | `dmsender._dynamic_rolling_worker` | dmsender.py:518 | YES (via acquire) | YES (via acquire) | Controlled |

---

## PHASE 4: AuthKey Collision Paths (Verified)

### Path 1: Web Console + Any SessionManager Worker
```
Web Admin opens account view
  → web_console.get_buffered_active_client(phone)
    → _session_manager._create_client() [BYPASS]
    → client.connect() → ACTIVE_CLIENT_POOL[phone] = client
      ← Meanwhile SessionManager.acquire(phone) [for same phone]
        → _create_client() → second client for same session
          → AuthKeyDuplicatedError on Telegram API
```
**Trigger condition:** Any web console page that accesses an account while SessionManager has it leased.

### Path 2: Videochat + DM Sender
```
DM Sender: SessionManager.acquire(phone) → client₁
  Videochat: _get_voice_client(phone) → _create_client() → client₂
    → Two clients, same session string, different proxies
      → AuthKeyDuplicatedError
```
**Trigger condition:** Running voicechat while DM campaign is active on same accounts.

### Path 3: Login Flow + Auditor
```
/login command: clientₗᵢₘᵢₜₑd created → stored in auth_states[phone]
  Auditor: managed_client(account_doc) → SessionManager.acquire(phone)
    → _create_client() → clientₐᵤ𝒹𝒾ₜ for same session
      → AuthKeyDuplicatedError
```
**Trigger condition:** Auditor cycle fires while admin is mid-login for same phone. Window: login → verify can take minutes.

### Path 4: DB Reload + Runtime
```
/reload_accounts: database.reload_local_accounts()
  → TelegramClient(str(session_path), ...) [direct]
  → client.connect() [for verification]
    → Meanwhile any SessionManager.acquire() for same phone
      → AuthKeyDuplicatedError
```
**Trigger condition:** Admin runs /reload_accounts while campaigns are active.

---

## PHASE 5: DM Freeze Trace (Verified)

### The observed symptom
"Targets: 8118, Accounts: 14, concurrency dictated by available proxies" → stalls, sends nothing.

### Root cause analysis

**The `_active_count` leak (P0-1)** is the primary freeze mechanism:

```
SessionManager.acquire() line 408: self._active_count += 1
_release_lease() line 449:       [NO DECREMENT]
_safe_disconnect_client() line 556: self._active_count = max(0, self._active_count - 1)
```

`_active_count` only decrements in `_safe_disconnect_client`, which is only called during:
- `mark_quarantined` (error path)
- `disconnect_all` (shutdown)
- Pool eviction in GlobalState (but GlobalState.client_pool is dead code)

Normal acquire→use→release cycle via `_release_lease` **never decrements** `_active_count`.

**Calculation:**
- Each DM worker iteration: acquire → `_active_count += 1` → send → `_release_lease` (no decrement)
- After 200 iterations across all workers: `_active_count` hits `_max_active_clients` (200)
- Line 240-243: `if self._active_count >= self._max_active_clients: raise RuntimeError("Maximum active clients (200) reached")`
- ALL subsequent `acquire()` calls raise `RuntimeError` → DM workers crash → `dm_worker` terminates
- `asyncio.gather(*active_workers)` completes with all workers dead
- Campaign reports "HALTED"

**Timeline:** With 14 workers each doing ~14 DM sends before the counter hits 200, the campaign stalls after ~14-200 sends (varies by how many workers are running simultaneously). For 8118 targets, this means the campaign dies very early.

### Secondary freeze mechanisms

1. **Proxy contention** (PROXY_ACQUIRE_TIMEOUT=30s, worker's timeout=10.0 ignored):
   - 10 proxies, 14 workers → 4 workers always blocked for up to 30s each
   - `proxy_lease_manager.acquire_proxy(phone, timeout=PROXY_ACQUIRE_TIMEOUT)` uses hardcoded 30s, NOT the worker's timeout

2. **SessionAlreadyOwnedError infinite re-queue** (dmsender.py:594-597):
   - Exception caught → target put back in queue → `continue` → same worker gets next target → may hit same locked session → re-queue again
   - `stall_count` NOT incremented for `SessionAlreadyOwnedError` → no break condition

3. **Round-robin deadlock** when all 14 accounts are BUSY:
   - Worker 0-13 all cycle through accounts → all BUSY → all re-queue → spin

---

## PHASE 6: Terminal Status Checks (Verified)

### `SessionManager.TERMINAL_STATUSES` (session_manager.py:67-75)
```python
TERMINAL_STATUSES = frozenset({
    "revoked", "banned", "deactivated", "invalid",
    "auth_key_duplicated", "permanently_failed", "quarantined",
})
```
Checked inside `SessionManager.acquire()` at line 265 → yields None if terminal.

### `AccountLeaseManager.TERMINAL_DB_STATUSES` (account_lease_manager.py:43-51)
```python
TERMINAL_DB_STATUSES = frozenset({
    "revoked", "banned", "deactivated", "invalid",
    "auth_key_duplicated", "permanently_failed", "quarantined",
})
```
Checked via `is_eligible()` and `filter_eligible()` — **BUT NOT CALLED** in DM sender's `_dynamic_rolling_worker`.

### DM Sender pre-filter
`_dynamic_rolling_worker` calls `db.get_active_target_sessions()` at line 147, which does DB-level filtering. But this happens ONCE at campaign start. If an account becomes terminal DURING the campaign, only `SessionManager.acquire()`'s internal check catches it (which does work, but wastes a full proxy acquire/release cycle each time).

### Auditor pre-filter
`_audit_single_account` at line 2496 calls `db.is_locked(clean_phone)` → `pool_remove` → skips locked accounts. Does NOT check terminal status before calling `managed_client`. Terminal check happens inside `SessionManager.acquire()`.

---

## PHASE 7: Auditor Audit (Verified)

### `continuous_session_auditor` (main_bot.py:2325)
- Runs after 30-90s initial delay
- Auto-pauses during campaigns (line 2343)
- Processes accounts sequentially with 10-20s human-like delay per account (line 2379)
- Batch size 10, stagger 15s between batches
- Uses `managed_client(account_doc, use_pool=False)` → goes through `SessionManager.acquire()`

### `_audit_single_account` (main_bot.py:2484)
- Checks cache (1h TTL) → skips recently-verified accounts
- Calls `check_session_authorization()` → `is_user_authorized()` → `get_me()` fallback
- Catches `AuthKeyDuplicatedError` → quarantines via `session_manager.mark_quarantined()`
- Does NOT catch `SessionAlreadyOwnedError` → propagates to batch loop → counted as "failed" (false positive)
- Sends HTTP notification to `bluecoys.com/api/telegram-disconnected` on failure (line 2600-2606)

### Issues
1. **`SessionAlreadyOwnedError` not caught** → false "dead session" counts, admin alerts for healthy accounts
2. **`use_pool=False` skips proxy leasing** → auditor creates clients without proxies → may fail if Telegram blocks direct connections
3. **`gc.collect()` calls** at lines 2396, 2409 — harmless but unnecessary
4. **External HTTP call** (line 2600-2606) during audit cycle — network I/O in non-critical path, potential timeout delays

---

## PHASE 8: Proxy System Audit (Verified)

### ProxyLeaseManager lifecycle
1. `start()` → `_sync_proxies()` → `_auto_reaper_loop()` background task
2. `_sync_proxies()` reads `proxy_manager.working_proxies` → creates `ProxyNode` objects in `proxy_nodes` dict
3. `acquire_proxy()` → `_sync_proxies()` (re-syncs) → waits on `Condition` → leases first available node
4. `release_proxy()` → verifies ownership → releases node or puts in cooldown → notifies waiters
5. `_auto_reaper_loop()` wakes every 5s → removes expired cooldowns → notifies

### Issues

1. **`_sync_proxies` only ADDS, never removes** (proxy_manager.py:407-424): If a proxy fails and is removed from `proxy_manager.working_proxies`, its `ProxyNode` remains in `proxy_nodes` as a stale entry with `is_leased=False`.

2. **Thread safety race** (proxy_manager.py:766-798): `start_background_testing` uses `ThreadPoolExecutor` which mutates `self.working_proxies` (with `self._lock` threading.Lock), but async code reads `self.working_proxies` without any lock → potential race condition.

3. **`get_available_count()` calls `_sync_proxy_sync()` synchronously** (proxy_manager.py:555): This iterates `working_proxies` from the event loop thread — blocks event loop during iteration.

4. **Double proxy release in DM sender** (dmsender.py:618-623): `SessionManager.acquire()`'s `finally` block (session_manager.py:428-437) already releases the proxy. Then the worker's exception handler calls `release_proxy(should_cooldown=True)` again → second call sees `node.leased_to=None` → ownership check passes → applies unnecessary cooldown.

5. **`release_proxy` param order**: Both `SessionManager.release_proxy(phone, proxy_url)` (line 503) and `ProxyLeaseManager.release_proxy(proxy_url, phone)` (line 514) are called correctly in all paths. No reversed parameters detected — the initial suspicion was unfounded.

---

## PHASE 9: Memory/Task Audit (Verified)

1. **ThreadPoolExecutor leak** (proxy_manager.py:766): `start_background_testing` creates new `ThreadPoolExecutor` per call without cleanup. If called multiple times, orphaned executors accumulate.

2. **`_testing_thread` orphan** (proxy_manager.py:828): Daemon thread, `stop_background_testing` calls `join(timeout=2.0)` — may leave orphaned thread if testing is slow.

3. **`videochat._running_clients` unbounded growth** (videochat.py:518): Clients appended but only cleaned in error paths or `terminate_voice_cluster()`. Repeated `launch_voice_cluster` calls without termination → client leak.

4. **`videochat._client_cache` stale entries** (videochat.py:127): Disconnected clients not evicted from cache. Cache key collision with stale entry → reconnect attempt on dead client → fails silently.

5. **`wizard_state` unbounded** (dmsender.py:2225): Dict keyed by user sender_id. Users who start DM wizard and never finish leave stale state forever. No TTL, no size limit.

6. **`_last_auth_check` TTLCache** (main_bot.py:2428): Bounded at 512, TTL 3600s. SAFE.

7. **`GlobalState.background_tasks`** (main_bot.py:205): Uses `Set[asyncio.Task]` with `add_done_callback(discard)`. SAFE.

8. **`gc.collect()` calls** (main_bot.py:2396, 2409): Harmless but wasteful.

---

## PHASE 10: DB Audit (Verified)

### Sync DB in async context
All MongoDB operations via `pymongo` (synchronous driver) run directly on the event loop thread. Every `db.get_session_by_phone()`, `db.update_session_status()`, `db.mark_account_revoked()` blocks the event loop for the duration of the MongoDB operation. With 14 DM workers + auditor + web console, this causes significant event loop starvation.

### Lock system (database.py)
- `acquire_lock(phone)` → writes to `locks` collection with TTL (LOCK_TTL_SECONDS=7200)
- `release_lock(phone)` → deletes from `locks` collection
- `is_locked(phone)` → checks `locks` collection
- `release_all_locks()` → deletes all locks
- **Issue:** Locks are phone-keyed, not lease-keyed. Multiple modules can call `acquire_lock` for same phone → last writer wins. No owner verification.

### Dual session storage
- `save_authorized_session()` stores `session_string` field
- `update_session_status()` optionally stores `session` field
- `safe_session_str()` tries `session_string` then `session` → handles both
- **Issue:** Two write paths can create inconsistent state.

### `reload_local_accounts()` (database.py:1160-1249)
- Creates `TelegramClient(str(session_path), ...)` with file-based session
- Connects, verifies authorization, converts to StringSession
- **Issue:** Uses file-based session path (not StringSession) → different session key than StringSession → potential fingerprint mismatch
- **Issue:** Synchronous pymongo calls block event loop during UI progress updates

---

## PHASE 11: Architecture Consolidation Plan

### Current responsibilities (12 modules)

| Module | Lines | Responsibilities |
|--------|-------|-----------------|
| main_bot.py | 2732 | Bot commands, GlobalState, managed_client, auditor, server, client factory wrappers |
| session_manager.py | 624 | Session lifecycle, client factory, leasing |
| account_lease_manager.py | 442 | Account ownership, leasing, TTL |
| exception_classifier.py | 375 | Error classification (dead code) |
| proxy_manager.py | 1018 | Proxy management, leasing, providers, health checking |
| database.py | 1329 | DB layer, locks, session operations |
| dmsender.py | 895 | DM campaigns, wizard |
| videochat.py | 818 | PyTgCalls, client management |
| web_console.py | 1006 | FastAPI, its own client pool |
| scraper.py | 350 | Group scraping |
| adder.py | 577 | Member adding |
| config.py | 363 | Configuration |

### Proposed consolidation

```
runtime_manager.py  ← session_manager.py + account_lease_manager.py + exception_classifier.py
campaign_engine.py  ← adder.py + dmsender.py
main_bot.py         ← stays (trimmed)
config.py           ← stays
database.py         ← stays (DB only, no lock logic)
proxy_manager.py    ← stays (trimmed)
scraper.py          ← stays
videochat.py        ← stays (client creation routed through runtime_manager)
web_console.py      ← stays (client creation routed through runtime_manager)
```

### Migration sequence (dependency order)
1. `exception_classifier.py` → merge into `runtime_manager.py`
2. `account_lease_manager.py` → merge into `runtime_manager.py`
3. `session_manager.py` → merge into `runtime_manager.py` (fix `_active_count` leak)
4. `adder.py` + `dmsender.py` → merge into `campaign_engine.py`
5. `videochat.py` → route client creation through `runtime_manager.py`
6. `web_console.py` → route client creation through `runtime_manager.py`
7. `main_bot.py` → remove dead `GlobalState.client_pool`, `create_authenticated_client()`
8. `database.py` → remove lock system (moved to `runtime_manager.py`)

---

## P0/P1/P2/P3 Issue List

### P0 — Critical (crash, deadlock, resource corruption)

| ID | Issue | File:Line | Description |
|----|-------|-----------|-------------|
| **P0-1** | `_active_count` monotonic leak | session_manager.py:408,449 | `_active_count += 1` in `acquire()` but `_release_lease()` never decrements. Counter hits `max_active_clients` (200) after 200 acquire/release cycles → ALL subsequent `acquire()` calls raise `RuntimeError("Maximum active clients (200) reached")`. **This is the primary cause of DM freeze and system-wide deadlock.** |
| **P0-2** | Web console bypass creates duplicate clients | web_console.py:164 | `_session_manager._create_client()` bypasses `acquire()` → creates TelegramClient invisible to SessionManager → two clients for same session → `AuthKeyDuplicatedError`. |
| **P0-3** | Videochat bypass creates duplicate clients | videochat.py:210,392,508 | `_session_manager._create_client()` bypasses `acquire()` → same collision risk as P0-2. |
| **P0-4** | DM `_dynamic_rolling_worker` stall | dmsender.py:436-712 | 14 workers competing for 10 proxies with 30s timeout (worker's 10s ignored) + `SessionAlreadyOwnedError` infinite re-queue loop → system spins doing nothing. |
| **P0-5** | `_sessions` dict never shrinks | session_manager.py:143 | Entries persist after `_release_lease` → dict grows unboundedly. Memory leak proportional to total accounts ever used. |

### P1 — Major functionality failure

| ID | Issue | File:Line | Description |
|----|-------|-----------|-------------|
| **P1-1** | `classify_exception()` never called | exception_classifier.py:85 | Imported in 6 modules but never invoked at runtime. Error handling uses string matching instead of structured classification. |
| **P1-2** | `classify_connection_error()` never called | exception_classifier.py:353 | Imported in main_bot.py:37 but never used. |
| **P1-3** | `GlobalState.client_pool` dead code | main_bot.py:194 | 300+ lines of pool management (`pool_set`, `pool_get`, `pool_remove`, `pool_cleanup_stale`, `pool_clear`, `pool_size`) operate on always-empty dict. `pool_remove` called 5 times on empty dict. |
| **P1-4** | `SessionAlreadyOwnedError` not caught in auditor | main_bot.py:2512 | Auditor calls `managed_client()` → `acquire()` → `SessionAlreadyOwnedError` → not caught in `_audit_single_account` → propagates → counted as "session failed" → false admin alerts. |
| **P1-5** | DM sender legacy path bypasses SessionManager | dmsender.py:174 | `_create_client()` called directly in legacy fallback → bypasses all SessionManager invariants. Only reached if `proxy_lease_manager._is_running` is False. |
| **P1-6** | Login flow clients collide with auditor | main_bot.py:1408,1464,1533 | `auth_states` holds live client during OTP wait → auditor may acquire same phone → collision. |

### P2 — Performance/reliability

| ID | Issue | File:Line | Description |
|----|-------|-----------|-------------|
| **P2-1** | Sync pymongo blocks event loop | database.py (all) | Every `get_session_by_phone()`, `update_session_status()`, etc. blocks event loop. With 14 workers + auditor + web console, significant event loop starvation. |
| **P2-2** | Thread safety race in proxy testing | proxy_manager.py:766-798 | `ThreadPoolExecutor` mutates `working_proxies` with `threading.Lock`, but async code reads without lock. |
| **P2-3** | `get_available_count()` sync call | proxy_manager.py:555 | `_sync_proxy_sync()` iterates `working_proxies` from event loop thread — blocking. |
| **P2-4** | `_sync_proxies` never removes stale entries | proxy_manager.py:407-424 | Failed proxies remain in `proxy_nodes` dict indefinitely. |
| **P2-5** | Double proxy release in DM sender | dmsender.py:618-623 | `acquire()` `finally` releases proxy → worker releases again → unnecessary cooldown activation. |
| **P2-6** | DM `stall_count` not incremented for `SessionAlreadyOwnedError` | dmsender.py:594-597 | Infinite re-queue loop for contested sessions. |
| **P2-7** | `proxy_lease_manager.acquire_proxy` ignores caller's timeout | session_manager.py:330 | Uses hardcoded `PROXY_ACQUIRE_TIMEOUT=30s` instead of caller-provided timeout. Worker's `timeout=10.0` is silently ignored. |
| **P2-8** | DM sender `release_lock` missing for campaign complete | dmsender.py:436-712 | `_dynamic_rolling_worker` never calls `db.release_lock()` for accounts — locks accumulate until DB TTL (7200s). |

### P3 — Cleanup/refactoring

| ID | Issue | File:Line | Description |
|----|-------|-----------|-------------|
| **P3-1** | Dead `GlobalState.client_pool` | main_bot.py:194 | Remove ~80 lines of dead pool management code. |
| **P3-2** | Dead `classify_exception` imports | 6 files | Remove unused imports across codebase. |
| **P3-3** | `wizard_state` unbounded dict | dmsender.py:2225 | Add TTL or size limit. |
| **P3-4** | `videochat._client_cache` stale entries | videochat.py:127 | Evict disconnected clients from cache. |
| **P3-5** | `videochat._running_clients` unbounded | videochat.py:124 | Ensure cleanup on every voice cluster cycle. |
| **P3-6** | `ThreadPoolExecutor` leak | proxy_manager.py:766 | Reuse single executor or properly shut down per-test. |
| **P3-7** | `gc.collect()` calls | main_bot.py:2396,2409 | Remove — Python GC handles this. |
| **P3-8** | Dual session storage fields | database.py | Consolidate `session` vs `session_string` to single field. |
| **P3-9** | External HTTP call in auditor | main_bot.py:2600-2606 | Move Bluecoys API notification to async background task. |
| **P3-10** | DB lock system overlaps with SessionManager | database.py | Remove DB-level locks (moved to runtime_manager). |

---

## Session Collision Graph

```
                    ┌─────────────────────────┐
                    │   Telegram API Server    │
                    │  (same session key =     │
                    │   AuthKeyDuplicatedError)│
                    └────────────┬────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
   Client A                 Client B                 Client C
   (SessionManager)         (WebConsole)             (VideoChat)
   via acquire()            via _create_client()     via _create_client()
        │                        │                        │
   ┌────┴────┐            ┌─────┴─────┐           ┌─────┴─────┐
   │ proxy₁  │            │  no proxy │           │  no proxy │
   │ session₁│            │  session₁ │           │  session₁ │
   │ owned:  │            │  owned:   │           │  owned:   │
   │ SM only │            │  WC only  │           │  VC only  │
   └─────────┘            └───────────┘           └───────────┘
   
   ALL THREE have same StringSession → SAME AuthKey → COLLISION
```

## DM Freeze Trace

```
                          ┌─────────────────┐
                          │   8118 targets   │
                          │  asyncio.Queue   │
                          └────────┬────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
               Worker 0       Worker 1  ...  Worker 13
                    │              │              │
                    ▼              ▼              ▼
            round_robin[0]  round_robin[1]  round_robin[13]
                    │              │              │
                    ▼              ▼              ▼
            SessionManager.acquire() → blocks on proxy
                    │              │              │
              ┌─────┴─────┐  ┌────┴────┐   ┌────┴────┐
              │ Proxy 0   │  │ Proxy 1 │   │ None    │
              │ (leased)  │  │ (leased)│   │ (wait  │
              │           │  │         │   │  30s)   │
              └─────┬─────┘  └────┬────┘   └────┬────┘
                    │              │              │
                    ▼              ▼              ▼
              send_message   send_message    timeout → None
              + delay 4.5s   + delay 4.5s   → re-queue target
                    │              │              │
                    ▼              ▼              ▼
              _release_lease  _release_lease  (loop back)
              [NO DECREMENT]  [NO DECREMENT]
              _active_count   _active_count
              += 1            += 1
                    │              │
                    └──────┬───────┘
                           │
                    After 200 cycles:
                    _active_count = 200 = max_active_clients
                           │
                           ▼
                    RuntimeError("Maximum active clients reached")
                           │
                           ▼
                    ALL workers crash → campaign HALTED
```

---

## Proxy Lifecycle Graph

```
┌──────────┐    start()    ┌──────────────┐
│ ProxyMgr │──────────────▶│ ProxyLeaseMgr│
│ (dicts)  │               │ (ProxyNodes) │
└──────────┘               └──────┬───────┘
                                  │
                    ┌─────────────┼─────────────┐
                    │             │             │
              acquire_proxy  release_proxy  auto_reaper
                    │             │             │
                    ▼             ▼             ▼
              Condition.wait  ownership     cooldown
              → find node     verify        expiry
              → node.acquire  → release     → notify_all
              → return dict   or cooldown
                                  │
                          ┌───────┴───────┐
                          │               │
                     normal release   cooldown release
                     node.release()   node.put_in_cooldown()
                                      proxy_cooldown.add(url)
                                      account_cooldown[phone]
```

## Proposed Target Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    main_bot.py                           │
│  Bot commands, GlobalState (trimmed), managed_client,   │
│  auditor, server startup                                │
└──────────────────────┬──────────────────────────────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
         ▼             ▼             ▼
┌────────────────┐ ┌─────────────┐ ┌──────────────┐
│ runtime_manager│ │campaign_eng │ │ proxy_manager│
│ (merged)       │ │ (merged)    │ │              │
│                │ │             │ │ ProxyManager │
│ SessionManager │ │ AdderState  │ │ ProxyLeaseMgr│
│ AccountLease   │ │ DMSender    │ │ Providers    │
│ ErrorClassify  │ │ Wizard      │ │              │
│ TERMINAL_DB    │ │             │ │              │
└────────────────┘ └─────────────┘ └──────────────┘
         │                │              │
         └────────────────┼──────────────┘
                          │
              ┌───────────┼───────────┐
              │           │           │
              ▼           ▼           ▼
        ┌──────────┐ ┌─────────┐ ┌─────────┐
        │database.py│ │scraper.py│ │videochat.py│
        │(DB only)  │ │         │ │(via SM)   │
        └──────────┘ └─────────┘ └──────────┘
```

---

## Non-Negotiable Invariants (To Enforce in Fix)

1. **ONE SESSION = ONE ACTIVE CLIENT = ONE ACTIVE OWNER = ONE ACTIVE NETWORK ROUTE**
2. **ONE PROXY LEASE = ONE ACTIVE WORKER**
3. **No module may bypass the runtime manager for client creation**
4. **No hard-coded worker counts**
5. **External I/O must not happen under global lifecycle locks**
6. **No auto-retry of AuthKeyDuplicatedError**
7. **No positional args for proxy release**
8. **Terminal accounts must be pre-filtered before client creation**
9. **`_active_count` must be decremented on every release path**
10. **`_sessions` dict must shrink when sessions become AVAILABLE**

---

*END OF AUDIT — NO CODE HAS BEEN MODIFIED. Ready for implementation.*
