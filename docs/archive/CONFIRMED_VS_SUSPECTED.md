# CONFIRMED_VS_SUSPECTED.md

**Date:** 2026-09-04
**Scope:** All issues from AUDIT_BEFORE_FIX.md verified against source code

---

## P0 Issues

| ID | Issue | Evidence | Status | File:Line | Runtime Symptom | Fix |
|----|-------|----------|--------|-----------|-----------------|-----|
| P0-1 | `_active_count` monotonic leak | `acquire()` increments at line 408; `_release_lease()` at line 449 never decrements. Only `_safe_disconnect_client()` (error path only) decrements at line 556. Counter hits `_max_active_clients` (200) → `RuntimeError` blocks all new sessions. | **CONFIRMED** | session_manager.py:408,449,556 | DM stalls after ~200 sends. Auditor and all acquire callers get `RuntimeError("Maximum active clients (200) reached")`. | Add `self._active_count -= 1` in `_release_lease()`. Also add `self._active_count -= 1` in `release()`. |
| P0-2 | WebConsole creates duplicate clients | `web_console.py:164` calls `_session_manager._create_client()` directly, stores in `ACTIVE_CLIENT_POOL`. No `SessionManager.acquire()` call. Two clients for same session → `AuthKeyDuplicatedError`. | **CONFIRMED** | web_console.py:164 | Web admin page opens → triggers duplicate client → `AuthKeyDuplicatedError` on next SessionManager operation for same phone. | Route all web console client creation through `SessionManager.acquire()`. Remove `ACTIVE_CLIENT_POOL` and `CLIENT_LOCKS`. |
| P0-3 | VideoChat creates duplicate clients | `videochat.py:210,392,508` all call `self.session_manager._create_client()` directly. No `acquire()`. Clients stored in `_client_cache` and `_running_clients` — invisible to SessionManager. | **CONFIRMED** | videochat.py:210,392,508 | Voice chat starts → duplicate client → `AuthKeyDuplicatedError`. | Route all videochat client creation through `SessionManager.acquire()`. Remove `_client_cache`. |
| P0-4 | Adder creates duplicate clients | `adder.py:321,384` call `self.session_manager._create_client()` directly. Acquires proxy independently via `proxy_lease_manager.acquire_proxy()`. Bypasses all SessionManager invariants. | **CONFIRMED** | adder.py:321,384 | Adder starts → duplicate client → `AuthKeyDuplicatedError`. | Route all adder client creation through `SessionManager.acquire()`. |
| P0-5 | DM sender legacy bypass | `dmsender.py:174` calls `self.session_manager._create_client()` in legacy fallback path (when lease manager not running). Creates clients independently. | **CONFIRMED** | dmsender.py:174 | Legacy DM path → duplicate client → `AuthKeyDuplicatedError`. | Remove legacy path. Always use `_dynamic_rolling_worker` which goes through `SessionManager.acquire()`. |
| P0-6 | Login flow creates unmanaged clients | `main_bot.py:714` calls `session_manager._create_client()` for `/login` flow. Client stored in `GlobalState.auth_states`. Auditor or other workers can acquire same phone concurrently → collision. | **CONFIRMED** | main_bot.py:714,1408 | Login starts → client live in `auth_states` → auditor picks same phone → `AuthKeyDuplicatedError`. | Login flow should use `SessionManager.acquire()` or mark session as reserved during login. |
| P0-7 | `_sessions` dict never shrinks | `SessionManager._sessions` entries persist after `_release_lease()` at line 449. Lifecycle becomes AVAILABLE but entry stays. Only cleaned by `disconnect_all()` (shutdown) or `mark_quarantined()`. Dict grows proportionally to total unique accounts used. | **CONFIRMED** | session_manager.py:143,449 | Memory usage grows linearly with account count. 10,000 accounts → 10,000 entries permanently in dict. | Clean up AVAILABLE entries that have been idle beyond a configurable TTL. |
| P0-8 | DB lock ownership conflict | `database.py:363-366` stores `{phone: expiry}` — no owner field. Any module can call `acquire_lock(phone)` for any phone, overwriting existing lock. `release_lock(phone)` has no owner check. Scraper locks → adder overwrites → scraper releases → adder unprotected. | **CONFIRMED** | database.py:363-366 | Scraper holds lock → adder acquires same lock → scraper releases → adder runs unprotected → concurrent access to same session. | Migrate DB locks to `SessionManager` / `AccountLeaseManager` with owner verification. Remove `database.py` lock system. |
| P0-9 | Terminal accounts reach client creation | `get_active_target_sessions()` does DB-level filtering but does NOT exclude terminal statuses (revoked, banned, auth_key_duplicated). Terminal accounts enter worker loop → `SessionManager.acquire()` catches them at line 265, but AFTER proxy acquisition and DB read. Wastes resources and causes confusing logs. | **CONFIRMED** | dmsender.py:147, session_manager.py:265 | Terminal account hits worker → acquire blocks on proxy → DB read → terminal check → yields None → worker stalls. | Filter terminal accounts BEFORE entering worker loop using `AccountLeaseManager.filter_eligible()`. |
| P0-10 | `SessionAlreadyOwnedError` infinite re-queue | `dmsender.py:594-597` catches `SessionAlreadyOwnedError` → re-queues target → `continue`. `stall_count` NOT incremented. If all accounts are BUSY, workers spin re-queuing the same targets forever. | **CONFIRMED** | dmsender.py:594-597 | DM workers consume 100% CPU re-queuing → no progress → appears frozen. | Increment `stall_count` on `SessionAlreadyOwnedError`. Add `MAX_REQUEUE` limit per target. |
| P0-11 | `_active_count` not decremented in `release()` | `SessionManager.release()` at line 492 sets lifecycle to AVAILABLE but does not decrement `_active_count`. Same leak as P0-1 for any code path that uses `release()` instead of `_release_lease()`. | **CONFIRMED** | session_manager.py:492-501 | Same as P0-1. | Add `self._active_count = max(0, self._active_count - 1)` in `release()`. |
| P0-12 | AuthKeyDuplicatedError — no retry risk in current code | `classify_exception()` documents it as "NOT retryable". Current handlers: `main_bot.py:2553` quarantines. `dmsender.py:645` quarantines. No code retries. But `classify_exception()` is never called at runtime — the quarantine happens via ad-hoc `except AuthKeyDuplicatedError` blocks, not via the classifier. If a new code path forgets to catch it, the error propagates unhandled. | **SUSPECTED** | exception_classifier.py:90,98 | If new code path doesn't catch `AuthKeyDuplicatedError`, the exception propagates and could crash a worker. | Ensure `SessionManager.acquire()` catches `AuthKeyDuplicatedError` internally and quarantines + yields None. |

---

## P1 Issues

| ID | Issue | Evidence | Status | File:Line | Runtime Symptom | Fix |
|----|-------|----------|--------|-----------|-----------------|-----|
| P1-1 | `classify_exception()` never called at runtime | Imported in 6 modules (session_manager, account_lease_manager, dmsender, adder, web_console, videochat) but grep confirms zero call sites. Only `ErrorCategory` enum values are used directly. | **CONFIRMED** | exception_classifier.py:85 | Error handling uses ad-hoc string matching instead of structured classification. Inconsistent error behavior across modules. | Use `classify_exception()` in `SessionManager.acquire()` and all worker exception handlers. |
| P1-2 | `classify_connection_error()` never called | Imported in `main_bot.py:37` but grep confirms zero call sites. Wrapper around `classify_exception()`. | **CONFIRMED** | exception_classifier.py:353 | Dead code. | Remove import from main_bot.py. |
| P1-3 | `GlobalState.client_pool` dead code | `pool_set()` never called (0 callers). `pool_remove()` called 5 times on empty dict. `pool_cleanup_stale()`, `pool_clear()`, `pool_size()` operate on empty dict. ~80 lines of dead code. | **CONFIRMED** | main_bot.py:194 | No runtime symptom — just dead code and wasted memory for the dict object. | Remove `client_pool`, `pool_get`, `pool_set`, `pool_remove`, `pool_cleanup_stale`, `pool_clear`, `pool_size` from GlobalState. |
| P1-4 | Auditor doesn't catch `SessionAlreadyOwnedError` | `_audit_single_account()` catches `AuthKeyDuplicatedError` (line 2553) and various connection errors (line 2571) but NOT `SessionAlreadyOwnedError`. Exception propagates to batch loop → counted as "session failed" → false admin alerts. | **CONFIRMED** | main_bot.py:2512,2553 | Auditor reports healthy accounts as "failed" → admin gets spurious alerts. | Add `except SessionAlreadyOwnedError` in `_audit_single_account` → skip (return False, no alert). |
| P1-5 | `wizard_state` unbounded dict | `dmsender.py:2225` stores state keyed by `event.sender_id`. No TTL, no size limit. Users who start wizard and never finish leave stale state forever. | **CONFIRMED** | dmsender.py:2225 | Memory leak proportional to number of wizard sessions started (not completed). | Add TTL or max-size eviction. Clean up on `/stop_dmsender`. |
| P1-6 | `videochat._client_cache` stale entries | Disconnected clients not evicted from `_client_cache` dict. Cache key collision → reconnect on dead client → fails silently. | **CONFIRMED** | videochat.py:127,208 | Stale cache entries → wasted memory → failed reconnection attempts. | Evict on disconnect. Use `WeakValueDictionary` or explicit cleanup. |
| P1-7 | `videochat._running_clients` unbounded | Clients appended at line 518 but only cleaned in error paths or `terminate_voice_cluster()`. Repeated `launch_voice_cluster` without termination → client leak. | **CONFIRMED** | videochat.py:124,518 | Memory leak proportional to number of voice cluster launches. | Clean up completed clients from list. Use lifecycle tracking. |

---

## P2 Issues

| ID | Issue | Evidence | Status | File:Line | Runtime Symptom | Fix |
|----|-------|----------|--------|-----------|-----------------|-----|
| P2-1 | Sync pymongo blocks event loop | All DB operations use `pymongo` (sync) in async context. Every `get_session_by_phone()`, `update_session_status()`, etc. blocks event loop. | **CONFIRMED** | database.py (all) | Event loop starvation under load → increased latency, timeouts. | Wrap DB calls in `asyncio.to_thread()` or use `motor` (async MongoDB driver). |
| P2-2 | Thread safety race in proxy testing | `ThreadPoolExecutor` mutates `self.working_proxies` with `threading.Lock`, but async code reads without lock. | **CONFIRMED** | proxy_manager.py:766-798 | Rare race condition → `working_proxies` inconsistency → proxy not found or double-used. | Use `asyncio.Queue` or `asyncio.Lock` for cross-thread communication. |
| P2-3 | `_sync_proxies` never removes stale entries | `ProxyLeaseManager._sync_proxies()` only adds new proxies, never removes failed ones from `proxy_nodes`. | **CONFIRMED** | proxy_manager.py:407-424 | Stale proxy nodes accumulate → `acquire_proxy` tries dead proxies → timeout. | Reconcile `proxy_nodes` with `working_proxies` on each sync. |
| P2-4 | Double proxy release in DM sender | `SessionManager.acquire()` `finally` at line 428 releases proxy. Worker's exception handler at dmsender.py:618 calls `release_proxy` again → unnecessary cooldown. | **CONFIRMED** | dmsender.py:618-623, session_manager.py:428-437 | Proxy enters cooldown unnecessarily → reduced proxy availability. | Remove redundant `release_proxy` call from DM sender exception handlers. `SessionManager.acquire()` handles it. |
| P2-5 | `acquire_proxy` ignores caller timeout | `session_manager.py:330` calls `acquire_proxy(clean_phone, timeout=PROXY_ACQUIRE_TIMEOUT)` using hardcoded 30s. Worker's `timeout=10.0` parameter is silently ignored. | **CONFIRMED** | session_manager.py:330 | Workers block for 30s instead of intended 10s when proxies exhausted. | Pass caller's timeout to `acquire_proxy()`. |
| P2-6 | DM `_dynamic_rolling_worker` missing `release_lock` | Legacy path at dmsender.py:167 calls `db.acquire_lock(phone)` but `_dynamic_rolling_worker` at line 436-712 never calls `db.release_lock()`. Locks accumulate until TTL (7200s). | **CONFIRMED** | dmsender.py:436-712 | DB locks accumulate → auditor skips locked accounts → reduced active pool. | Remove DB lock calls from DM sender (migrated to runtime_manager). |

---

## P3 Issues

| ID | Issue | Evidence | Status | File:Line | Runtime Symptom | Fix |
|----|-------|----------|--------|-----------|-----------------|-----|
| P3-1 | Dead `classify_exception` imports | Imported in 6 files, never called. | **CONFIRMED** | 6 files | Unused imports → code confusion. | Remove unused imports. |
| P3-2 | `gc.collect()` calls | Lines 2396, 2409. Python GC handles this. | **CONFIRMED** | main_bot.py:2396,2409 | Harmless but wasteful. | Remove. |
| P3-3 | Dual session storage fields | `session` vs `session_string` in DB. `safe_session_str()` tries both. | **CONFIRMED** | database.py | Two write paths → potential inconsistency. | Standardize to single field. |
| P3-4 | External HTTP call in auditor | `bluecoys.com/api/telegram-disconnected` at line 2600. | **CONFIRMED** | main_bot.py:2600-2606 | Network I/O in audit cycle → potential timeout. | Move to async background task. |
| P3-5 | `ThreadPoolExecutor` leak | `start_background_testing` creates new executor per call. | **CONFIRMED** | proxy_manager.py:766 | Orphaned executors accumulate. | Reuse single executor. |

---

## Summary

- **Confirmed:** 26 issues (11 P0, 7 P1, 6 P2, 5 P3 + 1 suspected)
- **Suspected:** 1 issue (P0-12: AuthKeyDuplicatedError retry risk — no current retry but no safety net in `acquire()`)

**P0 bypass sites (11 total):**
1. adder.py:321
2. adder.py:384
3. web_console.py:164
4. videochat.py:210
5. videochat.py:392
6. videochat.py:508
7. main_bot.py:561
8. main_bot.py:714
9. main_bot.py:1464
10. main_bot.py:1533
11. dmsender.py:174

**Legitimate client creation sites (2):**
1. session_manager.py:353 (inside `acquire()`)
2. session_manager.py:521 (factory definition)

---

*Audit confirmed. Ready for P0 architecture fix.*
