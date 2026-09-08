# PATCH #9 — DATABASE / MONGODB FINAL HARDENING

## OBJECTIVE

Harden `database.py` (SuiteDatabase) so that every status transition is
atomic, every async hot path is thread-safe, the TTL cache is concurrency-safe,
and secrets (OTP bodies, 2FA passwords, proxy credentials) are never persisted.
Legacy DB-lock API is confirmed dead code and the shutdown path is idempotent.

Only `database.py` was modified, plus the new `test_database_lifecycle.py` and
this report + `DATABASE_LOCK_CALLERS.md`. No other module was touched. No
redesign, no new lock system, no driver migration.

This patch is a hardening pass — **it is not a claim that the database layer is
production-ready**. Remaining risks are listed in section 18.

---

## 1. Atomic transitions

- `set_account_state()` is now a compare-and-set transition: one atomic
  `update_one()` — never a read-then-write pair. Success derives from
  `matched_count`, not from a prior `find_one()`.
- Optional `expected_states` kwarg makes the transition conditional with a
  `{"status": {"$in": [...]}}` filter, so a stale worker can never overwrite a
  newer state (e.g. reactivate `auth_key_duplicated` → `active`).
- The finite set of terminal statuses is
  `TERMINAL_DB_STATUSES = {revoked, banned, deactivated, invalid,
  auth_key_duplicated, permanently_failed, quarantined}`.
- `update_session_status()` writes non-terminal targets only through an
  `{"status": {"$nin": TERMINAL_DB_STATUSES}}` filter; a terminal account can
  never be reactivated by a blind status update (log `STATUS_TRANSITION_REJECTED`).
- `set_account_state_async()` mirrors the sync signature and is safe to await
  from the event loop.

## 2. Async boundaries

- All new async wrappers (`set_account_state_async`,
  `save_pending_session_async`, `save_authorized_session_async`,
  `update_session_status_async`, `mark_account_failed_async`,
  `mark_account_revoked_async`, `update_account_proxy_async`,
  `bulk_set_account_state_async`, `log_received_otp_async`,
  `get_latest_otp_async`) run the sync core inside `run_in_executor`, keeping
  Mongo I/O off the event loop thread (single thread boundary).
- `ensure_connection_async()` propagates `RuntimeError` when the DB is closed.

## 3. Cache synchronization

- `TTLCache` rewritten to be thread-safe: every mutation is guarded by a single
  `threading.RLock`, expiry uses `time.monotonic()`, and eviction always drops
  the matching timestamp so `_timestamps` cannot grow unboundedly.
- `size()` and `cleanup()` added; maxsize eviction is now bounds-exact.
- Session/stats caches are invalidated on every status/proxy/session write
  (including in a `finally` block in bulk paths, regardless of outcome).

## 4. Indexes

- No index changes were made in this patch; `src_accounts.phone`,
  `otp_logs.phone/timestamp` uniqueness/sort reliance is unchanged. Index
  management remains a deployment/ops concern (unchanged from prior patches).

## 5. Phone normalization

- All write paths normalize through `_normalize()` (strips whitespace and
  leading `+`); status values are canonicalized via `_status_value()` to a
  lowercase string whether the caller passes an enum or a string.

## 6. Bulk operations

- New `bulk_set_account_state()` / `bulk_set_account_state_async()`: builds
  conditional `UpdateOne` ops (`upsert=False`, optional `$in` filter), batches
  by `BULK_BATCH_SIZE`, counts matched/modified/duplicate-errors, and always
  invalidates the affected session caches + status-bar cache.
- `DuplicateKeyError` from a raced upsert is recovered (`_upsert_by_phone`),
  so a duplicate insert never leaves a partial state.

## 7. Legacy lock status

- `acquire_lock`, `acquire_lock_async`, `release_lock`, `release_lock_async`,
  `is_locked`, `release_all_locks` all raise `NotImplementedError`.
- Zero runtime callers across the entire codebase (see
  `DATABASE_LOCK_CALLERS.md`). Kept as loud stubs to fail fast on accidental use.

## 8. OTP security

- `log_received_otp()` now persists **metadata only**: phone, sender,
  `message_present`, `message_sha256`, timestamp, `date_received`. The raw OTP
  body is never written to MongoDB.
- `get_latest_otp()` masks the message body on read (`OTP_MESSAGE_MASK`),
  redacting even legacy documents that still carry a plaintext `message`
  field. Reads never surface a code.

## 9. 2FA security

- `save_authorized_session()` never persists the 2FA password; only a
  `has_2fa` boolean is stored. The parameter is kept purely for signature
  compatibility. The main_bot login path already calls it with
  `two_fa_password=None`.

## 10. Session secrets

- Session strings are persisted only in the account document (as before);
  `backup_original_session()` snapshots session strings into
  `session_backups`, unchanged. `save_migrated_session()` unchanged.

## 11. Connection pool

- `_init_mongo()` now forces a finite `socketTimeoutMS`
  (`DATABASE_SOCKET_TIMEOUT_MS`, env `MONGO_SOCKET_TIMEOUT_MS`, default
  20000 ms). PyMongo's default socket timeout is infinite; a stalled server
  can no longer hang the process.
- Pool sizing is unchanged (capped, existing settings).

## 12. Error handling

- `_check_open()` raises a clear `RuntimeError` immediately after `close()`.
- Ops that previously swallowed every failure now surface a deterministic
  closed-state error; non-closed failures keep their conservative
  log-and-return-None / return-False behaviour (callers are unchanged).

## 13. Shutdown / close

- `close()` is idempotent (`_closed` guard, second call is a no-op), tolerates
  `AutoReconnect` during client close, and logs exactly once.
- `_ensure_connection()`, `get_session_by_phone()`,
  `get_active_target_sessions()`, `save_pending_session()`,
  `save_authorized_session()`, `update_session_status()`,
  `mark_account_failed()`, `mark_account_revoked()`, `update_account_proxy()`,
  `log_received_otp()`, `get_latest_otp()` all consult `_check_open()`.

## 14. Tests

`test_database_lifecycle.py` (26 tests):
- Atomic: unconditional + conditional apply, conditional reject, main_bot
  backward-compatible call shape, concurrent single-winner race, terminal
  guard on `update_session_status`.
- Cache: get/set, monotonic expiry, invalidate (key/clear), pattern
  invalidation, maxsize eviction, 8-thread concurrency stress.
- Async: worker-thread offload, 8-way `gather` with no deadlock, closed-DB
  `RuntimeError` propagation through `ensure_connection_async`.
- Lifecycle: idempotent close, AutoReconnect-tolerant close, predictable
  post-close failures.
- Secrets: OTP body never persisted/logged, masked legacy OTP reads, 2FA
  password never persisted, proxy credentials never persisted, upsert
  duplicate-key race recovery.
- Normalization + bulk conditional CAS with cache invalidation.

## 15. Warnings

- `python -m pytest test_database_lifecycle.py -q -W error::RuntimeWarning`
  → **26 passed, 0 warnings**.

## 16. Compile

- `python -m py_compile -q database.py` → **COMPILE_OK**.

## 17. Import

- `python -c "import database; print('DATABASE_IMPORT_OK')"` →
  **DATABASE_IMPORT_OK**.

## 18. Remaining risks (NOT production-ready)

- `backup_original_session()` snapshots session strings into
  `session_backups` — historical backups may still contain secrets created
  before this hardening.
- The 2FA snippet is verified by the caller (`main_bot`) passing
  `two_fa_password=None`; if any future caller passes a real password it is
  still simply dropped (boolean only) — safe, but the caller loses the value.
- `save_migrated_session()` still writes raw `api_hash`/`api_hash` +
  `session_string` into the account document (needed for Telethon auth) and
  was intentionally left unchanged.
- Index management, MongoDB access-control, and network-level encryption are
  deployment concerns outside this patch.
- This patch does **not** claim production readiness for the database layer.