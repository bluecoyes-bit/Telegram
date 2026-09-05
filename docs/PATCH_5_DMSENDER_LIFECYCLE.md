# DMSENDER PATCH #5 — COMPLETE

## SUMMARY
Rewrote `dmsender.py` so the DM engine uses only the validated SessionManager /
ProxyLeaseManager lifecycle. Resource starvation and session contention are now
treated as WAITING states (never target failure), `AuthKeyDuplicated` quarantines
the account, workers are cancelled deterministically with every context body
unwound, and the wizard-state store is bounded, TTL-managed, and dict-compatible
with `main_bot.py`. Proven by a dedicated 22-test suite plus the full lifecycle
regression set.

---

## P0 FIXED
- **Context-manager lifecycle only**: all session usage goes through
  `async with self.session_manager.acquire(..., module="dmsender", auto_release=True)`.
  No `await self.session_manager.acquire(...)` in the worker lifecycle; the sole
  runtime `acquire()` call is the `async with` at line 879.
- **`_record_result` TypeError**: was invoked with `await` on a synchronous method
  (`TypeError: object NoneType can't be used in 'await' expression` after the first
  successful send). Now called as `self._record_result(...)`.
- **`dict.move_to_end` removed**: `_WizardStateStore` uses a plain `dict` (no
  `move_to_end`, which only exists on `OrderedDict`). Eviction is insertion-ordered
  via `while len(self) > self.max_items: dict.__delitem__(self, next(iter(self)))`.
- **`execute_dm_campaign` returns a completion message**: `_dynamic_rolling_worker`
  now `return final_msg` (previously returned `None`, so the campaign wrapper
  returned nothing).
- **No infinite requeue hang**: when `candidate_accounts` is empty (e.g. every
  account quarantined mid-campaign) `_handle_target` emits `TARGET_FAILED`,
  increments `stats["failed"]`, emits `send_failure`, and returns without requeue.

---

## P1 FIXED
- **SessionAlreadyOwnedError is a WAITING condition**: raised/caught during
  acquisition → `WAITING_FOR_ACCOUNT` + contended counter, bounded wait, requeue,
  and retry. Never recorded as a target failure.
- **Lease starvation is a WAITING condition**: `lease is None` →
  `WAITING_FOR_PROXY` (when the proxy snapshot count is 0) or `WAITING_FOR_ACCOUNT`
  otherwise; bounded via `_resource_wait_bounds` and `_busy_exclusion_seconds`.
- **AuthKeyDuplicated quarantine**: → `TERMINAL_ACCOUNT`,
  `await session_manager.mark_quarantined(phone, reason, category)`, removed from
  the candidate pool, and never retried.
- **Deterministic cancellation/shutdown**: `halt_campaign()` sets `is_running =
  False`, cancels the worker tasks, and unwinds every open `async with` body. The
  reporter starts after the workers, runs `while self.is_running`, and is cancelled
  in the finalizer.
- **Static lifecycle hygiene**: no direct `TelegramClient(` construction, no
  `_create_client(`, no `._release_lease(` / `release_proxy(` calls in the worker
  lifecycle, no DB runtime lock calls, and no `time.sleep(` in async code.

---

## CHANGES MADE

### `dmsender.py` (rewritten, 1196 lines)
- Worker lifecycle wrapped end-to-end in the SessionManager acquire context
  (single source of client/lease cleanup via `auto_release=True`).
- `_WizardStateStore`: `max_items=1000`, `ttl=1800`, insertion-order eviction,
  full `dict` API compatibility (`save_state`/`load_state`/`delete_many` retained).
- `_dynamic_rolling_worker` / `_handle_target` / `_handle_operation_error`:
  resource-exhaustion vs terminal-error split, quarantine path, bounded waits,
  entry counters (`queued`/`inflight`/`accepted`), `ROOT_CAUSE_SENT_SUCCESS` /
  `ROOT_CAUSE_RESOURCE` / `ROOT_CAUSE_TERMINAL` classification keys.
- `compute_dm_worker_capacity` behavior preserved exactly (imported by
  `test_p0_closeout.py`); capacity uses `get_available_count_async()` when present.
- Compatibility kept: `EnterpriseDMSender(db, ...)` mutable signature,
  `_generate_live_status()` (async), `_emit()`, `get_lifecycle_events()`,
  `reset_stats()`, `halt_campaign()`, `setup_dmsender_handlers(bot, db, ...)`.

### `test_dmsender_lifecycle.py` (new)
- `ScriptedAcquire` — a real `@asynccontextmanager` mock of `SessionManager.acquire`
  that records enter/exit/max-concurrency and supports three outcome conventions:
  raises directly, returns an `Exception` instance, returns `None`, or returns a
  lease. The direct-raise path decrements `current` so raises at `__aenter__`
  never leak a "body" count.
- 22 tests: static contract (6), eligibility prefilter (1), context-manager
  lifecycle (2), resource starvation (2), AuthKey quarantine (1), reporter and
  completion (2), campaign control (1), worker capacity (2), wizard state (2),
  contention regression (1), resource cleanup (2).

---

## VALIDATION RESULTS

### Dedicated suite
```bash
python -m pytest test_dmsender_lifecycle.py -v
# 22 passed in ~2s
```

### Full regression set (run individually)
```bash
python -m pytest test_adder_lifecycle.py -q    # 15 passed
python -m pytest test_p0_lifecycle.py -q       # 33 passed
python -m pytest test_proxy_lease.py -q        # 7 passed
# Combined: 55 passed
```

### Compile + import
```bash
python -m compileall dmsender.py     # no syntax errors
python -c "import dmsender; print('DMSENDER_IMPORT_OK')"
# DMSENDER_IMPORT_OK
```

### Static audit
`grep (incl. dmsender.py)` for forbidden patterns:
`release_proxy(`, `_create_client(`, `._release_lease(`, `acquire_lock(`,
`release_lock(`, `is_locked(`, `await self.session_manager.acquire(`, `time.sleep(`,
`client.disconnect(`, `TelegramClient(` → only docstring prose matches (verified
with the module docstring stripped). Sole runtime session call is
`async with self.session_manager.acquire(` at line 879.

### Contention regression (P1 #10 / "wait, don't fail")
20 targets / 14 accounts / 10 proxies with 8 scripted SessionAlreadyOwned
contentions, account starvation and proxy exhaustion windows: engine emits
WAITING events, resumes after resources return, sends all 20 targets, records
`failed == 0`, and unwinds every context (`current == 0` after completion).

---

## REGRESSION NOTE (pre-existing, out of scope)
`test_p0_closeout.py` has 3 failing tests:
- `TestProxyMatrix::test_all_proxies_busy_returns_none`
- `TestShutdownWithResources::test_shutdown_returns_all_to_zero`
- `TestReleaseLeasePublicInterface::test_release_lease_wrong_owner_rejected`

These exercise SessionManager/ProxyLeaseManager teardown semantics (lease counts,
`disconnect_all`, owner rejection) and do NOT use `dmsender`. They were reproduced
identically with the **original `dmsender.py` restored from git** (3 failed in
2.25s), i.e. they are independent of PATCH #5. Fixing them requires touching
`session_manager.py` / `proxy_manager.py`, which is out of scope for this patch.

---

## FILES MODIFIED
- `dmsender.py` (rewritten)
- `test_dmsender_lifecycle.py` (new)

## FILES NOT TOUCHED
- `session_manager.py`, `proxy_manager.py`, `adder.py`, `main_bot.py`,
  `database.py`, `videochat.py` — all unchanged by this patch.

---

## NEXT STEPS
1. If desired, a separate out-of-scope patch addresses the 3 pre-existing
   `test_p0_closeout.py` teardown failures (session/proxy manager semantics).
2. Manual smoke of a real campaign (`execute_dm_campaign`) with live credentials.

---

## PATCH STATUS: COMPLETE