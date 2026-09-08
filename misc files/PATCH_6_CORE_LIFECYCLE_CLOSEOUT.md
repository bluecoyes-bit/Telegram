# CORE PATCH #6 — CORE LIFECYCLE CLOSEOUT

## ORIGINAL FAILURES

Run: `python -m pytest test_p0_closeout.py -v`

```text
1. FAILED TestProxyMatrix::test_all_proxies_busy_returns_none
   E AssertionError: session leases != 0  (line 196, _assert_clean_resources)
2. FAILED TestShutdownWithResources::test_shutdown_returns_all_to_zero
   E AssertionError: assert 20 == 0      (line 612, len(plm._leased))
3. FAILED TestReleaseLeasePublicInterface::test_release_lease_wrong_owner_rejected
   E AssertionError: session leases != 0  (line 196, _assert_clean_resources)
```

Result: `3 failed, 37 passed`.

---

## ROOT CAUSES

### 1. `SessionManager.disconnect_all()` was dead code
The method built an **empty** `snapshots = []` list and then iterated over that
empty list in both its lock-held loop and its I/O loop. Nothing was ever
snapshotted, so:

- clients were never disconnected (`_active_count` was force-set to `0` while
  the real clients stayed alive),
- session leases were never cleared (`lifecycle` stayed `BUSY`),
- proxy leases were never released (`plm._leased` stayed full at 10/20/1).

This single defect accounted for all three failures:
- `test_all_proxies_busy_returns_none` -> teardown `_assert_clean_resources` saw
  the 10 busy sessions still leased.
- `test_shutdown_returns_all_to_zero` -> after `disconnect_all()`,
  `plm._leased` still held 20 entries.
- `test_release_lease_wrong_owner_rejected` -> the forged release was correctly
  rejected (m1 stayed BUSY), but the final `_assert_clean_resources` could not
  clean the still-live client/proxy because `disconnect_all` did nothing.

### 2. `release_lease()` mutated the lease object before ownership validation
The public API set `lease.released = True` **before** `_release_lease` could
reject a wrong-owner/stale caller. Validation now precedes any mutation, so a
rejected release leaves the lease, session, client, proxy, and all counters
untouched and a legitimate release of the same lease impossible-to-be-shadowed.

### 3. Proxy release keyed on `proxy_url`, but the real `ProxyLeaseManager`
records carry `__proxy_id` / `__lease_id` and no `url` key
`SessionInfo.proxy_url` was therefore `None` under the real lease manager, so
every release path that guarded on `if proxy_url:` silently skipped the proxy
release. Fix applied to all six paths so a proxy is released when either its
`proxy_url` **or** its `proxy_id` is known. The real `ProxyLeaseManager.release_proxy`
looks up by `proxy_id` and validates the `lease_id` epoch, so precision is kept.

Network-record detail: `acquire_proxy()` returns `to_telethon_proxy()` (a dict
with `proxy_type/addr/port/...`) plus internal `__proxy_id`/`__lease_id` — it
has never had a `url` field; that is why the `proxy_id`-keyed release is the
authoritative path and the whole reason requirement #5 ("do not release by URL
when a precise proxy_id exists") mattered here.

---

## FIXES

### `session_manager.py`
- **`disconnect_all()` rewritten** (two-phase, no I/O under the lock):
  1. snapshot every tracked session's `client`, `proxy_url`, `proxy_id`,
     `proxy_lease_id` and **atomically** clear ownership/fields;
  2. set `_active_count = 0`;
  3. outside the lock, disconnect each client, then release each proxy using
     the captured `proxy_id`/`lease_id` + `phone`.
  Preserves `QUARANTINED`/`TERMINAL` lifecycles (never flips back to
  `AVAILABLE`); non-terminal sessions become `DISCONNECTED`. Idempotent: a
  second call snapshots an empty set.
- **`release_lease()` reordered** — ownership + lease-epoch validation happens
  BEFORE any mutation; `lease.released` is only set once ownership is confirmed.
  No counter/lifecycle/client/proxy change on rejection.
- **Proxy release guards corrected** in `_release_lease`, `mark_quarantined`,
  `release_login`, `_rollback_acquire_failure`, `build_login_client` rollback,
  and `disconnect_all`: release runs when `proxy_url or proxy_id` is present,
  always passing `proxy_id` + `lease_id` to the lease manager.

### `proxy_manager.py`
- **No change required.** `acquire_proxy()` already uses a
  `Condition`-based wait with a hard deadline (no busy-loop), only increments
  `current_active_leases` on a successful acquire, and `release_proxy()` already
  validates node-exists -> is-leased -> `leased_to == phone` -> `lease_id` epoch
  BEFORE releasing, cooldown, or decrementing counters. `current_active_leases`
  is bounded `0 <= n <= total_proxy_nodes` and matches the actual leased set.

### Files NOT touched
`dmsender.py`, `adder.py`, `videochat.py`, `main_bot.py`, `database.py`,
`web_console.py` — none modified.

---

## TESTS

### New regression tests (wrong-owner / stale-lease / double release / all-busy)
- `test_p0_closeout.py::TestReleaseLeasePublicInterface::test_release_lease_correct_owner_stale_lease_id_rejected`
  — correct owner + stale lease epoch -> rejected, session stays `BUSY`,
  `_active_count` unchanged.
- `test_p0_closeout.py::TestResourceInvariants::test_session_and_proxy_counts_match`
  — real `ProxyLeaseManager` + real `SessionManager`: `_active_count` == live
  tracked clients, `stats["current_active_leases"]` == actual `node.is_leased`
  set, bounds `0 <= n <= total_proxy_nodes`, shutdown returns both to `0`, and
  repeated shutdown is idempotent.
- `test_proxy_lease.py::test_correct_owner_release_frees_node_and_counter`
  — correct owner + correct lease -> node freed, counter `0`.
- `test_proxy_lease.py::test_all_proxies_busy_timeout_returns_none_counter_stable`
  — N proxies all leased, next acquire waits then times out -> `None`,
  `current_active_leases == N`, existing lease node untouched.
- `test_p0_lifecycle.py::TestGracefulShutdown::test_disconnect_all_preserves_quarantined_state`
  — shutdown must not flip QUARANTINED back to AVAILABLE.

(Existing coverage already exercised: `release_lease` correct-owner, idempotent
double release, `None` no-op, wrong-owner; PLM wrong-account / stale-epoch /
double-release.)

### Suite matrix (all expected 0 failures)
```text
test_p0_closeout.py         42 passed
test_p0_lifecycle.py        34 passed
test_proxy_lease.py          9 passed
test_adder_lifecycle.py     15 passed
test_dmsender_lifecycle.py  22 passed
```

### Compile / import
```text
python -m compileall session_manager.py proxy_manager.py   -> clean
python -c "import session_manager, proxy_manager; ..."     -> CORE_IMPORT_OK
```

---

## FINAL RESOURCE INVARIANTS
- `SessionManager._active_count == actual tracked live clients` (verified +
  enforced by `validate_invariants`).
- `ProxyLeaseManager.stats["current_active_leases"] == count(nodes where
  node.is_leased)` — verified; bounded `0 <= n <= total_proxy_nodes`.
- Wrong-owner / stale-lease / double-release -> no lifecycle mutation, no
  disconnect, no proxy release, no counter change.
- `disconnect_all()` idempotent: repeated calls leave `_active_count == 0`,
  `current_active_leases == 0`; never negative counters.
- Release ORDER: clear ownership atomically -> disconnect clients -> release
  matching proxy leases (by `proxy_id`/`lease_id`, not URL-only) -> counters
  finalized.
- Shutdown preserves QUARANTINED/TERMINAL (never AVAILABLE).

---

## REMAINING ISSUES
- None in the patched scope. The three regressions are closed; all five suites
  pass with 0 failures.
- Out-of-scope notes (unchanged by this patch): no live-network smoke test was
  performed; authored ProxyNode/PLM providers load only when their env
  credentials are set. The repository is **not** hereby declared
  production-ready.

## PATCH STATUS: COMPLETE