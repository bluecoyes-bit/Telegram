# FINAL_RESOURCE_INVARIANTS

Formal statements of the resource-lifetime invariants, with the enforcement point and the unit-test
evidence for each.

## 0. Core invariants

```
I0  ONE SESSION = ONE ACTIVE CLIENT = ONE ACTIVE OWNER = ONE ACTIVE NETWORK ROUTE
I1  ONE PROXY LEASE = ONE ACTIVE OWNER
I2  TERMINAL ACCOUNT => ZERO client = ZERO proxy = ZERO worker = ZERO retry
I3  `_active_count` <= `_max_active_clients` always.
I4  Lease acquire/release idempotent; release with wrong owner rejected; release(None) no-op.
```

## I0 — Session/client/owner/route uniqueness

- **Enforcement:** `SessionManager._sessions` dict keyed by normalized phone, mutated only under
  the internal `asyncio.Lock`; `_active_count` tracks live client count (new tracked +1, permanent
  removal −1, no change on lease acquire/release). An existing connected client always yields its
  lease (no path returns `None` for an owned, connected client).
- **Evidence:** `test_p0_lifecycle.py` reserve/acquire/release tests assert `lifecycle.value`,
  `owner is None`, and `_active_count` semantics; `_assert_clean_resources` in closeout suite
  confirms zero leaked sessions/clients/leases.

## I1 — Proxy lease uniqueness

- **Enforcement:** `ProxyLeaseManager` per-key lease ownership; auto-reaper releases expired
  leases and clears cooldowns; one active owner per proxy node.
- **Evidence:** proxy_lease tests in `test_p0_closeout.py` (reap/idle/cooldown) validate
  single-owner transitions.

## I2 — Terminal accounts yield nothing

- **Enforcement:** first gate = DB status in `TERMINAL_STATUSES` ⇒ `acquire()` yields `None`
  (with rollback of any reservation), `reserve_login()` returns early, eligibility returns
  `False`, `_create_client` never reached. `AuthKeyDuplicated` maps to terminal with permanent
  removal.
- **Evidence:** `test_p0_lifecycle.py` terminal-skip tests; `test_p0_closeout.py` AuthKey terminal
  policy tests.

## I3 — Capacity bound

- **Enforcement:** `acquire()` raises if `_active_count >= _max_active_clients`.
- **Evidence:** capacity/limit tests in lifecycle suite.

## I4 — Idempotent, owner-safe release

- **Enforcement:** public `SessionManager.release_lease(lease)` (delegates to internal
  `_release_lease`) — idempotent on `None`/already-released; wrong owner logs
  `LEASE_RELEASE_OWNER_MISMATCH` and refuses. Feature modules call only the public method.
- **Evidence:** `test_p0_closeout.py::TestReleaseLeasePublicInterface` (5 tests) including a
  source scan asserting **no** `.release_lease(` on a private symbol in
  adder/dmsender/web_console/videochat/main_bot.

## I5 — Owned client always yields a lease

- On acquire, if an owned tracked client exists it is returned with a lease; there is no
  return-`None` path for an owned, connected client. Verified by lease-acquire success tests.

## Invariant test map

| # | Assertion | File |
|---|-----------|------|
| I0 | active/client/route uniqueness | test_p0_lifecycle.py |
| I1 | proxy lease single owner | test_p0_closeout.py |
| I2 | terminal ⇒ zero resources | test_p0_lifecycle.py, test_p0_closeout.py |
| I3 | capacity ≤ max | test_p0_lifecycle.py |
| I4 | idempotent/owner-safe release | test_p0_closeout.py |
| I5 | owned client yields lease | test_p0_closeout.py |
