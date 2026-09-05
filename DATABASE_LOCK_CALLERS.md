# DATABASE LOCK CALLERS — LEGACY LOCK API AUDIT (PATCH #9)

## Status: LEGACY LOCK API HAS ZERO RUNTIME CALLERS

The lock methods on `SuiteDatabase` are **deprecated stubs that raise
`NotImplementedError`**. Nothing anywhere in the runtime codebase calls them.
The only text occurrences left are the deprecation messages inside the stub
bodies themselves in `database.py`.

## Runtime callers

| Method | database.py | main_bot.py | session_manager.py | account_lease_manager.py | adder / dmsender / videochat / web_console | Runtime callers |
|---|---|---|---|---|---|---|
| `acquire_lock` | stub only | none | none | none | none | **0** |
| `acquire_lock_async` | stub only | none | none | none | none | **0** |
| `release_lock` | stub only | none | none | none | none | **0** |
| `release_lock_async` | stub only | none | none | none | none | **0** |
| `is_locked` | stub only | none | none | none | none | **0** |
| `release_all_locks` | stub only | none | none | none | none | **0** |

## Evidence

Static search across `database.py`, `main_bot.py`, `session_manager.py`,
`account_lease_manager.py`, `adder.py`, `dmsender.py`, `videochat.py`,
`web_console.py` for the patterns
`db.acquire_lock / database.acquire_lock / self.db.release_lock / ...`:

- The only matches are inside `database.py` `NotImplementedError` messages
  (lines 435–470):
  - `database.acquire_lock is DEPRECATED ...`
  - `database.acquire_lock_async is DEPRECATED ...`
  - `database.release_lock is DEPRECATED ...`
  - `database.release_lock_async is DEPRECATED ...`
  - `database.is_locked is DEPRECATED ...`
  - `database.release_all_locks is DEPRECATED ...`
- No production module imports or calls any lock method.

## Why they can stay as raising stubs

Session ownership/lease rules are enforced by `SessionManager` and
`AccountLeaseManager` (in-process ownership model), so DB-level locks are an
abandoned design. Keeping the stubs guarantees any accidental caller gets an
immediate, loud `NotImplementedError` instead of silent no-op behaviour.

## Tests covering this

- `test_database_lifecycle.py` — static audit only (legacy lock methods raise
  `NotImplementedError`); no runtime lock-path tests exist because there is no
  runtime lock path.