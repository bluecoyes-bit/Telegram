"""
PATCH #9 Test Suite — database / MongoDB final hardening.

Covers the SuiteDatabase hardening delivered in PATCH #9:
  - Atomic conditional status transitions (compare-and-set) and the
    single-winner guarantee under concurrent writers.
  - Terminal-status immutability through the blind update path.
  - Thread-safe TTL cache (monotonic timestamps, maxsize eviction,
    pattern invalidation, concurrency stress).
  - Async hot paths that offload blocking Mongo I/O to a worker thread
    (run_in_executor) with no loop blocking / no deadlock under gather.
  - Secret hygiene: OTP bodies are never persisted nor logged; 2FA
    passwords are never persisted; proxy passwords/usernames are never
    persisted; masked OTP reads redact legacy records.
  - Lifecycle: close() is idempotent, tolerates AutoReconnect, and every
    operation fails predictably after close.
  - Phone normalization and bulk compare-and-set batching.

Run:        python -m pytest test_database_lifecycle.py -v
Baselines:  python -m pytest test_main_bot_lifecycle.py \
                test_p0_*.py test_proxy_lease.py \
                test_adder_lifecycle.py test_dmsender_lifecycle.py \
                test_videochat_lifecycle.py -q
"""
import asyncio
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum

import pytest
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

pytest_plugins = ["pytest_asyncio"]

import database


class AccountStatus(str, Enum):
    """Mirror of main_bot.AccountStatus for the backward-compat call shape."""

    AUTH_KEY_DUPLICATED = "auth_key_duplicated"
    REVOKED = "revoked"


# ---------------------------------------------------------------------------
# Atomic-in-memory Mongo fakes
# ---------------------------------------------------------------------------

class _UpdateResult:
    def __init__(self, matched=0, modified=0, upserted_id=None):
        self.matched_count = matched
        self.modified_count = modified
        self.upserted_id = upserted_id


class _BulkResult:
    def __init__(self, matched=0, modified=0, upserted=0, upserted_ids=()):
        self.matched_count = matched
        self.modified_count = modified
        self.upserted_count = upserted
        self.upserted_ids = upserted_ids


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    def sort(self, key, direction):
        self._docs.sort(
            key=lambda d: d.get(key, 0),
            reverse=(direction == -1),
        )
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)


class FakeCollection:
    """Thread-safe in-memory Mongo stand-in with per-document atomic updates.

    update_one serializes under a lock and applies the filter exactly like a
    single atomic write in MongoDB, so compare-and-set races are preserved.
    Optional ``race_upserts`` simulates the upsert duplicate-key insert race.
    """

    def __init__(self, docs=None, sleep=0.0, record_thread=False,
                 thread_ids=None, race_upserts=None):
        self._docs = [dict(d) for d in (docs or [])]
        self._lock = threading.RLock()
        self._sleep = float(sleep)
        self._record_thread = record_thread
        self._thread_ids = thread_ids if thread_ids is not None else []
        self._race_upserts = dict(race_upserts or {})

    @property
    def docs(self):
        with self._lock:
            return [dict(d) for d in self._docs]

    def _annotate_thread(self):
        if self._record_thread:
            self._thread_ids.append(threading.get_ident())

    def _maybe_sleep(self):
        if self._sleep:
            time.sleep(self._sleep)

    def by_phone(self, phone):
        for d in self.docs:
            if d.get("phone") == phone:
                return d
        return None

    def set_status(self, phone, status):
        with self._lock:
            for d in self._docs:
                if d.get("phone") == phone:
                    d["status"] = status
                    return True
            return False

    @staticmethod
    def _matches(doc, query):
        if not query:
            return True
        for key, cond in query.items():
            if isinstance(cond, dict):
                inn = cond.get("$in")
                nin = cond.get("$nin")
                if inn is not None and doc.get(key) not in inn:
                    return False
                if nin is not None and doc.get(key) in nin:
                    return False
                continue
            if doc.get(key) != cond:
                return False
        return True

    def find_one(self, query=None, projection=None):
        self._maybe_sleep()
        self._annotate_thread()
        with self._lock:
            for d in self._docs:
                if self._matches(d, query or {}):
                    if projection:
                        return {
                            k: d.get(k) for k in projection if projection[k]
                        }
                    return dict(d)
            return None

    def find(self, query=None):
        self._maybe_sleep()
        self._annotate_thread()
        with self._lock:
            docs = [dict(d) for d in self._docs
                    if self._matches(d, query or {})]
        return _FakeCursor(docs)

    def insert_one(self, doc):
        self._maybe_sleep()
        self._annotate_thread()
        with self._lock:
            self._docs.append(dict(doc))
            return None

    @staticmethod
    def _apply_set(doc, update):
        if "$set" in update:
            doc.update(update["$set"])

    def update_one(self, query, update, upsert=False):
        self._maybe_sleep()
        self._annotate_thread()
        with self._lock:
            for d in self._docs:
                if self._matches(d, query):
                    self._apply_set(d, update)
                    return _UpdateResult(matched=1, modified=1)
            if upsert:
                phone = query.get("phone")
                if phone and self._race_upserts.pop(phone, False):
                    self._docs.append({"phone": phone})
                    raise DuplicateKeyError("simulated upsert insert race")
                new_doc = {}
                for k, v in query.items():
                    if not isinstance(v, dict):
                        new_doc[k] = v
                new_doc.update(update.get("$set", {}))
                new_doc.update(update.get("$setOnInsert", {}))
                self._docs.append(new_doc)
                return _UpdateResult(matched=0, modified=0,
                                     upserted_id=new_doc.get("_id"))
            return _UpdateResult(matched=0, modified=0)

    def bulk_write(self, operations, ordered=False):
        self._maybe_sleep()
        self._annotate_thread()
        matched = modified = upserted = 0
        with self._lock:
            for op in operations:
                query = op._filter
                update = op._doc
                ups = getattr(op, "_upsert", False)
                hit = next(
                    (d for d in self._docs if self._matches(d, query)), None
                )
                if hit is not None:
                    matched += 1
                    modified += 1
                    self._apply_set(hit, update)
                elif ups and query.get("phone"):
                    new_doc = {"phone": query["phone"]}
                    new_doc.update(update.get("$set", {}))
                    new_doc.update(update.get("$setOnInsert", {}))
                    self._docs.append(new_doc)
                    matched += 1
                    upserted += 1
        return _BulkResult(matched=matched, modified=modified,
                           upserted=upserted)


def make_db(src_docs=None, otp_docs=None, sleep=0.0, record_thread=False,
            thread_ids=None, race_upserts=None):
    """Build a SuiteDatabase instance without touching __init__/_init_mongo."""
    db = database.SuiteDatabase.__new__(database.SuiteDatabase)
    db._closed = False
    db._session_cache = database.TTLCache(maxsize=256, ttl=30)
    db._stats_cache = database.TTLCache(maxsize=8, ttl=30)
    db.src_accounts = FakeCollection(
        docs=src_docs, sleep=sleep, record_thread=record_thread,
        thread_ids=thread_ids, race_upserts=race_upserts,
    )
    db.session_backups = FakeCollection()
    db.otp_logs = FakeCollection(docs=otp_docs)
    db.scraped_members = FakeCollection()
    db.client = None
    return db


# ---------------------------------------------------------------------------
# 1. Atomic conditional status transitions
# ---------------------------------------------------------------------------

def test_unconditional_set_account_state_single_atomic_update():
    db = make_db(src_docs=[{"phone": "1", "status": "active"}])
    assert db.set_account_state("1", "banned") is True
    assert db.src_accounts.by_phone("1")["status"] == "banned"


def test_conditional_set_account_state_applies_for_expected_state():
    db = make_db(src_docs=[{"phone": "1", "status": "active"}])
    ok = db.set_account_state("1", "inactive", expected_states=["active"])
    assert ok is True
    assert db.src_accounts.by_phone("1")["status"] == "inactive"


def test_conditional_set_account_state_rejected_when_status_not_expected():
    db = make_db(src_docs=[{"phone": "1", "status": "auth_key_duplicated"}])
    ok = db.set_account_state(
        "1", "active", expected_states=["active", "inactive"]
    )
    assert ok is False
    assert db.src_accounts.by_phone("1")["status"] == "auth_key_duplicated"


def test_set_account_state_backward_compatible_main_bot_call_shape():
    db = make_db(src_docs=[{"phone": "1", "status": "active"}])
    ok = db.set_account_state("1", AccountStatus.AUTH_KEY_DUPLICATED)
    assert ok is True
    assert (db.src_accounts.by_phone("1")["status"]
            == AccountStatus.AUTH_KEY_DUPLICATED.value)


def test_concurrent_conditional_transitions_have_single_winner():
    db = make_db(src_docs=[{"phone": "1", "status": "active"}])
    barrier = threading.Barrier(2)
    flags = []

    def worker(target):
        barrier.wait()
        flags.append(db.set_account_state(
            "1", target, expected_states=["active"]
        ))

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(worker, "revoked")
        f2 = pool.submit(worker, "quarantined")
        f1.result()
        f2.result()

    assert flags.count(True) == 1
    assert db.src_accounts.by_phone("1")["status"] in ("revoked", "quarantined")


def test_update_session_status_never_reactivates_terminal():
    db = make_db(src_docs=[{
        "phone": "1", "status": "auth_key_duplicated", "session_string": "sess",
    }])
    db.update_session_status("1", "active", "new-session")
    doc = db.src_accounts.by_phone("1")
    assert doc["status"] == "auth_key_duplicated"
    assert doc.get("session_string") == "sess"

    db.update_session_status("1", "revoked")
    assert db.src_accounts.by_phone("1")["status"] == "revoked"


# ---------------------------------------------------------------------------
# 2. Thread-safe TTL cache
# ---------------------------------------------------------------------------

def test_ttl_cache_set_get():
    cache = database.TTLCache(maxsize=16, ttl=60)
    cache.set("a", {"x": 1})
    assert cache.get("a") == {"x": 1}
    assert cache.get("missing") is None


def test_ttl_cache_expiry_purges_and_removes_timestamp():
    cache = database.TTLCache(maxsize=16, ttl=0.05)
    cache.set("a", 1)
    time.sleep(0.1)
    assert cache.get("a") is None
    cache.cleanup()
    assert cache.size() == 0


def test_ttl_cache_invalidate_key_and_clear():
    cache = database.TTLCache(maxsize=16, ttl=60)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.invalidate("a")
    assert cache.get("a") is None
    assert cache.get("b") == 2
    cache.invalidate()
    assert cache.get("b") is None


def test_ttl_cache_invalidate_pattern():
    cache = database.TTLCache(maxsize=32, ttl=60)
    cache.set("session:1", "s1")
    cache.set("session:2", "s2")
    cache.set("stats:status_bar", {})
    cache.invalidate_pattern("session:")
    assert cache.get("session:1") is None
    assert cache.get("session:2") is None
    assert cache.get("stats:status_bar") is not None


def test_ttl_cache_maxsize_eviction_bounds_size():
    cache = database.TTLCache(maxsize=2, ttl=60)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    assert cache.size() == 2
    assert cache.get("a") is None
    assert cache.get("c") == 3


def test_ttl_cache_thread_safe_under_concurrency():
    cache = database.TTLCache(maxsize=64, ttl=30)
    errors = []

    def worker(idx):
        try:
            for j in range(200):
                key = f"{idx}:{j % 17}"
                cache.set(key, j)
                cache.get(key)
                cache.invalidate_pattern(f"{idx}:")
        except Exception as exc:  # pragma: no cover - failure reporter
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert cache.size() <= 64


# ---------------------------------------------------------------------------
# 3. Async boundaries (worker-thread offload, no deadlock, closed guard)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_hot_path_offloaded_to_worker_thread():
    thread_ids = []
    db = make_db(
        src_docs=[{"phone": "5550011", "status": "active"}],
        record_thread=True,
        thread_ids=thread_ids,
    )
    loop_thread = threading.get_ident()
    doc = await db.get_session_by_phone_async("5550011")
    assert doc is not None
    assert doc["status"] == "active"
    assert thread_ids, "fake collection never executed"
    assert all(t != loop_thread for t in thread_ids)


@pytest.mark.asyncio
async def test_async_concurrent_writes_no_deadlock():
    db = make_db(
        sleep=0.02,
        src_docs=[{"phone": f"{i}000000{i}", "status": "inactive"}
                  for i in range(8)],
    )
    phones = [f"{i}000000{i}" for i in range(8)]

    async def one(phone, idx):
        started = time.monotonic()
        flag = await db.set_account_state_async(
            phone, "revoked" if idx % 2 else "active"
        )
        return time.monotonic() - started, flag

    results = await asyncio.gather(*(one(p, i) for i, p in enumerate(phones)))
    assert all(flag is True for _, flag in results)
    assert sum(elapsed for elapsed, _ in results) < 3.0


@pytest.mark.asyncio
async def test_ensure_connection_async_propagates_closed_error():
    db = make_db()
    db._closed = True
    with pytest.raises(RuntimeError):
        await db.ensure_connection_async()


# ---------------------------------------------------------------------------
# 4. Lifecycle: idempotent close + predictable post-close failures
# ---------------------------------------------------------------------------

class _FakeClient:
    def __init__(self, fail=False):
        self.closed = 0
        self._fail = fail

    def close(self):
        self.closed += 1
        if self._fail:
            from pymongo.errors import AutoReconnect
            raise AutoReconnect("already disconnected")


def test_close_is_idempotent(caplog):
    db = make_db()
    db.client = _FakeClient()
    caplog.set_level(logging.INFO, logger="SuiteDatabase")
    db.close()
    db.close()
    assert db.src_accounts is not None
    messages = [r.getMessage() for r in caplog.records]
    assert messages.count("MongoDB connection closed.") == 1


def test_close_tolerates_autoreconnect(caplog):
    db = make_db()
    db.client = _FakeClient(fail=True)
    caplog.set_level(logging.WARNING, logger="SuiteDatabase")
    db.close()
    db.close()
    assert any("AutoReconnect" in r.getMessage() or
               "already disconnected" in r.getMessage().lower()
               for r in caplog.records)


def test_closed_database_fails_predictably():
    db = make_db(src_docs=[{"phone": "1", "status": "active"}])
    db._closed = True

    with pytest.raises(RuntimeError):
        db.set_account_state("1", "banned")
    with pytest.raises(RuntimeError):
        db.get_session_by_phone("1")
    with pytest.raises(RuntimeError):
        db.save_authorized_session(
            "1", "sess", "active", {"device_model": "X"}
        )
    with pytest.raises(RuntimeError):
        db.log_received_otp("1", "Telegram", "000000")


# ---------------------------------------------------------------------------
# 5. Secret hygiene (OTP / 2FA / proxy)
# ---------------------------------------------------------------------------

def test_otp_body_never_persisted_nor_logged(caplog):
    db = make_db()
    caplog.set_level(logging.WARNING, logger="SuiteDatabase")
    secret = "SECRET-OTP-CODE-12345"
    db.log_received_otp("+39 55566 778899", "Telegram", f"Your code: {secret}")

    stored = db.otp_logs.docs[0]
    blob = json.dumps(stored, default=str)
    assert secret not in blob
    assert "Your code" not in blob
    assert stored["message_present"] is True
    assert "message" not in stored
    assert stored.get("message_sha256") == database.SuiteDatabase._sha256_hex(
        f"Your code: {secret}"
    )
    assert "message_raw" not in stored

    for rec in caplog.records:
        assert secret not in rec.getMessage()


def test_otp_read_masks_legacy_plaintext():
    db = make_db(otp_docs=[{
        "phone": "555", "message": "LEGACY-PLAINTEXT-CODE",
        "message_raw": "raw", "timestamp": 100, "date_received": "now",
    }])
    doc = db.get_latest_otp("555")
    assert doc["message"] == database.OTP_MESSAGE_MASK
    assert "LEGACY-PLAINTEXT-CODE" not in json.dumps(doc)


def test_authorized_session_never_persists_2fa_password():
    db = make_db()
    db.save_authorized_session(
        "5557788", "SESSIONSTRING", "active",
        {"device_model": "PC 64bit", "system_version": "Windows 11",
         "app_version": "4.8.4"},
        two_fa_password="SuperSecret2FA99",
    )
    stored = db.src_accounts.by_phone("5557788")
    assert stored is not None
    assert stored["has_2fa"] is True
    assert stored["status"] == "active"
    assert "SuperSecret2FA99" not in json.dumps(stored, default=str)


def test_proxy_credentials_never_persisted():
    db = make_db(src_docs=[{"phone": "5557788", "status": "active"}])
    db.update_account_proxy(
        "5557788",
        {
            "provider": "proxy6", "host": "geo.example", "port": 8080,
            "scheme": "socks5", "username": "proxyuser1",
            "password": "proxypassX",
        },
    )
    proxy = db.src_accounts.by_phone("5557788")["proxy"]
    assert proxy["host"] == "geo.example"
    assert proxy["port"] == 8080
    assert proxy["username_present"] is True
    assert proxy["password_present"] is True
    blob = json.dumps(proxy)
    assert "proxyuser1" not in blob
    assert "proxypassX" not in blob


def test_upsert_duplicate_key_race_is_recovered():
    db = make_db(race_upserts={"5557788": True})
    db.save_authorized_session(
        "5557788", "SESSIONSTRING", "active",
        {"device_model": "PC 64bit", "system_version": "Windows 11",
         "app_version": "4.8.4"},
        two_fa_password=None,
    )
    stored = db.src_accounts.by_phone("5557788")
    assert stored["session_string"] == "SESSIONSTRING"
    assert stored["status"] == "active"


# ---------------------------------------------------------------------------
# 6. Normalization and bulk operations
# ---------------------------------------------------------------------------

def test_phone_normalization_on_write_paths():
    db = make_db(src_docs=[
        {"phone": "1(555)000-0000", "status": "active"},
    ])
    ok = db.set_account_state("+1 (555) 000-0000", "banned")
    assert ok is True
    assert db.src_accounts.by_phone("1(555)000-0000") is not None


def test_bulk_set_account_state_conditional_and_cache_invalidation():
    db = make_db(src_docs=[
        {"phone": "1", "status": "active"},
        {"phone": "2", "status": "active"},
        {"phone": "3", "status": "active"},
        {"phone": "4", "status": "revoked"},
    ])
    for p in ("1", "2", "3", "4"):
        db._session_cache.set(f"session:{p}", {"status": "x"})

    res = db.bulk_set_account_state(
        [("2", "banned"), ("3", "quarantined"), ("4", "active")],
        expected_states=["active"],
    )
    assert res["matched"] == 2
    assert res["modified"] == 2
    assert db.src_accounts.by_phone("2")["status"] == "banned"
    assert db.src_accounts.by_phone("3")["status"] == "quarantined"
    assert db.src_accounts.by_phone("4")["status"] == "revoked"
    for p in ("2", "3", "4"):
        assert db._session_cache.get(f"session:{p}") is None
    assert db._stats_cache.get("status_bar") is None


def test_bulk_set_account_state_without_expected_is_unconditional():
    db = make_db(src_docs=[{"phone": "1", "status": "revoked"}])
    res = db.bulk_set_account_state([("1", "banned")])
    assert res["matched"] == 1
    assert db.src_accounts.by_phone("1")["status"] == "banned"