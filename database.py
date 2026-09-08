#!/usr/bin/env python3
"""
Filename: database.py
"""

import time
import re
import os
import time
import re
import pickle
import random
import pathlib
import json
import logging
import asyncio
import hashlib
import threading
import functools
import importlib
from functools import partial
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Generator, Union, Collection
from collections import OrderedDict

from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING, DeleteMany
from pymongo.errors import (
    BulkWriteError, AutoReconnect, ServerSelectionTimeoutError,
    ConnectionFailure, NetworkTimeout, OperationFailure, DuplicateKeyError
)

from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from config import CONFIG, DEVICE_PROFILES, MONGODB_SETTINGS, MONGO_CFG

logger = logging.getLogger("SuiteDatabase")

# ────────────────────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────────────────────
BULK_BATCH_SIZE = 500          # Documents per bulk_write
CURSOR_BATCH_SIZE = 200        # Documents fetched per cursor batch
LOCK_TTL_SECONDS = 7200         # Auto-expire locks after 5 min
CACHE_TTL_SECONDS = 30         # Status bar / stats cache
MAX_PROJECTION_FIELDS = {      # Always fetch only what's needed
    "phone": 1, "status": 1, "session": 1, "session_string": 1,
    "device_model": 1, "system_version": 1, "app_version": 1,
    "api_id": 1, "api_hash": 1, "device_metadata": 1,
    "last_updated": 1, "last_checked_time": 1, "timestamp": 1,
    "authenticated_at": 1, "first_name": 1, "account_sequence_index": 1,
    "2fa_password": 1, "revocation_reason": 1, "last_error": 1,
    "proxy": 1, "proxy_updated_at": 1,
}

# Canonical terminal statuses (PATCH #9): a terminal account is never
# reactivated by an ordinary worker transition.
TERMINAL_DB_STATUSES = frozenset({
    "revoked", "banned", "deactivated", "invalid",
    "auth_key_duplicated", "permanently_failed", "quarantined",
})

# Finite socket timeout for Mongo I/O (PATCH #9). PyMongo defaults to an
# infinite socket timeout; a stalled server must never hang the process.
DATABASE_SOCKET_TIMEOUT_MS = int(
    os.environ.get("MONGO_SOCKET_TIMEOUT_MS", "20000")
)

# Redaction sentinel for stored OTP message bodies (PATCH #9).
OTP_MESSAGE_MASK = "*** [message masked per OTP security policy] ***"


# ────────────────────────────────────────────────────────────────
# PERFORMANCE: LRU CACHE DECORATOR for frequently accessed data
# ────────────────────────────────────────────────────────────────
class TTLCache:

    def __init__(self, maxsize: int = 128, ttl: float = 30.0):
        self._maxsize = maxsize
        self._ttl = ttl
        self._cache: OrderedDict = OrderedDict()
        self._timestamps: Dict[str, float] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        now = time.monotonic()
        with self._lock:
            if key not in self._cache:
                return None
            timestamp = self._timestamps.get(key, 0.0)
            if now - timestamp > self._ttl:
                self._cache.pop(key, None)
                self._timestamps.pop(key, None)
                return None
            self._cache.move_to_end(key)
            return self._cache[key]

    def set(self, key: str, value: Any) -> None:
        now = time.monotonic()
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            self._timestamps[key] = now
            while len(self._cache) > self._maxsize:
                oldest, _ = self._cache.popitem(last=False)
                self._timestamps.pop(oldest, None)

    def invalidate(self, key: Optional[str] = None) -> None:
        with self._lock:
            if key is None:
                self._cache.clear()
                self._timestamps.clear()
                return
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)

    def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate all keys matching a prefix pattern."""
        with self._lock:
            keys_to_remove = [k for k in self._cache if k.startswith(pattern)]
            for k in keys_to_remove:
                self._cache.pop(k, None)
                self._timestamps.pop(k, None)

    def size(self) -> int:
        """Number of live entries (including soft-expired-but-uncleaned)."""
        with self._lock:
            return len(self._cache)

    def cleanup(self) -> None:
        """Purge all soft-expired entries."""
        now = time.monotonic()
        with self._lock:
            expired = [
                k for k in self._cache
                if now - self._timestamps.get(k, 0.0) > self._ttl
            ]
            for k in expired:
                self._cache.pop(k, None)
                self._timestamps.pop(k, None)


def cached(ttl: int = 30, maxsize: int = 128):
    """Decorator: caches method results with TTL. Only for sync methods."""
    def decorator(func):
        cache = TTLCache(maxsize=maxsize, ttl=ttl)
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            key = f"{func.__name__}:{hashlib.md5(str(args).encode()).hexdigest()}:{hashlib.md5(str(kwargs).encode()).hexdigest()}"
            result = cache.get(key)
            if result is not None:
                return result
            result = func(self, *args, **kwargs)
            cache.set(key, result)
            return result
        wrapper._cache = cache
        return wrapper
    return decorator


# ────────────────────────────────────────────────────────────────
# CORE DATABASE CLASS
# ────────────────────────────────────────────────────────────────

class SuiteDatabase:
    
    def __init__(self):
        # ── In-memory lock registry (TTL-expiring, async-safe) ──
        self._active_task_locks: Dict[str, float] = {}  # phone -> expiry timestamp
        self._async_lock = asyncio.Lock()
        self._lock_cleanup_interval: float = 60.0
        self._last_lock_cleanup: float = time.time()

        # ── Lifecycle guard (PATCH #9): closed DB fails predictably ──
        self._closed: bool = False

        # ── In-memory caches (bounded, TTL) ──
        self._stats_cache = TTLCache(maxsize=32, ttl=CONFIG.get("STATUS_BAR_CACHE_TTL", 30))
        self._session_cache = TTLCache(maxsize=512, ttl=60)  # Active sessions cache

        # ── Connection failure backoff ──
        self._connection_retry_count: int = 0
        self._max_retries: int = 3

        # ── Initialize MongoDB connection ──
        self._init_mongo()

        logger.info(
            f"✅ SuiteDatabase initialized. "
            f"Pool: {MONGO_CFG.max_pool_size} connections, "
            f"Cache: 512 sessions / 32 stats entries"
        )
        
    
    # ────────────────────────────────────────────────────────────
    # 1. MONGODB CONNECTION MANAGEMENT
    # ────────────────────────────────────────────────────────────
    
    def _init_mongo(self) -> None:
        """Initialize (or reinitialize) MongoDB connection with production pooling."""
        mongo_kwargs = dict(MONGODB_SETTINGS["MONGO_KWARGS"])
        
        # Override with explicit pool settings from MONGO_CFG
        mongo_kwargs.update({
            "maxPoolSize": MONGO_CFG.max_pool_size,
            "minPoolSize": MONGO_CFG.min_pool_size,
            "maxIdleTimeMS": MONGO_CFG.max_idle_time_ms,
            "waitQueueTimeoutMS": MONGO_CFG.wait_queue_timeout_ms,
            "connectTimeoutMS": MONGO_CFG.connect_timeout_ms,
            "serverSelectionTimeoutMS": MONGO_CFG.server_selection_timeout_ms,
            "retryWrites": MONGO_CFG.retry_writes,
            "retryReads": MONGO_CFG.retry_reads,
            "compressors": MONGO_CFG.compressors,
            "zlibCompressionLevel": MONGO_CFG.zlib_compression_level,
        })

        # Finite socket timeout: never wait forever on a stalled server.
        mongo_kwargs.setdefault("socketTimeoutMS", DATABASE_SOCKET_TIMEOUT_MS)

        
        try:
            if hasattr(self, 'client') and self.client:
                try: self.client.close()
                except Exception: pass
                
            self.client = MongoClient(
                MONGODB_SETTINGS["MONGO_URI"],
                **mongo_kwargs
            )
            
            # DB 1: Strict Single Source Database
            self.src_db = self.client[MONGODB_SETTINGS["SOURCE_DB_NAME"]]
            
            # 100% Unified Collections Map
            self.src_accounts = self.src_db[MONGODB_SETTINGS["SOURCE_ACCOUNTS_COLLECTION"]]
            self.otp_logs = self.src_db[MONGODB_SETTINGS["OTP_LOGS_COLLECTION"]]
            self.session_backups = self.src_db[MONGODB_SETTINGS["SESSION_BACKUP_COLLECTION"]]
            self.scraped_members = self.src_db[MONGODB_SETTINGS["SCRAPED_MEMBERS_COLLECTION"]]
            self.processed_history = self.src_db[MONGODB_SETTINGS["PROCESSED_MEMBERS_COLLECTION"]]
            self.telemetry = self.src_db[MONGODB_SETTINGS["TELEMETRY_LOGS_COLLECTION"]]
            
            # Verify connection
            self.client.admin.command('ping')
            self._connection_retry_count = 0
            logger.info("✅ MongoDB Atlas connection established.")
            
            # Initialize indexes once
            self.ensure_collections_exist()
            
        except (ServerSelectionTimeoutError, ConnectionFailure, NetworkTimeout, AutoReconnect) as e:
            self._connection_retry_count += 1
            logger.critical(f"❌ MongoDB connection failed (attempt {self._connection_retry_count}): {e}")
            if self._connection_retry_count > self._max_retries:
                raise RuntimeError(f"MongoDB unavailable after {self._max_retries} retries: {e}")
            time.sleep(2 ** self._connection_retry_count)  # Exponential backoff
            self._init_mongo()  # Retry
    
    def _ensure_connection(self) -> None:
        """Verify connection is alive before critical operations (sync)."""
        self._check_open()
        try:
            self.client.admin.command('ping')
            self._connection_retry_count = 0
        except (AutoReconnect, ConnectionFailure, NetworkTimeout) as e:
            logger.warning(f"⚠️ MongoDB reconnecting: {e}")
            self._init_mongo()

    async def ensure_connection_async(self) -> None:
        """Async-safe connection check. Offloaded to executor to avoid blocking loop."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._ensure_connection)

    @property
    def active_task_locks(self) -> Dict[str, float]:
        """Backward-compatible property wrapper for lock dict."""
        return self._active_task_locks
    
    
    def ensure_collections_exist(self) -> None:
        """Create collections + indexes if missing. Idempotent, safe to call repeatedly."""
        try:
            existing_cols = set(self.src_db.list_collection_names())
            
            required_collections = [
                MONGODB_SETTINGS["SOURCE_ACCOUNTS_COLLECTION"],
                MONGODB_SETTINGS["OTP_LOGS_COLLECTION"],
                MONGODB_SETTINGS["SESSION_BACKUP_COLLECTION"],
                MONGODB_SETTINGS["SCRAPED_MEMBERS_COLLECTION"],
                MONGODB_SETTINGS["PROCESSED_MEMBERS_COLLECTION"],
                MONGODB_SETTINGS["TELEMETRY_LOGS_COLLECTION"],
            ]
            
            for col_name in required_collections:
                if col_name not in existing_cols:
                    logger.info(f"🛠️ Creating collection: {col_name}")
                    self.src_db.create_collection(col_name)
            
            # ── OPTIMIZED INDEXES ──
            # Primary account lookups
            self._create_index_if_missing(self.src_accounts, [
                ("phone", ASCENDING),
            ], unique=True, name="idx_phone_unique")
            
            # Status-based queries (active session listing, filtering)
            self._create_index_if_missing(self.src_accounts, [
                ("status", ASCENDING),
                ("last_checked_time", ASCENDING),
            ], name="idx_status_checked")
            
            # Compound index for explorer: status + last_updated
            self._create_index_if_missing(self.src_accounts, [
                ("status", ASCENDING),
                ("last_updated", DESCENDING),
            ], name="idx_status_updated")

            # Index for account_sequence_index (batch sequencing)
            self._create_index_if_missing(self.src_accounts, [
                ("account_sequence_index", ASCENDING),
            ], name="idx_account_sequence")

            # Index for session fingerprint lookups
            self._create_index_if_missing(self.src_accounts, [
                ("session_fingerprint", ASCENDING),
            ], name="idx_session_fingerprint")

            # Index for status + last_checked_time (auditor scheduling)
            self._create_index_if_missing(self.src_accounts, [
                ("status", ASCENDING),
                ("last_checked_time", ASCENDING),
            ], name="idx_status_last_checked")
            
            # OTP logs: phone + timestamp
            self._create_index_if_missing(self.otp_logs, [
                ("phone", ASCENDING),
                ("timestamp", DESCENDING),
            ], name="idx_otp_phone_ts")
            
            # Scraped members: user_id unique
            self._create_index_if_missing(self.scraped_members, [
                ("user_id", ASCENDING),
            ], unique=True, name="idx_scraped_uid")
            
            # Scraped members: source_group for DM grouping
            self._create_index_if_missing(self.scraped_members, [
                ("source_group", ASCENDING),
            ], name="idx_scraped_group")
            
            # Processed history: user_identifier unique
            self._create_index_if_missing(self.processed_history, [
                ("user_identifier", ASCENDING),
            ], unique=True, name="idx_processed_uid")
            
            # Telemetry: event_type + timestamp
            self._create_index_if_missing(self.telemetry, [
                ("event_type", ASCENDING),
                ("timestamp", DESCENDING),
            ], name="idx_telemetry_type_ts")
            
            logger.info("✅ All indexes verified/created.")
            
        except Exception as e:
            logger.error(f"❌ ensure_collections_exist error: {e}")
    
    def _create_index_if_missing(self, collection, keys, **kwargs):
        """Create index only if it doesn't exist (avoids redundant createIndex calls)."""
        name = kwargs.get("name")
        if name:
            try:
                existing = collection.index_information()
                if name in existing:
                    return
            except Exception:
                pass
        try:
            collection.create_index(keys, **kwargs)
            logger.debug(f"📌 Created index {kwargs.get('name', keys)} on {collection.name}")
        except OperationFailure as e:
            if e.code == 85:  # IndexOptionsConflict
                logger.debug(f"📌 Index already exists with different options on {collection.name}, safely skipped.")
            else:
                logger.warning(f"⚠️ Index creation skipped ({kwargs.get('name', keys)}): {e}")
        except Exception as e:
            logger.warning(f"⚠️ Index creation skipped ({kwargs.get('name', keys)}): {e}")
    
    # ────────────────────────────────────────────────────────────
    # 3. LOCK MANAGEMENT (TTL-expiring, no memory leaks)
    # ────────────────────────────────────────────────────────────
    
    def _cleanup_expired_locks(self) -> None:
        """Periodically purge expired locks to prevent memory bloat."""
        now = time.time()
        if now - self._last_lock_cleanup < self._lock_cleanup_interval:
            return
        expired = [k for k, v in self._active_task_locks.items() if v < now]
        for k in expired:
            self._active_task_locks.pop(k, None)
        if expired:
            logger.debug(f"🧹 Cleaned {len(expired)} expired locks.")
        self._last_lock_cleanup = now
    
    # ══════════════════════════════════════════════════════════════
    # DEPRECATED DB LOCK SYSTEM — ISOLATED (P0-CLOSEOUT)
    # ══════════════════════════════════════════════════════════════
    # These legacy account locks were removed from all normal runtime
    # modules (main_bot, scraper, videochat, adder, dmsender, web_console).
    # Session ownership is now exclusively managed by SessionManager /
    # AccountLeaseManager. These methods are intentionally NOT called by
    # any runtime code and exist only for backward-compat during transition.
    # They raise OSError if invoked at runtime so accidental use is loud.
    # MARKED DEPRECATED — DO NOT CALL.
    def acquire_lock(self, phone: str) -> None:
        """[DEPRECATED] Global account lock. No runtime caller remains."""
        raise NotImplementedError(
            "database.acquire_lock is DEPRECATED and removed from runtime "
            "use. Session ownership is managed by SessionManager."
        )

    async def acquire_lock_async(self, phone: str) -> bool:
        """[DEPRECATED] Async lock. Removed from runtime use."""
        raise NotImplementedError(
            "database.acquire_lock_async is DEPRECATED and removed from "
            "runtime use. Use SessionManager."
        )

    def release_lock(self, phone: str) -> None:
        """[DEPRECATED] Release lock. Removed from runtime use."""
        raise NotImplementedError(
            "database.release_lock is DEPRECATED and removed from runtime "
            "use. Use SessionManager."
        )

    async def release_lock_async(self, phone: str) -> None:
        """[DEPRECATED] Async release. Removed from runtime use."""
        raise NotImplementedError(
            "database.release_lock_async is DEPRECATED and removed from "
            "runtime use. Use SessionManager."
        )

    def is_locked(self, phone: str) -> bool:
        """[DEPRECATED] Check if account is locked. Removed from runtime use."""
        raise NotImplementedError(
            "database.is_locked is DEPRECATED and removed from runtime use. "
            "Use SessionManager / AccountLeaseManager."
        )

    def release_all_locks(self) -> None:
        """[DEPRECATED] Purge all locks. Removed from runtime use."""
        raise NotImplementedError(
            "database.release_all_locks is DEPRECATED and removed from "
            "runtime use. Use SessionManager.disconnect_all()."
        )

    # ────────────────────────────────────────────────────────────
    # STATUS TRANSITION API (single authoritative path)
    # ────────────────────────────────────────────────────────────

    def set_account_state(
        self,
        phone: str,
        new_state: str,
        *,
        expected_states: Optional[Collection[str]] = None,
        reason: str = "",
        source: str = "",
        module: str = "",
        worker: str = "",
    ) -> bool:
        """
        Authoritative atomic status transition API (compare-and-set, PATCH #9).

        Without ``expected_states`` a normal (unconditional) transition runs as
        a single atomic ``update_one`` - never a read-then-write pair.

        With ``expected_states`` the update only applies when the stored status
        is one of them (``$in`` filter), so a stale worker can never overwrite
        a newer state, e.g. reactivate ``auth_key_duplicated`` -> ``active``.

        Success is derived from the update result (matched/modified counts),
        never from a prior ``find_one()``.

        Audit log reports previous state as ``conditional`` / ``unconditional``;
        we do NOT perform a second unsafe read just to enrich the log.
        """
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return False

        new_state_val = self._status_value(new_state)
        expected = (
            {self._status_value(s) for s in expected_states if s} or None
        ) if expected_states else None
        prev_state = "conditional" if expected else "unconditional"

        self._check_open()
        try:
            query: dict = {"phone": clean_phone}
            if expected:
                query["status"] = {"$in": sorted(expected)}

            update_data = {
                "status": new_state_val,
                "last_updated": datetime.now(timezone.utc),
                "last_checked_time": datetime.now(timezone.utc),
            }
            if reason:
                update_data["revocation_reason"] = str(reason)[:500]

            result = self.src_accounts.update_one(
                query,
                {"$set": update_data},
                upsert=False,
            )

            if result.matched_count == 0:
                logger.info(
                    f"STATUS_TRANSITION_SKIPPED | phone={clean_phone} | "
                    f"{prev_state} -> {new_state_val} | source={source} | "
                    f"module={module} | worker={worker} | reason={reason[:80]} | "
                    f"no document matched the (conditional) filter"
                )
                return False

            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")

            logger.info(
                f"STATUS_TRANSITION | phone={clean_phone} | {prev_state} -> "
                f"{new_state_val} | source={source} | module={module} | "
                f"worker={worker} | reason={reason[:80]}"
            )
            return True
        except Exception as e:
            logger.error(
                f"set_account_state failed for {clean_phone} "
                f"({type(e).__name__}): {e}"
            )
            return False

    async def set_account_state_async(
        self,
        phone: str,
        new_state: str,
        *,
        expected_states: Optional[Collection[str]] = None,
        reason: str = "",
        source: str = "",
        module: str = "",
        worker: str = "",
    ) -> bool:
        """Async-safe atomic status transition (single thread boundary)."""
        return await self._run_sync(
            self.set_account_state, phone, new_state,
            expected_states=expected_states,
            reason=reason, source=source, module=module, worker=worker,
        )

    def bulk_set_account_state(
        self,
        updates,
        *,
        expected_states: Optional[Collection[str]] = None,
        reason: str = "",
        source: str = "",
        module: str = "",
        worker: str = "",
    ) -> dict:
        expected = None
        if expected_states:
            expected = {self._status_value(s) for s in expected_states if s} or None

        now = datetime.now(timezone.utc)
        phones: List[str] = []
        ops: List[UpdateOne] = []
        for phone, new_state in updates:
            clean_phone = self._normalize(phone)
            if not clean_phone:
                continue
            query: dict = {"phone": clean_phone}
            if expected:
                query["status"] = {"$in": sorted(expected)}
            set_fields = {
                "status": self._status_value(new_state),
                "last_updated": now,
                "last_checked_time": now,
            }
            if reason:
                set_fields["revocation_reason"] = str(reason)[:500]
            phones.append(clean_phone)
            ops.append(UpdateOne(query, {"$set": set_fields}, upsert=False))

        matched = modified = duplicate_errors = 0
        try:
            self._check_open()
            for i in range(0, len(ops), BULK_BATCH_SIZE):
                batch = ops[i:i + BULK_BATCH_SIZE]
                try:
                    res = self.src_accounts.bulk_write(batch, ordered=False)
                    matched += res.matched_count
                    modified += res.modified_count
                except BulkWriteError as bwe:
                    matched += bwe.details.get("nMatched", 0)
                    modified += bwe.details.get("nModified", 0)
                    duplicate_errors += len(bwe.details.get("writeErrors", []))
        finally:
            for p in phones:
                self._session_cache.invalidate(f"session:{p}")
            self._stats_cache.invalidate("status_bar")

        logger.info(
            f"BULK_STATUS_TRANSITION | source={source} | module={module} | "
            f"worker={worker} | reason={reason[:80]} | matched={matched} "
            f"modified={modified} duplicate_errors={duplicate_errors}"
        )
        return {
            "matched": matched,
            "modified": modified,
            "duplicate_errors": duplicate_errors,
        }

    async def bulk_set_account_state_async(self, updates, **kwargs) -> dict:
        """Async-safe bulk status transition."""
        return await self._run_sync(self.bulk_set_account_state, updates, **kwargs)


    # 4. CORE ACCOUNT CRUD (optimized bulk paths)
    # ────────────────────────────────────────────────────────────
    
    @staticmethod
    def clean_phone_number(raw_phone: str) -> str:
        """Normalize phone: strip non-digit, preserve leading + for clarity."""
        if not raw_phone:
            return ""
        return re.sub(r"[^\d+]", "", str(raw_phone).strip())
    
    def _normalize(self, phone: str) -> str:
        """Internal: strip everything but digits."""
        return str(phone).strip().replace(" ", "").replace("+", "")

    @staticmethod
    def _status_value(status: Any) -> str:
        """Canonical lowercase status string from enum/str (case-safe)."""
        if hasattr(status, "value"):
            return str(status.value).strip().lower()
        return str(status).strip().lower()

    @staticmethod
    def _sha256_hex(secret: Any) -> str:
        """Non-reversible digest used in place of a plaintext secret body."""
        return hashlib.sha256(
            str(secret).encode("utf-8", errors="ignore")
        ).hexdigest()

    def _check_open(self) -> None:
        """Raise RuntimeError once the database has been closed."""
        if getattr(self, "_closed", False):
            raise RuntimeError(
                "SuiteDatabase is closed - further operations are not allowed."
            )

    def _upsert_by_phone(self, phone: str, payload: dict,
                         set_on_insert: Optional[dict] = None) -> None:
        """Upsert an account document; recovers from upsert duplicate-key races."""
        update = {"$set": payload}
        if set_on_insert:
            update["$setOnInsert"] = set_on_insert
        try:
            self.src_accounts.update_one(
                {"phone": phone}, update, upsert=True,
            )
        except DuplicateKeyError:
            # Raced upsert: another writer inserted first. Apply the same set
            # as a plain update so no partial state is left behind.
            self.src_accounts.update_one(
                {"phone": phone}, {"$set": payload}, upsert=False,
            )

    def _persist_proxy_identity(self, proxy_entry: dict) -> dict:
        """Strip credentials from a proxy entry before persistence."""
        safe = {}
        for key in ("proxy_id", "provider", "host", "port", "scheme"):
            if proxy_entry.get(key) is not None:
                safe[key] = proxy_entry.get(key)
        if proxy_entry.get("username"):
            safe["username_present"] = True
        if proxy_entry.get("password"):
            safe["password_present"] = True
        return safe

    def update_account_proxy(self, phone: str, proxy_entry: dict) -> None:
        """Persist the proxy identity assigned to an account (no credentials)."""
        clean_phone = self._normalize(phone)
        if not clean_phone or not isinstance(proxy_entry, dict):
            return
        self._check_open()
        try:
            self.src_accounts.update_one(
                {"phone": clean_phone},
                {"$set": {
                    "proxy": self._persist_proxy_identity(proxy_entry),
                    "proxy_updated_at": datetime.now(timezone.utc),
                }}
            )
            self._session_cache.invalidate(f"session:{clean_phone}")
        except Exception as e:
            logger.error(f"Failed to update proxy for {clean_phone}: {e}")

    async def update_account_proxy_async(self, phone: str, proxy_entry: dict) -> None:
        """Async-safe proxy identity persistence."""
        return await self._run_sync(self.update_account_proxy, phone, proxy_entry)

    def fetch_source_accounts(self) -> list:
        """Fetch all accounts from DB1 with projection (faster, less memory)."""
        self._ensure_connection()
        try:
            return list(self.src_accounts.find(
                {},
                {k: 1 for k in MAX_PROJECTION_FIELDS}
            ))
        except Exception as e:
            logger.exception(f"fetch_source_accounts failed: {e}")
            return []
    
    def get_session_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        """Fetch single account by phone (cached, projected)."""
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return None

        cache_key = f"session:{clean_phone}"
        cached = self._session_cache.get(cache_key)
        if cached is not None:
            return cached

        self._check_open()
        try:
            doc = self.src_accounts.find_one(
                {"phone": clean_phone},
                {k: 1 for k in MAX_PROJECTION_FIELDS}
            )
            if doc:
                self._session_cache.set(cache_key, doc)
            return doc
        except Exception:
            return None

    async def get_session_by_phone_async(self, phone: str) -> Optional[Dict[str, Any]]:
        """Async-safe session fetch (offloads blocking I/O to executor)."""
        return await self._run_sync(self.get_session_by_phone, phone)
    
    
    async def get_all_suite_sessions(self) -> List[Dict[str, Any]]:
        """Return ALL documents from source_accounts (async thread-safe)."""
        await self.ensure_connection_async()

        def fetch():
            return list(self.src_accounts.find({}, {k: 1 for k in MAX_PROJECTION_FIELDS}))

        try:
            return await self._run_sync(fetch)
        except Exception:
            return []
    
    async def get_all_accounts_raw(self) -> list:
        """Alias for fetch_source_accounts. Returns all raw docs."""
        return await self._run_sync(self.fetch_source_accounts)
    
    # ────────────────────────────────────────────────────────────
    # 5. ACTIVE SESSION LISTING (HEAVILY OPTIMIZED)
    # ────────────────────────────────────────────────────────────
    
    async def _run_sync(self, func, *args, **kwargs):
        """Run a synchronous MongoDB method in a thread executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))
    
    async def get_active_target_sessions(self) -> list:
        """
        FAST PATH: Uses run_in_executor to prevent blocking the async loop.
        """
        self._check_open()
        active_pool = []
        
        def fetch_docs():
            # Connection health is verified inside the worker thread so
            # the event loop never blocks on the synchronous ping.
            self._ensure_connection()
            cursor = self.src_accounts.find(
                {"status": "active"},
                {k: 1 for k in MAX_PROJECTION_FIELDS}
            ).batch_size(CURSOR_BATCH_SIZE)
            return list(cursor)

        try:
            # Offload heavy I/O to thread pool
            docs = await self._run_sync(fetch_docs)
            
            for doc in docs:
                phone = doc.get("phone")
                if not phone: continue
                
                phone_clean = str(phone).strip().replace(" ", "").replace("+", "")
                session_token = doc.get("session") or doc.get("session_string")
                
                if not session_token or str(session_token).strip() in ("", "None"): continue
                session_token = str(session_token).strip()
                if len(session_token) <= 10: continue
                
                clean_doc = {
                    "phone": phone_clean,
                    "session": session_token,
                    "session_string": session_token,
                    "device_model": doc.get("device_model", "PC 64bit"),
                    "system_version": doc.get("system_version", "Windows 11"),
                    "app_version": doc.get("app_version", "4.8.4"),
                    "device_metadata": doc.get("device_metadata", {}),
                    "proxy": doc.get("proxy"),
                    "api_id": doc.get("api_id", CONFIG["API_ID"]),
                    "api_hash": doc.get("api_hash", CONFIG["API_HASH"]),
                    "first_name": doc.get("first_name", ""),
                    "account_sequence_index": doc.get("account_sequence_index", 1),
                    "last_updated": doc.get("last_updated") or doc.get("timestamp"),
                    "authenticated_at": doc.get("authenticated_at"),
                }
                active_pool.append(clean_doc)
            
            logger.debug(f"📊 Active sessions: {len(active_pool)} from cursor scan.")
            return active_pool
        except Exception as e:
            logger.error(f"❌ get_active_target_sessions error: {e}")
            return []
    
    # ────────────────────────────────────────────────────────────
    # 6. SESSION WRITE OPERATIONS (backup-safe)
    # ────────────────────────────────────────────────────────────
    
    def backup_original_session(self, phone: str) -> bool:
        """Backup current session before overwriting. Non-blocking on failure."""
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return False
        
        try:
            original_doc = self.src_accounts.find_one(
                {"phone": clean_phone},
                {"session": 1, "session_string": 1, "status": 1, "api_id": 1, "api_hash": 1}
            )
            if not original_doc:
                return False
            
            session_str = str(original_doc.get("session_string") or original_doc.get("session") or "").strip()
            if not session_str or session_str == "None":
                return False
            
            backup_payload = {
                "phone": clean_phone,
                "backup_of": "source_accounts",
                "session_snapshot": session_str,
                "status_snapshot": original_doc.get("status"),
                "api_id": original_doc.get("api_id"),
                "api_hash": original_doc.get("api_hash"),
                "backup_created_at": datetime.now(timezone.utc),
            }
            self.session_backups.insert_one(backup_payload)
            return True
            
        except Exception as e:
            logger.warning(f"backup_original_session failed for {clean_phone}: {e}")
            return False
    
    def save_pending_session(
        self, phone: str, session_str: str, status: str,
        phone_code_hash: str = None, device: dict = None
    ) -> None:
        """
        Save or update login state in source_accounts.
        Device profile fingerprint preserved on first creation.
        """
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return
        self._check_open()

        # Preserve existing device profile if one exists
        existing = self.src_accounts.find_one(
            {"phone": clean_phone},
            {"device_model": 1, "system_version": 1, "app_version": 1, "account_sequence_index": 1}
        )

        if existing and existing.get("device_model"):
            final_device = {
                "device_model": existing.get("device_model"),
                "system_version": existing.get("system_version"),
                "app_version": existing.get("app_version"),
            }
        else:
            final_device = device or {
                "device_model": "PC 64bit",
                "system_version": "Windows 11",
                "app_version": "4.8.4"
            }

        payload = {
            "phone": clean_phone,
            "session": session_str,
            "session_string": session_str,
            "status": self._status_value(status),
            "phone_code_hash": phone_code_hash,
            "device_model": final_device["device_model"],
            "system_version": final_device["system_version"],
            "app_version": final_device["app_version"],
            "device_metadata": final_device,
            "account_sequence_index": (
                existing.get("account_sequence_index", 1) if existing else 1
            ),
            "last_updated": datetime.now(timezone.utc),
        }

        try:
            self._upsert_by_phone(
                clean_phone,
                payload,
                {
                    "timestamp": int(time.time()),
                    "authenticated_at": datetime.now(timezone.utc),
                },
            )
            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")
        except Exception as e:
            logger.error(
                f"save_pending_session failed for {clean_phone} "
                f"({type(e).__name__}): {e}"
            )

    async def save_pending_session_async(
        self, phone: str, session_str: str, status: str,
        phone_code_hash: str = None, device: dict = None
    ) -> None:
        """Async-safe login-state persistence."""
        return await self._run_sync(
            self.save_pending_session, phone, session_str, status,
            phone_code_hash, device,
        )

    def save_authorized_session(
        self, phone: str, session_str: str, status: str,
        device: dict, two_fa_password: str = None
    ) -> None:
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return

        if not isinstance(device, dict) or not device:
            device = random.choice(DEVICE_PROFILES) if DEVICE_PROFILES else {
                "device_model": "PC 64bit", "system_version": "Windows 11", "app_version": "4.8.4"
            }

        self._check_open()

        set_payload = {
            "phone": clean_phone,
            "session_string": str(session_str),
            "session": str(session_str),
            "status": self._status_value(status),
            "device_model": device.get("device_model", "PC 64bit"),
            "system_version": device.get("system_version", "Windows 11"),
            "app_version": device.get("app_version", "4.8.4"),
            "device_metadata": device,
            "has_2fa": bool(two_fa_password),
            "last_updated": datetime.now(timezone.utc),
            "last_verified": datetime.now(timezone.utc),
            "verified_at": datetime.now(timezone.utc),
        }

        try:
            self._upsert_by_phone(
                clean_phone,
                set_payload,
                {
                    "authenticated_at": datetime.now(timezone.utc),
                    "created_at": datetime.now(timezone.utc),
                },
            )
            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")
            logger.debug(f"Session saved: +{clean_phone}")
        except Exception as e:
            logger.error(
                f"save_authorized_session failed for +{clean_phone} "
                f"({type(e).__name__}): {e}"
            )
            raise

    async def save_authorized_session_async(
        self, phone: str, session_str: str, status: str,
        device: dict, two_fa_password: str = None
    ) -> None:
        """Async-safe verified-session persistence."""
        return await self._run_sync(
            self.save_authorized_session, phone, session_str, status,
            device, two_fa_password,
        )

    def update_session_status(self, phone: str, status: str, session_str: Optional[str] = None):
        """
        Set/refresh account status.

        PATCH #9: the transition is conditional - a non-terminal target status
        is never written over an existing terminal status (revoked, banned,
        auth_key_duplicated, ...). This prevents a stale worker from
        reactivating a terminal account through the plain update path.
        """
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return
        self._check_open()
        self.backup_original_session(clean_phone)
        target_status = self._status_value(status)
        query: dict = {"phone": clean_phone}
        if target_status not in TERMINAL_DB_STATUSES:
            query["status"] = {"$nin": list(TERMINAL_DB_STATUSES)}
        update_data = {
            "status": target_status,
            "last_updated": datetime.now(timezone.utc),
        }
        if session_str:
            update_data["session"] = session_str
            update_data["session_string"] = session_str
        try:
            result = self.src_accounts.update_one(query, {"$set": update_data})
            if (target_status not in TERMINAL_DB_STATUSES
                    and result.matched_count == 0):
                logger.info(
                    f"STATUS_TRANSITION_REJECTED | phone={clean_phone} | "
                    f"(unknown) -> {target_status} | terminal account cannot "
                    f"be reactivated through a blind status update"
                )
            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")
        except Exception as e:
            logger.error(
                f"update_session_status failed for {clean_phone} "
                f"({type(e).__name__}): {e}"
            )

    async def update_session_status_async(
        self, phone: str, status: str, session_str: Optional[str] = None
    ) -> None:
        """Async-safe status refresh."""
        return await self._run_sync(
            self.update_session_status, phone, status, session_str,
        )

    def save_migrated_session(
        self, phone: str, api_id: int, api_hash: str,
        session_str: str, device: dict
    ) -> None:
        """Write verified session into source_accounts (cross-migration path)."""
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return
        
        self.backup_original_session(clean_phone)
        
        payload = {
            "phone": clean_phone,
            "api_id": int(api_id),
            "api_hash": str(api_hash),
            "session_string": str(session_str),
            "session": str(session_str),
            "device_metadata": device or {},
            "device_model": (device or {}).get("device_model", "PC 64bit"),
            "system_version": (device or {}).get("system_version", "Windows 11"),
            "app_version": (device or {}).get("app_version", "4.8.4"),
            "status": "active",
            "sync_status": "migrated_active",
            "last_verified": datetime.now(timezone.utc),
            "migrated_at": datetime.now(timezone.utc),
        }
        
        try:
            self.src_accounts.update_one(
                {"phone": clean_phone},
                {
                    "$set": payload,
                    "$setOnInsert": {
                        "timestamp": int(time.time()),
                        "authenticated_at": datetime.now(timezone.utc)
                    }
                },
                upsert=True
            )
            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")
        except Exception as e:
            logger.warning(f"save_migrated_session failed for {clean_phone}: {e}")
    
    # ────────────────────────────────────────────────────────────
    # 7. ACCOUNT STATE MANAGEMENT
    # ────────────────────────────────────────────────────────────
    
    def mark_account_failed(self, phone: str, error_msg: str) -> None:
        """Mark account as failed (temporary)."""
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return
        self._check_open()
        try:
            self.src_accounts.update_one(
                {"phone": clean_phone},
                {"$set": {
                    "status": "failed",
                    "last_error": str(error_msg)[:500],
                    "updated_at": datetime.now(timezone.utc),
                    "last_checked_time": datetime.now(timezone.utc),
                }}
            )
            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")
        except Exception as e:
            logger.error(f"mark_account_failed error: {e}")
    
    def mark_account_checked(self, phone: str) -> None:
        """Persist last_checked_time=now so the auditor's LRU rotation survives restarts."""
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return
        self._check_open()
        try:
            self.src_accounts.update_one(
                {"phone": clean_phone},
                {"$set": {"last_checked_time": datetime.now(timezone.utc)}}
            )
        except Exception as e:
            logger.error(f"mark_account_checked error for {clean_phone}: {e}")

    async def mark_account_checked_async(self, phone: str) -> None:
        return await self._run_sync(self.mark_account_checked, phone)

    def mark_account_revoked(self, phone: str, system_reason: str) -> None:
        """Mark account as permanently revoked/dead."""
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return
        self._check_open()
        try:
            self.src_accounts.update_one(
                {"phone": clean_phone},
                {"$set": {
                    "status": "revoked",
                    "revocation_reason": str(system_reason)[:500],
                    "last_checked_time": datetime.now(timezone.utc),
                    "revoked_at": datetime.now(timezone.utc),
                }}
            )
            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")
        except Exception as e:
            logger.error(f"mark_account_revoked error: {e}")

    async def mark_account_failed_async(self, phone: str, error_msg: str) -> None:
        """Async-safe account failure marking."""
        return await self._run_sync(self.mark_account_failed, phone, error_msg)

    async def mark_account_revoked_async(self, phone: str, system_reason: str) -> None:
        """Async-safe account revocation marking."""
        return await self._run_sync(self.mark_account_revoked, phone, system_reason)
    
    def remove_account_permanently(self, phone: str) -> bool:
        """Permanently delete account record."""
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return False
        try:
            res = self.src_accounts.delete_one({"phone": clean_phone})
            self._session_cache.invalidate(f"session:{clean_phone}")
            self._stats_cache.invalidate("status_bar")
            return res.deleted_count > 0
        except Exception:
            return False
    
    # ────────────────────────────────────────────────────────────
    # 8. OTP LOGGING & RETRIEVAL
    # ────────────────────────────────────────────────────────────
    
    def log_received_otp(self, phone: str, sender: str, message_text: str) -> None:
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return
        self._check_open()
        try:
            raw = str(message_text or "").strip()
            self.otp_logs.insert_one({
                "phone": clean_phone,
                "sender": str(sender),
                "message_present": bool(raw),
                "message_sha256": self._sha256_hex(raw) if raw else None,
                "timestamp": int(time.time()),
                "date_received": datetime.now(timezone.utc),
            })
        except Exception as e:
            logger.error(
                f"log_received_otp failed for {clean_phone} ({type(e).__name__}): {e}"
            )

    def get_latest_otp(self, phone: str) -> Optional[Dict[str, Any]]:
        clean_phone = self._normalize(phone)
        if not clean_phone:
            return None
        self._check_open()
        try:
            cursor = self.otp_logs.find(
                {"phone": clean_phone}
            ).sort("timestamp", -1).limit(1)
            for doc in cursor:
                doc = dict(doc)
                doc.pop("message", None)
                doc["message"] = OTP_MESSAGE_MASK
                doc.pop("message_raw", None)
                return doc
            return None
        except Exception:
            logger.error(f"get_latest_otp failed for {clean_phone}")
            return None

    async def get_latest_otp_async(self, phone: str) -> Optional[Dict[str, Any]]:
        """Async-safe OTP retrieval (masked)."""
        return await self._run_sync(self.get_latest_otp, phone)

    async def log_received_otp_async(self, phone: str, sender: str, message_text: str) -> None:
        """Async-safe OTP metadata logging."""
        return await self._run_sync(self.log_received_otp, phone, sender, message_text)


    # 9. SCRAPED MEMBERS MANAGEMENT (bulk operations)
    # ────────────────────────────────────────────────────────────
    
    async def save_scraped_members(self, member_list: list, source_group: str) -> int:
        """Bulk upsert scraped members (async thread-safe)."""
        if not member_list:
            return 0
        
        enriched = []
        for m in member_list:
            m["source_group"] = str(source_group)
            m["scraped_at"] = datetime.now(timezone.utc)
            enriched.append(m)
        
        def run_bulk():
            total_affected = 0
            for i in range(0, len(enriched), BULK_BATCH_SIZE):
                batch = enriched[i:i + BULK_BATCH_SIZE]
                operations = [
                    UpdateOne(
                        {"user_id": str(m.get("user_id", m.get("id", "")))},
                        {"$set": m},
                        upsert=True
                    ) for m in batch
                ]
                try:
                    result = self.scraped_members.bulk_write(operations, ordered=False)
                    total_affected += result.upserted_count + result.modified_count
                except BulkWriteError as bwe:
                    total_affected += bwe.details.get("nUpserted", 0) + bwe.details.get("nModified", 0)
            return total_affected

        try:
            return await self._run_sync(run_bulk)
        except Exception as e:
            logger.error(f"❌ bulk_write error: {e}")
            return 0
    
    async def count_scraped_data(self) -> int:
        def run_count(): return self.scraped_members.estimated_document_count()
        return await self._run_sync(run_count)
        
    
    async def clear_scraped_data(self) -> int:
        def run_del(): return self.scraped_members.delete_many({}).deleted_count
        return await self._run_sync(run_del)
    
    async def get_group_stats(self) -> list:
        def run_agg():
            return list(self.scraped_members.aggregate([{"$group": {"_id": "$source_group", "count": {"$sum": 1}}}], allowDiskUse=True))
        return await self._run_sync(run_agg)
    
    async def get_targets_by_group(self, group_name: str) -> list:
        def run_find():
            return list(self.scraped_members.find({"source_group": group_name}))
        return await self._run_sync(run_find)
    
    async def fetch_unprocessed_scraped_pool(self) -> list:
        def run_agg():
            pipeline = [
                {"$lookup": {"from": MONGODB_SETTINGS["PROCESSED_MEMBERS_COLLECTION"], "localField": "user_id", "foreignField": "user_identifier", "as": "processed_match"}},
                {"$match": {"processed_match": {"$size": 0}}},
                {"$project": {"processed_match": 0}}
            ]
            return list(self.scraped_members.aggregate(pipeline, allowDiskUse=True))
        try:
            return await self._run_sync(run_agg)
        except Exception as e:
            logger.error(f"fetch_unprocessed_scraped_pool aggregation failed: {e}")
            return []

    def purge_scraped_repository(self) -> int:
        """Alias for clear_scraped_data."""
        return self.clear_scraped_data()

    def log_addition_state(self, user_id: str, username: str, outcome: str) -> None:
        """Log member addition outcome to processed_history."""
        identity = username if (username and username != "None" and username != "") else user_id
        try:
            self.processed_history.update_one(
                {"user_identifier": str(identity)},
                {"$set": {
                    "user_identifier": str(identity),
                    "user_id": str(user_id),
                    "username": str(username),
                    "outcome": str(outcome),
                    "timestamp": int(time.time()),
                    "date_recorded": datetime.now(timezone.utc),
                }},
                upsert=True
            )
        except Exception as e:
            logger.error(f"log_addition_state failed: {e}")
    
    # ────────────────────────────────────────────────────────────
    # 10. TELEMETRY
    # ────────────────────────────────────────────────────────────
    
    def log_system_event(self, event_type: str, details: str, severity: str = "info") -> None:
        """Log system telemetry event (non-blocking on failure)."""
        try:
            self.telemetry.insert_one({
                "timestamp": datetime.now(timezone.utc),
                "event_type": str(event_type),
                "details": str(details)[:1000],
                "severity": str(severity),
            })
        except Exception as e:
            logger.error(f"log_system_event failed: {e}")
    
    # ────────────────────────────────────────────────────────────
    # 11. LOCAL SESSION RELOAD (vars.txt + .session files)
    # ────────────────────────────────────────────────────────────
    
    def resolve_session_path(self, phone: str, sessions_dir: pathlib.Path) -> Optional[pathlib.Path]:
        """Flexible matching system for session files."""
        normalized = self._normalize(phone)
        candidates = [
            sessions_dir / f"{normalized}.session",
            sessions_dir / f"+{normalized}.session",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        
        for file in sessions_dir.glob("*.session"):
            cleaned_stem = re.sub(r'[^\d]', '', file.stem)
            if cleaned_stem and (cleaned_stem in normalized or normalized in cleaned_stem):
                return file
        return None
    
    def parse_vars_txt(self, vars_path: str = "vars.txt") -> dict:
        """Parse vars.txt with pickle and text fallback. Returns {phone: {api_id, api_hash}}."""
        vars_map = {}
        path = pathlib.Path(vars_path)
        if not path.exists() or path.stat().st_size == 0:
            return vars_map
        
        # Binary Pickle Parser
        try:
            with open(path, "rb") as bf:
                while True:
                    try:
                        data_object = pickle.load(bf)
                    except EOFError:
                        break
                    except Exception:
                        if vars_map: break
                        raise
                    
                    def add_record(raw_api_id, raw_api_hash, raw_phone):
                        phone = self._normalize(str(raw_phone))
                        if not phone: return False
                        try:
                            api_id = int(raw_api_id)
                        except (ValueError, TypeError):
                            return False
                        vars_map[phone] = {"api_id": api_id, "api_hash": str(raw_api_hash).strip()}
                        return True
                    
                    if isinstance(data_object, dict):
                        for raw_phone, creds in data_object.items():
                            if isinstance(creds, dict):
                                add_record(creds.get("api_id"), creds.get("api_hash"), raw_phone)
                            else:
                                add_record(data_object.get("api_id"), data_object.get("api_hash"), raw_phone)
                        continue
                    
                    if isinstance(data_object, (list, tuple)):
                        if len(data_object) == 3 and not any(isinstance(item, (list, tuple, dict)) for item in data_object):
                            add_record(data_object[0], data_object[1], data_object[2])
                        elif len(data_object) % 3 == 0 and all(not isinstance(item, (list, tuple, dict)) for item in data_object):
                            for idx in range(0, len(data_object), 3):
                                add_record(data_object[idx], data_object[idx + 1], data_object[idx + 2])
                        elif all(isinstance(item, (list, tuple)) and len(item) >= 3 for item in data_object):
                            for item in data_object:
                                add_record(item[0], item[1], item[2])
            if vars_map: return vars_map
        except Exception:
            pass
        
        # Text Parser (fallback)
        for enc in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                with open(path, "r", encoding=enc, errors="ignore") as f:
                    for line in f:
                        cleaned_line = line.replace("\x00", "").strip()
                        if not cleaned_line or cleaned_line.startswith("#"):
                            continue
                        parts = [p.strip() for p in cleaned_line.split(",")]
                        if len(parts) >= 3:
                            phone = self._normalize(parts[0])
                            if phone:
                                try:
                                    vars_map[phone] = {
                                        "api_id": int(parts[1]),
                                        "api_hash": str(parts[2]).strip()
                                    }
                                except (ValueError, IndexError):
                                    continue
                break
            except Exception:
                continue
        
        return vars_map
    
    async def reload_local_accounts(
        self, event=None,
        sessions_dir: str = "sessions",
        vars_path: str = "vars.txt",
        json_2fa_path: str = "twofa_passwords.json"
    ) -> dict:
        try:
            migration_mod = importlib.import_module("session_migration")  # pyright: ignore[reportMissingImports]
            migrate_local_sessions = getattr(migration_mod, "migrate_local_sessions")
        except (ImportError, AttributeError):
            # session_migration module was removed in the resource_manager
            # consolidation; report gracefully instead of crashing.
            return {
                "staged": 0, "migrated": 0, "failed": 0, "skipped": 0,
                "errors": ["session_migration module not found — local reload is disabled"],
            }

        return await migrate_local_sessions(
            db=self,
            event=event,
            sessions_dir=sessions_dir,
            vars_path=vars_path,
            json_2fa_path=json_2fa_path,
        )
    
    # ────────────────────────────────────────────────────────────
    # 12. STATUS BAR CACHE (for UI)
    # ────────────────────────────────────────────────────────────
    
    def compute_status_bar_data(self) -> dict:
        
        try:
            pipeline = [
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }}
            ]
            results = list(self.src_accounts.aggregate(pipeline, allowDiskUse=True))
            
            total = 0
            active_cnt = 0
            revoked_cnt = 0
            pending_cnt = 0
            failed_cnt = 0
            
            for r in results:
                status = r.get("_id", "")
                count = r.get("count", 0)
                total += count
                if status == "active":
                    active_cnt = count
                elif status == "revoked":
                    revoked_cnt = count
                elif status in ("pending", "2fa_required"):
                    pending_cnt += count
                elif status in ("failed", "banned", "restricted"):
                    failed_cnt += count
                else:
                    pending_cnt += count
            
            return {
                "total": total,
                "active": active_cnt,
                "revoked": revoked_cnt,
                "pending": pending_cnt,
                "failed": failed_cnt,
            }
        except Exception as e:
            logger.error(f"compute_status_bar_data error: {e}")
            return {"total": 0, "active": 0, "revoked": 0, "pending": 0, "failed": 0}
        
        
    async def fetch_unprocessed_scraped_pool_paginated(self, skip: int, limit: int) -> list:
       
        pipeline = [
            {"$lookup": {"from": MONGODB_SETTINGS["PROCESSED_MEMBERS_COLLECTION"],
                         "localField": "user_id", "foreignField": "user_identifier", "as": "processed_match"}},
            {"$match": {"processed_match": {"$size": 0}}},
            {"$project": {"processed_match": 0}},
            {"$skip": skip},
            {"$limit": limit},
        ]

        def run_agg():
            self._ensure_connection()
            return list(self.scraped_members.aggregate(pipeline, allowDiskUse=True))

        return await self._run_sync(run_agg)

    def close(self):
        """Close MongoDB connection gracefully (idempotent, PATCH #9)."""
        if getattr(self, "_closed", False):
            return
        self._closed = True
        try:
            if hasattr(self, 'client') and self.client:
                self.client.close()
                logger.info("MongoDB connection closed.")
        except AutoReconnect:
            logger.warning("MongoDB already disconnected on close (ignored).")
        except Exception as e:
            logger.error(f"Error closing MongoDB: {e}")        