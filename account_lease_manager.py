#!/usr/bin/env python3
"""
AccountLeaseManager — Runtime account ownership and leasing.

Replaces the old sync dict-based lock system in database.py with a proper
async lease manager that:

  - Uses explicit AVAILABLE / RESERVED / BUSY / QUARANTINED states.
  - Supports lease expiration (TTL) so crashed workers release accounts.
  - Prevents the auditor from touching accounts owned by active workloads.
  - Provides bounded waiting with timeouts for acquiring accounts.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from database import SuiteDatabase
from exception_classifier import ErrorCategory, classify_exception

logger = logging.getLogger("AccountLeaseManager")


class AccountState(str, Enum):
    """Runtime account ownership states."""
    AVAILABLE = "available"
    RESERVED = "reserved"
    BUSY = "busy"
    QUARANTINED = "quarantined"
    TERMINAL = "terminal"

    # DB-mapped states that are terminal (never schedule)
    DB_TERMINAL = "db_terminal"


# DB statuses that should never enter active queues
TERMINAL_DB_STATUSES = frozenset({
    "revoked",
    "banned",
    "deactivated",
    "invalid",
    "auth_key_duplicated",
    "permanently_failed",
    "quarantined",
})

# DB statuses eligible for work
ELIGIBLE_DB_STATUSES = frozenset({
    "active",
    "pending",
    "2fa_required",
    "restricted",
})


@dataclass
class AccountLease:
    """Active lease record for an account."""
    phone: str
    session_fingerprint: str
    owner: str          # module:worker_id
    worker_id: str
    module: str
    proxy_id: Optional[str] = None
    acquired_at: float = field(default_factory=time.time)
    last_activity: float = field(default_factory=time.time)
    expires_at: float = field(default_factory=lambda: time.time() + 3600)
    lease_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    def touch(self) -> None:
        self.last_activity = time.time()
        self.expires_at = time.time() + 3600


class AccountLeaseManager:
    """
    Manages real-time account ownership as a separate concern from
    persistent DB state.

    Runtime state (this manager) is authoritative for:
      - active proxy lease
      - active session owner
      - active client
      - worker assignment

    Persistent DB state is authoritative for:
      - account metadata
      - eligibility filtering (status check before scheduling)
    """

    def __init__(
        self,
        db: SuiteDatabase,
        lease_ttl: float = 3600.0,
    ):
        self.db = db
        self._lease_ttl = lease_ttl
        self._lock = asyncio.Lock()
        self._leases: Dict[str, AccountLease] = {}
        self._states: Dict[str, AccountState] = {}
        self._reaper_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._stats: Dict[str, Any] = {
            "leases_active": 0,
            "leases_acquired": 0,
            "leases_released": 0,
            "leases_expired": 0,
            "quarantines": 0,
        }

    # ── lifecycle ──

    async def start(self) -> None:
        """Start the background lease reaper."""
        if self._is_running:
            return
        self._is_running = True
        self._reaper_task = asyncio.create_task(self._reaper_loop())
        logger.info("AccountLeaseManager started with reaper")

    async def stop(self) -> None:
        """Stop the reaper and release all leases."""
        self._is_running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass
            self._reaper_task = None
        async with self._lock:
            self._leases.clear()
            self._states.clear()
        logger.info("AccountLeaseManager stopped")

    async def _reaper_loop(self) -> None:
        """Periodically reclaim expired leases."""
        while self._is_running:
            try:
                await asyncio.sleep(30)
                now = time.time()
                expired: List[str] = []
                async with self._lock:
                    for phone_key, lease in list(self._leases.items()):
                        if lease.is_expired():
                            expired.append(phone_key)
                    for phone_key in expired:
                        lease = self._leases.pop(phone_key)
                        self._states[phone_key] = AccountState.AVAILABLE
                        self._stats["leases_expired"] += 1
                        logger.warning(
                            f"LEASE_EXPIRED | phone={phone_key} | owner={lease.owner} | "
                            f"leased_for={int(now - lease.acquired_at)}s"
                        )
                if expired:
                    logger.info(f"Reaper reclaimed {len(expired)} expired leases")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reaper error: {e}")

    # ── normalisation ──

    @staticmethod
    def normalize_phone(phone: str) -> str:
        return "".join(c for c in str(phone) if c.isdigit())

    def _key(self, phone: str) -> str:
        return self.normalize_phone(phone)

    # ── eligibility ──

    async def is_eligible(self, phone: str) -> bool:
        """
        Check DB status before scheduling an account.

        Returns True only if the account's DB status is in ELIGIBLE_DB_STATUSES.
        Terminal states are never eligible.
        """
        clean_phone = self._key(phone)
        record = await self.db.get_session_by_phone_async(clean_phone)
        if not record:
            return False
        status = str(record.get("status", "")).lower()
        return status in ELIGIBLE_DB_STATUSES

    async def filter_eligible(self, accounts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter a list of DB account documents to only eligible ones."""
        eligible: List[Dict[str, Any]] = []
        for acc in accounts:
            status = str(acc.get("status", "")).lower()
            if status in ELIGIBLE_DB_STATUSES:
                eligible.append(acc)
        return eligible

    # ── acquisition ──

    async def acquire(
        self,
        phone: str,
        *,
        module: str = "unknown",
        worker_id: Optional[str] = None,
        timeout: float = 30.0,
        skip_if_busy: bool = False,
    ) -> Optional[AccountLease]:
        """
        Reserve an account for exclusive use by ``module``.

        Args:
            phone: Account phone number.
            module: Which module is requesting (auditor, adder, dm, etc.)
            worker_id: Optional worker identifier.
            timeout: Max seconds to wait if account is BUSY.
            skip_if_busy: If True, return None immediately instead of waiting.

        Returns:
            AccountLease if acquired, None if:
            - account is QUARANTINED or TERMINAL
            - timeout reached
            - skip_if_busy=True and account is BUSY
        """
        clean_phone = self._key(phone)
        owner = f"{module}:{worker_id or uuid.uuid4().hex[:8]}"

        async with self._lock:
            existing = self._leases.get(clean_phone)
            state = self._states.get(clean_phone, AccountState.AVAILABLE)

            if existing and not existing.is_expired():
                if state in (AccountState.BUSY, AccountState.RESERVED):
                    if skip_if_busy:
                        logger.debug(f"ACCOUNT_SKIP_BUSY | phone={clean_phone} | owner={existing.owner}")
                        return None
                    # Will wait outside the lock
                    pass
                elif state == AccountState.QUARANTINED:
                    logger.debug(f"ACCOUNT_SKIP_QUARANTINED | phone={clean_phone}")
                    return None

        # Wait outside the lock if busy
        deadline = time.time() + timeout
        while True:
            async with self._lock:
                existing = self._leases.get(clean_phone)
                state = self._states.get(clean_phone, AccountState.AVAILABLE)

                if state == AccountState.QUARANTINED:
                    return None

                if state == AccountState.TERMINAL:
                    return None

                if (existing is None or existing.is_expired()) and state in (
                    AccountState.AVAILABLE,
                    AccountState.TERMINAL,
                ):
                    # Record session fingerprint
                    record = await self.db.get_session_by_phone_async(clean_phone)
                    if record:
                        sess_str = record.get("session_string") or record.get("session", "")
                        api_id = int(record.get("api_id", 0))
                        from session_manager import SessionManager
                        fingerprint = SessionManager.session_fingerprint(sess_str, api_id) if sess_str else ""
                    else:
                        fingerprint = ""

                    lease = AccountLease(
                        phone=clean_phone,
                        session_fingerprint=fingerprint,
                        owner=owner,
                        worker_id=worker_id or "",
                        module=module,
                    )
                    self._leases[clean_phone] = lease
                    self._states[clean_phone] = AccountState.RESERVED
                    self._stats["leases_acquired"] += 1
                    self._stats["leases_active"] = len(self._leases)
                    logger.debug(f"ACCOUNT_ACQUIRED | phone={clean_phone} | owner={owner}")
                    return lease

                if existing and not existing.is_expired() and state in (
                    AccountState.BUSY,
                    AccountState.RESERVED,
                ):
                    if skip_if_busy:
                        return None
                    if existing.owner == owner:
                        # Same owner re-acquiring
                        lease = existing
                        lease.touch()
                        self._states[clean_phone] = AccountState.BUSY
                        self._stats["leases_active"] = len(self._leases)
                        return lease

            if time.time() > deadline:
                logger.warning(f"ACCOUNT_ACQUIRE_TIMEOUT | phone={clean_phone} | module={module}")
                return None

            await asyncio.sleep(0.5)

    async def release(self, phone: str, owner: str) -> bool:
        """
        Release an account lease.

        Verifies ownership before releasing to prevent accidentally freeing
        another worker's lease.
        """
        clean_phone = self._key(phone)
        async with self._lock:
            lease = self._leases.get(clean_phone)
            if lease:
                if lease.owner != owner:
                    logger.warning(
                        f"LEASE_RELEASE_OWNER_MISMATCH | phone={clean_phone} | "
                        f"expected_owner={owner} | actual_owner={lease.owner}"
                    )
                    return False
                del self._leases[clean_phone]
                self._states[clean_phone] = AccountState.AVAILABLE
                self._stats["leases_released"] += 1
                self._stats["leases_active"] = len(self._leases)
                logger.debug(f"ACCOUNT_RELEASED | phone={clean_phone} | owner={owner}")
                return True
            return False

    async def fail_account(
        self,
        phone: str,
        category: ErrorCategory,
        reason: str,
    ) -> None:
        """
        Mark an account as quarantined or terminal based on error category.

        AuthKeyDuplicatedError and similar integrity errors -> QUARANTINED (terminal)
        Transient errors -> keep AVAILABLE for retry
        """
        clean_phone = self._key(phone)
        async with self._lock:
            if category in (
                ErrorCategory.AUTH_KEY_DUPLICATED,
                ErrorCategory.SESSION_REVOKED,
                ErrorCategory.AUTH_KEY_UNREGISTERED,
                ErrorCategory.ACCOUNT_BANNED,
            ):
                self._states[clean_phone] = AccountState.QUARANTINED
                self._stats["quarantines"] += 1
                # Remove from active leases
                self._leases.pop(clean_phone, None)
                self._stats["leases_active"] = len(self._leases)
            elif category in (
                ErrorCategory.NETWORK_TIMEOUT,
                ErrorCategory.PROXY_ERROR,
            ):
                # Transient — keep available, just remove from active lease
                lease = self._leases.pop(clean_phone, None)
                if lease:
                    self._stats["leases_active"] = len(self._leases)
                self._states[clean_phone] = AccountState.AVAILABLE
            else:
                # Other errors — keep available
                lease = self._leases.pop(clean_phone, None)
                if lease:
                    self._stats["leases_active"] = len(self._leases)
                self._states[clean_phone] = AccountState.AVAILABLE

        # Update DB status
        db_mappings = {
            ErrorCategory.AUTH_KEY_DUPLICATED: "auth_key_duplicated",
            ErrorCategory.AUTH_KEY_UNREGISTERED: "revoked",
            ErrorCategory.SESSION_REVOKED: "revoked",
            ErrorCategory.ACCOUNT_BANNED: "banned",
            ErrorCategory.ACCOUNT_FLOOD: "failed",
            ErrorCategory.NETWORK_TIMEOUT: "failed",
            ErrorCategory.PROXY_ERROR: "failed",
        }
        db_status = db_mappings.get(category, "failed")
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self.db.update_session_status(phone, db_status, None),
            )
        except Exception as e:
            logger.error(f"DB status update failed in fail_account: {e}")

        logger.warning(
            f"ACCOUNT_FAILED | phone={clean_phone} | category={category.value} | "
            f"reason={reason} | new_state={self._states.get(clean_phone, AccountState.AVAILABLE).value}"
        )

    async def get_state(self, phone: str) -> AccountState:
        """Get the runtime state of an account."""
        clean_phone = self._key(phone)
        async with self._lock:
            return self._states.get(clean_phone, AccountState.AVAILABLE)

    async def is_busy(self, phone: str) -> bool:
        """Check if an account is currently busy/leased."""
        clean_phone = self._key(phone)
        async with self._lock:
            state = self._states.get(clean_phone, AccountState.AVAILABLE)
            return state in (AccountState.BUSY, AccountState.RESERVED)

    async def get_stats(self) -> Dict[str, Any]:
        """Return runtime statistics."""
        async with self._lock:
            return {
                **self._stats,
                "leases_active": len(self._leases),
                "available": sum(
                    1 for s in self._states.values()
                    if s == AccountState.AVAILABLE
                ),
                "busy": sum(
                    1 for s in self._states.values()
                    if s in (AccountState.BUSY, AccountState.RESERVED)
                ),
                "quarantined": sum(
                    1 for s in self._states.values()
                    if s == AccountState.QUARANTINED
                ),
                "terminal": sum(
                    1 for s in self._states.values()
                    if s == AccountState.TERMINAL
                ),
            }

    async def get_owned_phones(self) -> List[str]:
        """Return list of phones currently in active leases."""
        async with self._lock:
            return list(self._leases.keys())
