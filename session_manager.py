#!/usr/bin/env python3
"""
SessionManager — Single Source of Truth for Telegram Client lifecycle.

Enforces the invariant:
  ONE SESSION -> ONE ACTIVE TELETHON CLIENT -> ONE ACTIVE NETWORK ROUTE

Every TelegramClient creation in the entire system MUST go through
SessionManager.acquire() / SessionManager.context().  No module may
create a TelegramClient directly.

Key responsibilities:
  - Centralised TelegramClient factory (no duplicate clients per session)
  - Session fingerprint tracking
  - Auth-key collision diagnostics (current ownership detection)
  - Structured lifecycle logging (SESSION_CONNECT_START, etc.)
  - Quarantine of AUTH_KEY_DUPLICATED sessions
  - Maximum active client count enforcement
  - Deterministic cleanup on shutdown
"""

from __future__ import annotations

import asyncio
import enum
import hashlib
import logging
import random
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import AuthKeyDuplicatedError

from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
from exception_classifier import ErrorCategory, classify_exception

# Default timeout for proxy lease acquisition (seconds)
PROXY_ACQUIRE_TIMEOUT: float = 30.0

logger = logging.getLogger("SessionManager")


# ──────────────────────────────────────────────
# Lifecycle states
# ──────────────────────────────────────────────


class SessionLifecycleState(str, enum.Enum):
    UNKNOWN = "unknown"
    AVAILABLE = "available"
    RESERVED = "reserved"
    BUSY = "busy"
    QUARANTINED = "quarantined"
    DISCONNECTED = "disconnected"
    TERMINAL = "terminal"
    LOGIN_PENDING = "login_pending"
    OTP_WAITING = "otp_waiting"
    TWOFA_WAITING = "twofa_waiting"


# ──────────────────────────────────────────────
# Terminal DB statuses — never attempt connection
# ──────────────────────────────────────────────

TERMINAL_STATUSES = frozenset({
    "revoked",
    "banned",
    "deactivated",
    "invalid",
    "auth_key_duplicated",
    "permanently_failed",
    "quarantined",
})


# ──────────────────────────────────────────────
# Data structures
# ──────────────────────────────────────────────


@dataclass
class SessionInfo:
    """Runtime state for a single logical session."""

    phone: str
    session_fingerprint: str
    status: str
    lifecycle: SessionLifecycleState = SessionLifecycleState.UNKNOWN
    client: Optional[TelegramClient] = None
    proxy_url: Optional[str] = None
    owner: Optional[str] = None
    worker_id: Optional[str] = None
    client_id: Optional[str] = None
    connection_ts: Optional[float] = None
    last_used_ts: float = field(default_factory=time.time)
    last_error: Optional[str] = None
    creation_ts: float = field(default_factory=time.time)


@dataclass
class SessionLease:
    """Lease record returned when a caller acquires a session."""

    phone: str
    session_fingerprint: str
    client: TelegramClient
    proxy_url: Optional[str]
    owner: str
    worker_id: Optional[str]
    lease_id: str
    acquired_at: float = field(default_factory=time.time)
    proxy_record: Optional[dict] = None


class SessionAlreadyOwnedError(Exception):
    """Raised when a session is already owned by another worker."""
    pass


class SessionManager:
    """
    Centralised session / TelegramClient lifecycle manager.

    Invariants:
      * One phone -> at most one active TelegramClient.
      * If a session is BUSY/RESERVED it cannot be acquired by a second caller.
      * AuthKeyDuplicatedError immediately quarantines the session.
      * _active_count always returns to zero after acquire/release cycles.
    """

    def __init__(
        self,
        db: SuiteDatabase,
        proxy_manager: Optional[Any] = None,
        proxy_lease_manager: Optional[Any] = None,
        max_active_clients: int = 200,
        session_idle_ttl: float = 600.0,
    ):
        self.db = db
        self.proxy_manager = proxy_manager
        self.proxy_lease_manager = proxy_lease_manager
        self._lock = asyncio.Lock()
        self._sessions: Dict[str, SessionInfo] = {}
        self._max_active_clients = max_active_clients
        self._active_count = 0
        self._closed = False
        self._session_idle_ttl = session_idle_ttl

    # ── helpers ──

    @staticmethod
    def session_fingerprint(session_str: str, api_id: int) -> str:
        """Deterministic fingerprint from session string + api_id."""
        raw = f"{session_str}:{api_id}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def normalize_phone(phone: str) -> str:
        return "".join(c for c in str(phone) if c.isdigit())

    def _session_key(self, phone: str) -> str:
        return self.normalize_phone(phone)

    async def _log_lifecycle(
        self,
        event: str,
        *,
        phone: str = "",
        session_fp: str = "",
        module: str = "",
        worker_id: str = "",
        client_id: str = "",
        proxy_url: str = "",
        error: str = "",
        **extra: Any,
    ) -> None:
        """Structured lifecycle logging."""
        safe_phone = phone if phone else "unknown"
        logger.info(
            "SESSION_LIFECYCLE | event=%s | phone=%s | session_fp=%s | module=%s | "
            "worker=%s | client_id=%s | proxy=%s | error=%s | extra=%s",
            event,
            safe_phone,
            session_fp,
            module,
            worker_id,
            client_id,
            proxy_url,
            error,
            extra,
        )

    # ── public API ──

    @asynccontextmanager
    async def acquire(
        self,
        phone: str,
        *,
        module: str = "unknown",
        worker_id: Optional[str] = None,
        proxy_provider: Optional[callable] = None,
        timeout: float = 30.0,
        auto_release: bool = True,
    ) -> AsyncIterator[Optional[SessionLease]]:
        """
        Acquire a session lease for ``phone``.

        Guarantees:
          - If already BUSY/RESERVED by another owner, raises SessionAlreadyOwnedError.
          - If QUARANTINED or TERMINAL, yields None (caller should skip).
          - Creates exactly ONE TelegramClient for the phone.
          - On exit, releases the lease unless ``auto_release`` is False.
        """
        if self._closed:
            raise RuntimeError("SessionManager is closed")

        clean_phone = self._session_key(phone)
        lease_owner_key = f"{module}:{worker_id or uuid.uuid4().hex[:8]}"
        lease: Optional[SessionLease] = None
        reuse_existing: bool = False
        proxy_record: Optional[dict] = None
        client: Optional[TelegramClient] = None

        # The entire pre-yield acquisition is cancellation-safe: if the caller's
        # task is cancelled before a lease is yielded (e.g. while blocked in
        # proxy acquisition, DB I/O, or a stale-client disconnect), we roll back
        # the RESERVED reservation, release any acquired proxy, and disconnect
        # any freshly created client so no resource leaks. A worker must never
        # silently leave an owned session behind.
        try:
            # ── Phase 0: Idle session cleanup (OUTSIDE lock) ──
            await self.cleanup_idle_sessions()

            # ── Phase 1: LOCK → check/reserve state → UNLOCK ──
            async with self._lock:
                existing = self._sessions.get(clean_phone)
                if existing:
                    if existing.lifecycle in (
                        SessionLifecycleState.BUSY,
                        SessionLifecycleState.RESERVED,
                        SessionLifecycleState.LOGIN_PENDING,
                        SessionLifecycleState.OTP_WAITING,
                        SessionLifecycleState.TWOFA_WAITING,
                    ):
                        raise SessionAlreadyOwnedError(
                            f"Session +{clean_phone} is already owned by "
                            f"{existing.owner} (lifecycle={existing.lifecycle.value})"
                        )
                    if existing.lifecycle in (
                        SessionLifecycleState.QUARANTINED,
                        SessionLifecycleState.TERMINAL,
                    ):
                        yield None
                        return

                if self._active_count >= self._max_active_clients:
                    raise RuntimeError(
                        f"Maximum active clients ({self._max_active_clients}) reached"
                    )

                info = existing or SessionInfo(
                    phone=clean_phone,
                    session_fingerprint="",
                    status="",
                )
                if info not in self._sessions.values():
                    self._sessions[clean_phone] = info
                info.lifecycle = SessionLifecycleState.RESERVED
                info.owner = lease_owner_key

            # ── Phase 2: I/O OUTSIDE the lock ──
            record = await self.db.get_session_by_phone_async(clean_phone)
            if not record:
                logger.warning(f"SESSION_ACQUIRE | phone={clean_phone} | no DB record")
                await self._rollback_reservation(clean_phone, lease_owner_key)
                yield None
                return

            status = str(record.get("status", "")).lower()

            if status in TERMINAL_STATUSES:
                session_str = record.get("session_string") or record.get("session", "")
                api_id = int(record.get("api_id", CONFIG["API_ID"]))
                fingerprint = self.session_fingerprint(session_str, api_id)
                await self._log_lifecycle(
                    "SESSION_SKIPPED_TERMINAL",
                    phone=clean_phone,
                    session_fp=fingerprint,
                    module=module,
                    worker_id=worker_id or "",
                    error=f"status={status}",
                )
                async with self._lock:
                    info = self._sessions.get(clean_phone)
                    if info and info.owner == lease_owner_key:
                        info.lifecycle = SessionLifecycleState.TERMINAL
                        info.status = status
                        info.session_fingerprint = fingerprint
                yield None
                return

            session_str = record.get("session_string") or record.get("session")
            if not session_str:
                await self._rollback_reservation(clean_phone, lease_owner_key)
                yield None
                return

            api_id = int(record.get("api_id", CONFIG["API_ID"]))
            api_hash = str(record.get("api_hash", CONFIG["API_HASH"]))
            fingerprint = self.session_fingerprint(session_str, api_id)
            device = record.get("device_metadata") or random.choice(DEVICE_PROFILES)

            # ── Phase 3: Check for reusable existing client ──
            existing_client: Optional[TelegramClient] = None
            async with self._lock:
                info = self._sessions.get(clean_phone)
                if info and info.client and info.client.is_connected():
                    existing_client = info.client

            if existing_client is not None:
                await self._log_lifecycle(
                    "SESSION_REUSED",
                    phone=clean_phone,
                    session_fp=fingerprint,
                    module=module,
                    worker_id=worker_id or "",
                    client_id=str(id(existing_client)),
                )
                client = existing_client
                reuse_existing = True
            else:
                # Client is gone or disconnected — clean up stale reference
                if existing_client is not None:
                    await self._safe_disconnect_client(existing_client)
                    async with self._lock:
                        info = self._sessions.get(clean_phone)
                        if info and info.owner == lease_owner_key:
                            info.client = None
                            info.lifecycle = SessionLifecycleState.RESERVED

                # ── Phase 4: Proxy acquisition + client creation (OUTSIDE lock) ──
                skip_due_to_proxy = False

                if proxy_provider is not None:
                    proxy_record = await proxy_provider(clean_phone)
                elif self.proxy_lease_manager is not None:
                    proxy_record = await self.proxy_lease_manager.acquire_proxy(
                        clean_phone, timeout=PROXY_ACQUIRE_TIMEOUT
                    )
                    if proxy_record is None:
                        logger.warning(
                            f"SESSION_SKIP_NO_PROXY | phone={clean_phone} | "
                            f"module={module} | all proxies leased or timed out"
                        )
                        skip_due_to_proxy = True

                if skip_due_to_proxy:
                    await self._log_lifecycle(
                        "SESSION_SKIP_NO_PROXY",
                        phone=clean_phone,
                        session_fp=fingerprint,
                        module=module,
                        worker_id=worker_id or "",
                        error="proxy lease timeout",
                    )
                    await self._rollback_reservation(clean_phone, lease_owner_key)
                    yield None
                    return

                client = self._create_client(
                    session_str=session_str,
                    api_id=api_id,
                    api_hash=api_hash,
                    device=device,
                    proxy=proxy_record,
                )

                client_id = str(id(client))
                await self._log_lifecycle(
                    "SESSION_CONNECT_START",
                    phone=clean_phone,
                    session_fp=fingerprint,
                    module=module,
                    worker_id=worker_id or "",
                    client_id=client_id,
                    proxy_url=(proxy_record or {}).get("url", "") if proxy_record else "",
                )

                # ── Phase 5: LOCK → commit state → UNLOCK ──
                async with self._lock:
                    info = self._sessions.get(clean_phone)
                    if info is None:
                        info = SessionInfo(
                            phone=clean_phone,
                            session_fingerprint=fingerprint,
                            status=status,
                            lifecycle=SessionLifecycleState.RESERVED,
                            client=client,
                            proxy_url=(proxy_record or {}).get("url", "") if proxy_record else None,
                            owner=lease_owner_key,
                            worker_id=worker_id,
                            client_id=client_id,
                            connection_ts=time.time(),
                        )
                        self._sessions[clean_phone] = info
                    else:
                        if info.owner != lease_owner_key:
                            raise SessionAlreadyOwnedError(
                                f"Session +{clean_phone} was acquired by another owner "
                                f"while lock was released"
                            )
                        info.client = client
                        info.lifecycle = SessionLifecycleState.RESERVED
                        info.owner = lease_owner_key
                        info.worker_id = worker_id
                        info.client_id = client_id
                        info.connection_ts = time.time()
                        info.proxy_url = (proxy_record or {}).get("url", "") if proxy_record else None
                        info.session_fingerprint = fingerprint
                        info.status = status

                    info.lifecycle = SessionLifecycleState.BUSY
                    info.owner = lease_owner_key
                    info.last_used_ts = time.time()
                    self._active_count += 1

            # ── Phase 6: Create lease (OUTSIDE lock, common path for reuse and new) ──
            proxy_url_for_lease = proxy_record.get("url", "") if proxy_record else None
            if reuse_existing:
                async with self._lock:
                    info = self._sessions.get(clean_phone)
                    proxy_url_for_lease = info.proxy_url if info else None

            lease = SessionLease(
                phone=clean_phone,
                session_fingerprint=fingerprint,
                client=client,
                proxy_url=proxy_url_for_lease,
                owner=lease_owner_key,
                worker_id=worker_id,
                lease_id=uuid.uuid4().hex[:12],
                proxy_record=(proxy_record if proxy_record else None),
            )

            # ── yield the lease to the caller (lock already released) ──
            try:
                yield lease
            except AuthKeyDuplicatedError:
                # Safety net: if caller doesn't catch this, quarantine automatically
                await self.mark_quarantined(
                    clean_phone,
                    reason="AuthKeyDuplicatedError in acquire() caller",
                    category=ErrorCategory.AUTH_KEY_DUPLICATED,
                )
                raise
        except asyncio.CancelledError:
            # Rollback any pre-yield state so the session is not left owned.
            await self._cancel_rollback(
                clean_phone, lease_owner_key, proxy_record, client
            )
            raise
        finally:
            clean_phone_release = self._session_key(phone)
            if lease is not None:
                if auto_release:
                    await self._release_lease(clean_phone_release, lease_owner_key)
                if (
                    lease.proxy_record
                    and lease.proxy_url
                    and self.proxy_lease_manager is not None
                ):
                    await self.proxy_lease_manager.release_proxy(
                        proxy_url=lease.proxy_url,
                        phone=clean_phone_release,
                    )

    # ── Login flow ownership ──
    # The login/OTP/2FA flow creates an interactive TelegramClient BEFORE any
    # session_string exists in the DB. During this window the phone must be
    # reserved so no other module (auditor/DM/adder/scraper/videochat/web) can
    # acquire it. LOGIN_PENDING → OTP_WAITING → TWOFA_WAITING → (release).
    # The SAME client is reused across all login steps; it is not a SessionManager
    # acquired client (there is no authorized session yet), so we track it as an
    # owned reservation with an explicit owner key.

    async def reserve_login(
        self, phone: str, owner_key: str, client: Optional[Any] = None
    ) -> bool:
        """
        Atomically reserve a phone for login. Returns True if reserved, False if
        already owned by someone else or terminal.
        """
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info and info.lifecycle in (
                SessionLifecycleState.BUSY,
                SessionLifecycleState.RESERVED,
                SessionLifecycleState.LOGIN_PENDING,
                SessionLifecycleState.OTP_WAITING,
                SessionLifecycleState.TWOFA_WAITING,
                SessionLifecycleState.QUARANTINED,
                SessionLifecycleState.TERMINAL,
            ):
                return False

            record = await self.db.get_session_by_phone_async(clean_phone)
            if record:
                status = str(record.get("status", "")).lower()
                if status in TERMINAL_STATUSES:
                    return False

            new_info = SessionInfo(
                phone=clean_phone,
                session_fingerprint="",
                status="login_pending",
                lifecycle=SessionLifecycleState.LOGIN_PENDING,
                client=client,
                owner=owner_key,
            )
            self._sessions[clean_phone] = new_info
            return True

    async def set_login_stage(
        self, phone: str, owner_key: str, stage: SessionLifecycleState
    ) -> bool:
        """Advance the login state (LOGIN_PENDING → OTP_WAITING → TWOFA_WAITING)."""
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if not info or info.owner != owner_key:
                return False
            info.lifecycle = stage
            return True

    async def release_login(self, phone: str, owner_key: str) -> None:
        """
        Release a login reservation after completion/failure. Disconnects the
        login client and removes the reservation so the phone can be acquired by
        normal modules once the session is authorized and saved to DB.
        """
        clean_phone = self._session_key(phone)
        client_to_disconnect: Optional[TelegramClient] = None
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info and info.owner == owner_key and info.lifecycle in (
                SessionLifecycleState.LOGIN_PENDING,
                SessionLifecycleState.OTP_WAITING,
                SessionLifecycleState.TWOFA_WAITING,
            ):
                client_to_disconnect = info.client
                self._sessions.pop(clean_phone, None)
            else:
                logger.warning(
                    f"LOGIN_RELEASE_SKIPPED | phone={clean_phone} | "
                    f"owner={owner_key} | state={info.lifecycle if info else None}"
                )
        if client_to_disconnect is not None:
            await self._safe_disconnect_client(client_to_disconnect)

    async def _rollback_reservation(self, clean_phone: str, owner_key: str) -> None:
        """Release a RESERVED state without disconnecting client or releasing proxy."""
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info and info.owner == owner_key:
                info.lifecycle = SessionLifecycleState.AVAILABLE
                info.owner = None
                info.worker_id = None
                info.last_used_ts = time.time()

    async def _cancel_rollback(
        self,
        clean_phone: str,
        owner_key: str,
        proxy_record: Optional[dict],
        client: Optional[TelegramClient],
    ) -> None:
        """Cancellation safety: release a RESERVED reservation, free an acquired
        proxy, and disconnect a freshly-created client whose lease was never
        yielded. Called only when a caller task is cancelled during pre-yield
        acquire() phases. No lock is held across any await."""
        # Release the reservation for this owner (no disconnect under lock).
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info and info.owner == owner_key:
                info.client = None
                info.lifecycle = SessionLifecycleState.AVAILABLE
                info.owner = None
                info.worker_id = None
                info.last_used_ts = time.time()

        # Free any proxy that was acquired before cancellation.
        if proxy_record and self.proxy_lease_manager is not None:
            url = proxy_record.get("url", "")
            if url:
                await self.proxy_lease_manager.release_proxy(
                    proxy_url=url,
                    phone=clean_phone,
                )

        # Disconnect a client created but never leased.
        if client is not None:
            await self._safe_disconnect_client(client)

    async def _release_lease(self, phone_key: str, owner_key: str) -> None:
        async with self._lock:
            info = self._sessions.get(phone_key)
            if not info:
                return
            if info.owner != owner_key:
                logger.warning(
                    f"LEASE_RELEASE_OWNER_MISMATCH | phone={phone_key} | "
                    f"expected_owner={owner_key} | actual_owner={info.owner}"
                )
                return
            if info.lifecycle == SessionLifecycleState.AVAILABLE:
                logger.warning(
                    f"DOUBLE_RELEASE_ATTEMPT | phone={phone_key} | owner={owner_key}"
                )
                return
            info.lifecycle = SessionLifecycleState.AVAILABLE
            info.owner = None
            info.worker_id = None
            info.last_used_ts = time.time()

    async def release_lease(self, lease: Optional[SessionLease]) -> None:
        """
        PUBLIC lease-release interface (Phase 3.4).

        Feature modules (adder, dmsender, web_console, videochat, main_bot)
        MUST release a session through this method rather than the private
        ``_release_lease``. Ownership is verified from the lease record, so an
        accidental release of another worker's lease is rejected.

        Idempotent: ``lease=None`` or an already-released lease is a no-op
        (a double release is logged as DOUBLE_RELEASE_ATTEMPT by
        ``_release_lease``).
        """
        if lease is None:
            return
        phone_key = self._session_key(lease.phone)
        await self._release_lease(phone_key, lease.owner)

    async def mark_quarantined(
        self, phone: str, reason: str, category: ErrorCategory
    ) -> None:
        """Mark a session as quarantined (e.g. AuthKeyDuplicatedError)."""
        clean_phone = self._session_key(phone)
        client_to_disconnect: Optional[TelegramClient] = None
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info:
                info.lifecycle = SessionLifecycleState.QUARANTINED
                info.last_error = f"{category.value}: {reason}"
                client_to_disconnect = info.client
                info.client = None
                self._active_count = max(0, self._active_count - 1)

        if client_to_disconnect is not None:
            await self._safe_disconnect_client(client_to_disconnect)

        # Map category to correct DB status
        category_to_db_status = {
            ErrorCategory.AUTH_KEY_DUPLICATED: "auth_key_duplicated",
            ErrorCategory.SESSION_REVOKED: "revoked",
            ErrorCategory.AUTH_KEY_UNREGISTERED: "revoked",
            ErrorCategory.ACCOUNT_BANNED: "banned",
            ErrorCategory.UNAUTHORIZED: "revoked",
        }
        db_status = category_to_db_status.get(category, "failed")
        try:
            self._update_db_status_sync(clean_phone, db_status, reason)
        except Exception as e:
            logger.error(f"DB status update failed for {clean_phone}: {e}")

        async with self._lock:
            info = self._sessions.get(clean_phone)
            session_fp = info.session_fingerprint if info else ""
        await self._log_lifecycle(
            "SESSION_QUARANTINED",
            phone=clean_phone,
            session_fp=session_fp,
            module="session_manager",
            error=f"category={category.value}, reason={reason}",
        )

    async def release(self, phone: str, module: str) -> None:
        """Release a previously acquired session."""
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if not info:
                return
            if info.owner != module:
                logger.warning(
                    f"RELEASE_OWNER_MISMATCH | phone={clean_phone} | "
                    f"expected={module} | actual={info.owner}"
                )
                return
            if info.lifecycle == SessionLifecycleState.AVAILABLE:
                logger.warning(
                    f"DOUBLE_RELEASE_ATTEMPT | phone={clean_phone} | module={module}"
                )
                return
            info.lifecycle = SessionLifecycleState.AVAILABLE
            info.owner = None
            info.worker_id = None
            info.last_used_ts = time.time()

    async def release_proxy(self, phone: str, proxy_url: str) -> None:
        """Release proxy lease associated with a session."""
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info:
                info.proxy_url = None

        if self.proxy_lease_manager and proxy_url:
            await self.proxy_lease_manager.release_proxy(proxy_url=proxy_url, phone=clean_phone)
        elif self.proxy_manager and proxy_url:
            try:
                self.proxy_manager.mark_failed({"url": proxy_url})
            except Exception:
                pass

    # ── internal ──

    def _create_client(
        self,
        *,
        session_str: str,
        api_id: int,
        api_hash: str,
        device: dict,
        proxy: Optional[dict] = None,
    ) -> TelegramClient:
        """Factory: create a single TelegramClient with normalised proxy."""
        return TelegramClient(
            StringSession(session_str),
            api_id=api_id,
            api_hash=api_hash,
            device_model=device.get("device_model", "PC 64bit"),
            system_version=device.get("system_version", "Windows 11"),
            app_version=device.get("app_version", "4.8.4"),
            proxy=proxy,
            entity_cache_limit=100,
            sequential_updates=False,
            receive_updates=False,
            timeout=10.0,
            connection_retries=1,
            request_retries=1,
        )

    async def _safe_disconnect_client(self, client: Optional[TelegramClient]) -> None:
        if not client:
            return
        try:
            if client.is_connected():
                await asyncio.wait_for(client.disconnect(), timeout=3.0)
        except Exception as e:
            logger.debug(f"Safe disconnect error: {e}")

    def _update_db_status_sync(self, phone: str, status: str, reason: str) -> None:
        """Update DB account status synchronously."""
        try:
            if status not in ("revoked", "banned"):
                self.db.mark_account_failed(phone, reason)
            self.db.update_session_status(phone, status)
        except Exception as e:
            logger.error(f"DB status update failed for {phone}: {e}")

    async def disconnect_all(self) -> int:
        """Disconnect every tracked client. Called on shutdown."""
        clients_to_disconnect: list = []
        async with self._lock:
            for info in list(self._sessions.values()):
                if info.client:
                    clients_to_disconnect.append(info.client)
                    info.client = None
                    info.lifecycle = SessionLifecycleState.DISCONNECTED
            count = len(clients_to_disconnect)
            self._active_count = 0
        for client in clients_to_disconnect:
            await self._safe_disconnect_client(client)
        return count

    async def get_stats(self) -> Dict[str, Any]:
        """Return runtime statistics."""
        terminal = sum(
            1 for s in self._sessions.values()
            if s.lifecycle in (SessionLifecycleState.TERMINAL, SessionLifecycleState.QUARANTINED)
        )
        busy = sum(
            1 for s in self._sessions.values()
            if s.lifecycle in (SessionLifecycleState.BUSY, SessionLifecycleState.RESERVED)
        )
        return {
            "total_tracked": len(self._sessions),
            "active_clients": self._active_count,
            "busy": busy,
            "quarantined": terminal,
            "max_clients": self._max_active_clients,
        }

    async def cleanup_idle_sessions(self) -> int:
        """Remove AVAILABLE sessions idle beyond TTL. Returns count removed."""
        now = time.time()
        removed = []
        async with self._lock:
            for phone_key, info in list(self._sessions.items()):
                if (
                    info.lifecycle == SessionLifecycleState.AVAILABLE
                    and (now - info.last_used_ts) > self._session_idle_ttl
                ):
                    removed.append((phone_key, info))
            for phone_key, info in removed:
                del self._sessions[phone_key]
                if info.client:
                    self._active_count = max(0, self._active_count - 1)
        # Disconnect clients outside lock
        for phone_key, info in removed:
            if info.client:
                await self._safe_disconnect_client(info.client)
        return len(removed)

    async def shutdown(self) -> None:
        """Graceful shutdown — disconnect all clients."""
        self._closed = True
        count = await self.disconnect_all()
        logger.info(f"SessionManager shutdown: disconnected {count} clients")


# Backwards-compatible helper used by legacy modules
async def safe_acquire_session(
    session_manager: SessionManager,
    phone: str,
    *,
    module: str = "unknown",
    worker_id: Optional[str] = None,
    proxy_provider=None,
    timeout: float = 30.0,
) -> Optional[SessionLease]:
    async with session_manager.acquire(
        phone,
        module=module,
        worker_id=worker_id,
        proxy_provider=proxy_provider,
        timeout=timeout,
        auto_release=False,
    ) as lease:
        return lease
