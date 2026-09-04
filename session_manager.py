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
    """

    def __init__(
        self,
        db: SuiteDatabase,
        proxy_manager: Optional[Any] = None,
        proxy_lease_manager: Optional[Any] = None,
        max_active_clients: int = 200,
    ):
        self.db = db
        self.proxy_manager = proxy_manager
        self.proxy_lease_manager = proxy_lease_manager
        self._lock = asyncio.Lock()
        self._sessions: Dict[str, SessionInfo] = {}
        self._max_active_clients = max_active_clients
        self._active_count = 0
        self._closed = False

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
        lease: Optional[SessionLease] = None
        lease_owner_key = f"{module}:{worker_id or uuid.uuid4().hex[:8]}"

        async with self._lock:
            existing = self._sessions.get(clean_phone)
            if existing:
                if existing.lifecycle in (
                    SessionLifecycleState.BUSY,
                    SessionLifecycleState.RESERVED,
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

            record = self.db.get_session_by_phone(clean_phone)
            if not record:
                logger.warning(f"SESSION_ACQUIRE | phone={clean_phone} | no DB record")
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
                self._sessions[clean_phone] = SessionInfo(
                    phone=clean_phone,
                    session_fingerprint=fingerprint,
                    status=status,
                    lifecycle=SessionLifecycleState.TERMINAL,
                )
                yield None
                return

            session_str = record.get("session_string") or record.get("session")
            if not session_str:
                yield None
                return

            api_id = int(record.get("api_id", CONFIG["API_ID"]))
            api_hash = str(record.get("api_hash", CONFIG["API_HASH"]))
            fingerprint = self.session_fingerprint(session_str, api_id)
            device = record.get("device_metadata") or random.choice(DEVICE_PROFILES)

            info = self._sessions.get(clean_phone)
            client: Optional[TelegramClient] = None

            if info and info.client:
                client = info.client
                if client.is_connected():
                    await self._log_lifecycle(
                        "SESSION_REUSED",
                        phone=clean_phone,
                        session_fp=fingerprint,
                        module=module,
                        worker_id=worker_id or "",
                        client_id=str(id(client)),
                    )
                else:
                    await self._safe_disconnect_client(client)
                    client = None
                    info.client = None
                    info.lifecycle = SessionLifecycleState.UNKNOWN

            if client is None:
                proxy_record: Optional[dict] = None
                skip_due_to_proxy = False

                if proxy_provider is not None:
                    # Explicit callable — caller decides whether to use a proxy
                    proxy_record = await proxy_provider(clean_phone)
                elif self.proxy_lease_manager is not None:
                    # Default fallback: acquire from lease manager
                    proxy_record = await self.proxy_lease_manager.acquire_proxy(
                        clean_phone, timeout=PROXY_ACQUIRE_TIMEOUT
                    )
                    if proxy_record is None:
                        # 🔥 CRITICAL: Proxy lease timed out — DO NOT create
                        # a client without a proxy.  This would cause
                        # AuthKeyDuplicatedError if the session was previously
                        # used through a different IP.  Skip this account instead.
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
                    self._active_count += 1
                else:
                    info.client = client
                    info.lifecycle = SessionLifecycleState.RESERVED
                    info.owner = lease_owner_key
                    info.worker_id = worker_id
                    info.client_id = client_id
                    info.connection_ts = time.time()
                    info.proxy_url = (proxy_record or {}).get("url", "") if proxy_record else None
                    if not info.client:
                        pass

            info.lifecycle = SessionLifecycleState.BUSY
            info.owner = lease_owner_key
            info.last_used_ts = time.time()

            lease = SessionLease(
                phone=clean_phone,
                session_fingerprint=fingerprint,
                client=client,
                proxy_url=info.proxy_url,
                owner=lease_owner_key,
                worker_id=worker_id,
                lease_id=uuid.uuid4().hex[:12],
                proxy_record=(proxy_record if proxy_record else None),
            )

        try:
            yield lease
        finally:
            clean_phone = self._session_key(phone)
            if auto_release:
                await self._release_lease(clean_phone, lease_owner_key)
            # 🔥 Always release proxy lease if one was acquired
            if (
                lease
                and lease.proxy_record
                and lease.proxy_url
                and self.proxy_lease_manager is not None
            ):
                await self.proxy_lease_manager.release_proxy(
                    phone=phone if isinstance(phone, str) else str(phone),
                    proxy_url=lease.proxy_url,
                )

    async def _release_lease(self, phone_key: str, owner_key: str) -> None:
        async with self._lock:
            info = self._sessions.get(phone_key)
            if info and info.owner == owner_key:
                info.lifecycle = SessionLifecycleState.AVAILABLE
                info.owner = None
                info.worker_id = None
                info.last_used_ts = time.time()

    async def mark_quarantined(
        self, phone: str, reason: str, category: ErrorCategory
    ) -> None:
        """Mark a session as quarantined (e.g. AuthKeyDuplicatedError)."""
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info:
                info.lifecycle = SessionLifecycleState.QUARANTINED
                info.last_error = f"{category.value}: {reason}"
                if info.client:
                    await self._safe_disconnect_client(info.client)
                    info.client = None
                self._active_count = max(0, self._active_count - 1)

            await self._update_db_status_sync(phone, "auth_key_duplicated", reason)

        await self._log_lifecycle(
            "SESSION_QUARANTINED",
            phone=clean_phone,
            session_fp=info.session_fingerprint if info else "",
            module="session_manager",
            error=f"category={category.value}, reason={reason}",
        )

    async def release(self, phone: str, module: str) -> None:
        """Release a previously acquired session."""
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if info and info.owner == module:
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
            await self.proxy_lease_manager.release_proxy(proxy_url, clean_phone)
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
        finally:
            self._active_count = max(0, self._active_count - 1)

    def _update_db_status_sync(self, phone: str, status: str, reason: str) -> None:
        """Update DB account status synchronously (for use inside lock)."""
        try:
            self.db.mark_account_failed(phone, reason) if status != "revoked" else None
            self.db.update_session_status(phone, status)
        except Exception as e:
            logger.error(f"DB status update failed for {phone}: {e}")

    async def disconnect_all(self) -> int:
        """Disconnect every tracked client. Called on shutdown."""
        count = 0
        async with self._lock:
            for info in list(self._sessions.values()):
                if info.client:
                    await self._safe_disconnect_client(info.client)
                    info.client = None
                    info.lifecycle = SessionLifecycleState.DISCONNECTED
                    count += 1
            self._active_count = 0
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
