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
from urllib.parse import urlparse

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
    phone: str
    session_fingerprint: str
    status: str
    lifecycle: SessionLifecycleState = SessionLifecycleState.UNKNOWN

    client: Optional[TelegramClient] = None
    client_building: bool = False

    proxy_url: Optional[str] = None
    proxy_id: Optional[str] = None
    proxy_lease_id: Optional[str] = None

    owner: Optional[str] = None
    worker_id: Optional[str] = None

    reservation_id: Optional[str] = None
    lease_id: Optional[str] = None

    client_id: Optional[str] = None
    connection_ts: Optional[float] = None
    last_used_ts: float = field(default_factory=time.time)
    last_error: Optional[str] = None
    creation_ts: float = field(default_factory=time.time)


@dataclass
class SessionLease:
    phone: str
    session_fingerprint: str
    client: TelegramClient

    proxy_url: Optional[str]
    proxy_id: Optional[str]
    proxy_lease_id: Optional[str]

    owner: str
    worker_id: Optional[str]
    lease_id: str

    acquired_at: float = field(
        default_factory=time.time
    )

    proxy_record: Optional[dict] = None

    proxy_should_cooldown: bool = False
    proxy_cooldown_reason: str = ""

    released: bool = False


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
    def _safe_proxy_label(proxy_url: Optional[str]) -> str:
        """
        Return a proxy label without exposing credentials.
        """
        if not proxy_url:
            return ""
    
        try:
            parsed = urlparse(proxy_url)
            host = parsed.hostname or ""
            port = parsed.port or ""
            return f"{host}:{port}" if host else "<proxy>"
        except Exception:
            return "<proxy>"    
        
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
        Acquire exclusive ownership of one user session.
    
        Lifecycle:
    
            reserve
              -> load DB record
              -> validate status
              -> acquire proxy
              -> create client
              -> atomically attach ownership
              -> yield lease
              -> release lease when auto_release=True
    
        Important:
          * A short-lived operation owns the client for the full lease lifetime.
          * The client and proxy are released together.
          * A pre-yield failure rolls everything back.
          * No session-manager lock is held during I/O.
        """
        if self._closed:
            raise RuntimeError("SessionManager is closed")
    
        clean_phone = self._session_key(phone)
        lease_owner_key = f"{module}:{worker_id or uuid.uuid4().hex[:8]}"
        reservation_id = uuid.uuid4().hex[:12]
    
        lease: Optional[SessionLease] = None
        proxy_record: Optional[dict] = None
        client: Optional[TelegramClient] = None
        yielded = False
    
        try:
            # ----------------------------------------------------------
            # PHASE 0: Clean genuinely idle sessions.
            # ----------------------------------------------------------
            await self.cleanup_idle_sessions()
    
            # ----------------------------------------------------------
            # PHASE 1: Reserve phone in memory.
            # NO external I/O while holding the lock.
            # ----------------------------------------------------------
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
                            f"{existing.owner} "
                            f"(lifecycle={existing.lifecycle.value})"
                        )
    
                    if existing.lifecycle in (
                        SessionLifecycleState.QUARANTINED,
                        SessionLifecycleState.TERMINAL,
                    ):
                        await self._log_lifecycle(
                            "SESSION_SKIPPED_TERMINAL",
                            phone=clean_phone,
                            session_fp=existing.session_fingerprint,
                            module=module,
                            worker_id=worker_id or "",
                            error=f"lifecycle={existing.lifecycle.value}",
                        )
                        yield None
                        return
    
                    # Never reuse a connected client in the normal short-lived
                    # acquisition path. A released lease owns its client until
                    # disconnect. Persistent workloads must explicitly keep their
                    # lease alive with auto_release=False.
                    if existing.client is not None:
                        raise RuntimeError(
                            f"Session +{clean_phone} has an unexpected retained "
                            f"client while lifecycle={existing.lifecycle.value}"
                        )
    
                if self._active_count >= self._max_active_clients:
                    raise RuntimeError(
                        f"Maximum active clients "
                        f"({self._max_active_clients}) reached"
                    )
    
                info = existing or SessionInfo(
                    phone=clean_phone,
                    session_fingerprint="",
                    status="",
                )
    
                self._sessions[clean_phone] = info
    
                info.lifecycle = SessionLifecycleState.RESERVED
                info.owner = lease_owner_key
                info.worker_id = worker_id
                info.reservation_id = reservation_id
                info.lease_id = None
                info.last_used_ts = time.time()
    
            # ----------------------------------------------------------
            # PHASE 2: DB I/O OUTSIDE lock.
            # ----------------------------------------------------------
            record = await self.db.get_session_by_phone_async(clean_phone)
    
            if not record:
                await self._rollback_reservation(
                    clean_phone,
                    lease_owner_key,
                    reservation_id,
                )
                yield None
                return
    
            status = str(record.get("status", "")).lower()
    
            if status in TERMINAL_STATUSES:
                session_str = (
                    record.get("session_string")
                    or record.get("session", "")
                )
    
                api_id = int(
                    record.get(
                        "api_id",
                        CONFIG["API_ID"],
                    )
                )
    
                fingerprint = self.session_fingerprint(
                    session_str,
                    api_id,
                )
    
                async with self._lock:
                    info = self._sessions.get(clean_phone)
    
                    if info and info.owner == lease_owner_key:
                        info.lifecycle = SessionLifecycleState.TERMINAL
                        info.status = status
                        info.session_fingerprint = fingerprint
                        info.owner = None
                        info.worker_id = None
                        info.reservation_id = None
                        info.lease_id = None
    
                await self._log_lifecycle(
                    "SESSION_SKIPPED_TERMINAL",
                    phone=clean_phone,
                    session_fp=fingerprint,
                    module=module,
                    worker_id=worker_id or "",
                    error=f"status={status}",
                )
    
                yield None
                return
    
            # ----------------------------------------------------------
            # PHASE 3: Validate session information.
            # ----------------------------------------------------------
            session_str = (
                record.get("session_string")
                or record.get("session")
            )
    
            if not session_str:
                await self._rollback_reservation(
                    clean_phone,
                    lease_owner_key,
                    reservation_id,
                )
                yield None
                return
    
            api_id = int(
                record.get(
                    "api_id",
                    CONFIG["API_ID"],
                )
            )
    
            api_hash = str(
                record.get(
                    "api_hash",
                    CONFIG["API_HASH"],
                )
            )
    
            fingerprint = self.session_fingerprint(
                session_str,
                api_id,
            )
    
            device = (
                record.get("device_metadata")
                or random.choice(DEVICE_PROFILES)
            )
    
            # ----------------------------------------------------------
            # PHASE 4: Acquire ONE proxy route.
            # ----------------------------------------------------------
            if proxy_provider is not None:
                proxy_record = await proxy_provider(clean_phone)
    
            elif self.proxy_lease_manager is not None:
                proxy_record = await self.proxy_lease_manager.acquire_proxy(
                    clean_phone,
                    timeout=PROXY_ACQUIRE_TIMEOUT,
                )
    
            if self.proxy_lease_manager is not None and proxy_record is None:
                await self._log_lifecycle(
                    "SESSION_WAITING_FOR_PROXY",
                    phone=clean_phone,
                    session_fp=fingerprint,
                    module=module,
                    worker_id=worker_id or "",
                    error="proxy acquisition returned no lease",
                )
    
                await self._rollback_reservation(
                    clean_phone,
                    lease_owner_key,
                    reservation_id,
                )
    
                yield None
                return
    
            # ----------------------------------------------------------
            # PHASE 5: Create exactly ONE Telegram client.
            # ----------------------------------------------------------
            client = self._create_client(
                session_str=session_str,
                api_id=api_id,
                api_hash=api_hash,
                device=device,
                proxy=proxy_record,
            )
    
            client_id = str(id(client))
    
            await self._log_lifecycle(
                "SESSION_CLIENT_CREATED",
                phone=clean_phone,
                session_fp=fingerprint,
                module=module,
                worker_id=worker_id or "",
                client_id=client_id,
                proxy_url=(
                    (proxy_record or {}).get("url", "")
                    if proxy_record else ""
                ),
            )
    
            # ----------------------------------------------------------
            # PHASE 6: Commit client + owner atomically.
            # ----------------------------------------------------------
            lease_id = uuid.uuid4().hex[:12]
    
            async with self._lock:
                info = self._sessions.get(clean_phone)
    
                if info is None:
                    raise SessionAlreadyOwnedError(
                        f"Session +{clean_phone} disappeared during acquisition"
                    )
    
                if info.owner != lease_owner_key:
                    raise SessionAlreadyOwnedError(
                        f"Session +{clean_phone} was acquired by another "
                        f"owner while I/O was in progress"
                    )

                if info.reservation_id != reservation_id:
                    raise SessionAlreadyOwnedError(
                        f"Session +{clean_phone} reservation changed "
                        "while I/O was in progress"
                    )
    
                if info.client is not None:
                    raise SessionAlreadyOwnedError(
                        f"Session +{clean_phone} already has an active client"
                    )
    
                info.client = client
                info.session_fingerprint = fingerprint
                info.status = status
                info.lifecycle = SessionLifecycleState.BUSY
                info.owner = lease_owner_key
                info.worker_id = worker_id
                info.lease_id = lease_id
                info.client_id = client_id
                info.connection_ts = time.time()
                info.last_used_ts = time.time()
                info.proxy_url = (
                    proxy_record.get("url")
                    if proxy_record
                    else None
                )
                
                info.proxy_id = (
                    proxy_record.get("__proxy_id")
                    if proxy_record
                    else None
                )
                
                info.proxy_lease_id = (
                    proxy_record.get("__lease_id")
                    if proxy_record
                    else None                
                )
                self._active_count += 1
    
            # ----------------------------------------------------------
            # PHASE 7: Produce the lease.
            # ----------------------------------------------------------

            lease = SessionLease(
                phone=clean_phone,
                session_fingerprint=fingerprint,
                client=client,
            
                proxy_url=(
                    proxy_record.get("url")
                    if proxy_record
                    else None
                ),
            
                proxy_id=(
                    proxy_record.get("__proxy_id")
                    if proxy_record
                    else None
                ),
            
                proxy_lease_id=(
                    proxy_record.get("__lease_id")
                    if proxy_record
                    else None
                ),
            
                owner=lease_owner_key,
                worker_id=worker_id,
                lease_id=lease_id,
                proxy_record=proxy_record,
            )            
    
            await self._log_lifecycle(
                "SESSION_ACQUIRED",
                phone=clean_phone,
                session_fp=fingerprint,
                module=module,
                worker_id=worker_id or "",
                client_id=client_id,
                proxy_url=lease.proxy_url or "",
                lease_id=lease_id,
            )
    
            yielded = True
            yield lease
    
        except AuthKeyDuplicatedError:
            # Handle terminal auth-key duplication centrally.
            await self.mark_quarantined(
                clean_phone,
                reason="AuthKeyDuplicatedError during session operation",
                category=ErrorCategory.AUTH_KEY_DUPLICATED,
            )
            raise
    
        except asyncio.CancelledError:
            if not yielded:
                await self._rollback_acquire_failure(
                    clean_phone=clean_phone,
                    owner_key=lease_owner_key,
                    reservation_id=reservation_id,
                    proxy_record=proxy_record,
                    client=client,
                )
            raise
    
        except BaseException:
            # IMPORTANT:
            # This catches acquisition failures that happen AFTER proxy/client
            # creation but BEFORE the lease reaches the caller.
            if not yielded:
                await self._rollback_acquire_failure(
                    clean_phone=clean_phone,
                    owner_key=lease_owner_key,
                    reservation_id=reservation_id,
                    proxy_record=proxy_record,
                    client=client,
                )
            raise
    
        finally:
            if lease is not None and auto_release:
                await self.release_lease(lease)

    # ── Login flow ownership ──
    # The login/OTP/2FA flow creates an interactive TelegramClient BEFORE any
    # session_string exists in the DB. During this window the phone must be
    # reserved so no other module (auditor/DM/adder/scraper/videochat/web) can
    # acquire it. LOGIN_PENDING → OTP_WAITING → TWOFA_WAITING → (release).
    # The SAME client is reused across all login steps; it is not a SessionManager
    # acquired client (there is no authorized session yet), so we track it as an
    # owned reservation with an explicit owner key.

    async def reserve_login(
        self,
        phone: str,
        owner_key: str,
        client: Optional[Any] = None,
    ) -> bool:
        """
        Reserve a phone for login without holding the runtime lock across DB I/O.
        """
        clean_phone = self._session_key(phone)
    
        # --------------------------------------------
        # DB check OUTSIDE runtime lock.
        # --------------------------------------------
        record = await self.db.get_session_by_phone_async(
            clean_phone
        )
    
        if record:
            status = str(
                record.get("status", "")
            ).lower()
    
            if status in TERMINAL_STATUSES:
                return False
    
        # --------------------------------------------
        # Atomic in-memory reservation.
        # --------------------------------------------
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
    
            new_info = SessionInfo(
                phone=clean_phone,
                session_fingerprint="",
                status="login_pending",
                lifecycle=SessionLifecycleState.LOGIN_PENDING,
                client=client,
                owner=owner_key,
                worker_id=owner_key,
                lease_id=uuid.uuid4().hex[:12],
            )
    
            if client is not None:
                new_info.client_id = str(id(client))
                new_info.connection_ts = time.time()
                self._active_count += 1
    
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

    async def release_login(
        self,
        phone: str,
        owner_key: str,
    ) -> None:
        """
        Release a login reservation and its client safely.
        """
        clean_phone = self._session_key(phone)
        client_to_disconnect: Optional[TelegramClient] = None
        proxy_url: Optional[str] = None
        proxy_id: Optional[str] = None
        proxy_lease_id: Optional[str] = None
    
        async with self._lock:
            info = self._sessions.get(clean_phone)
    
            if not info:
                return
    
            if info.owner != owner_key:
                logger.warning(
                    "LOGIN_RELEASE_OWNER_MISMATCH | "
                    "phone=%s | expected=%s | actual=%s",
                    clean_phone,
                    owner_key,
                    info.owner,
                )
                return
    
            if info.lifecycle not in (
                SessionLifecycleState.LOGIN_PENDING,
                SessionLifecycleState.OTP_WAITING,
                SessionLifecycleState.TWOFA_WAITING,
            ):
                logger.warning(
                    "LOGIN_RELEASE_INVALID_STATE | "
                    "phone=%s | state=%s",
                    clean_phone,
                    info.lifecycle.value,
                )
                return
    
            client_to_disconnect = info.client
            proxy_url = info.proxy_url
            proxy_id = info.proxy_id
            proxy_lease_id = info.proxy_lease_id
    
            if client_to_disconnect is not None:
                self._active_count = max(
                    0,
                    self._active_count - 1,
                )
    
            self._sessions.pop(clean_phone, None)
    
        if client_to_disconnect is not None:
            await self._safe_disconnect_client(
                client_to_disconnect
            )
    
        if (
            proxy_url
            or proxy_id
        ) and self.proxy_lease_manager is not None:
            try:
                await self.proxy_lease_manager.release_proxy(
                    proxy_url=proxy_url,
                    proxy_id=proxy_id,
                    lease_id=proxy_lease_id,
                    phone=clean_phone,
                )
            except Exception as exc:
                logger.error(
                    "LOGIN_PROXY_RELEASE_FAILED | "
                    "phone=%s | error=%s",
                    clean_phone,
                    exc,
                )

    async def build_login_client(
        self,
        phone: str,
        owner_key: str,
        *,
        session_str: str,
        api_id: int,
        api_hash: str,
        device: dict,
        proxy: Optional[dict] = None,
    ) -> Optional[TelegramClient]:
    
        clean_phone = self._session_key(phone)
    
        login_lifecycles = (
            SessionLifecycleState.LOGIN_PENDING,
            SessionLifecycleState.OTP_WAITING,
            SessionLifecycleState.TWOFA_WAITING,
        )
    
        async with self._lock:
            info = self._sessions.get(clean_phone)
    
            if (
                not info
                or info.owner != owner_key
                or info.lifecycle not in login_lifecycles
            ):
                return None
    
            if info.client is not None:
                return info.client
    
            if info.client_building:
                return None
    
            info.client_building = True
    
        client: Optional[TelegramClient] = None
        proxy_record = proxy
    
        try:
            # If login caller does not supply a proxy, acquire one here.
            if proxy_record is None and self.proxy_lease_manager is not None:
                proxy_record = await self.proxy_lease_manager.acquire_proxy(
                    clean_phone,
                    timeout=PROXY_ACQUIRE_TIMEOUT,
                )
    
                if proxy_record is None:
                    return None
    
            client = self._create_client(
                session_str=session_str,
                api_id=api_id,
                api_hash=api_hash,
                device=device,
                proxy=proxy_record,
            )
    
            async with self._lock:
                info = self._sessions.get(clean_phone)
    
                if (
                    not info
                    or info.owner != owner_key
                    or info.lifecycle not in login_lifecycles
                ):
                    raise SessionAlreadyOwnedError(
                        f"Login reservation lost for +{clean_phone}"
                    )
    
                if info.client is not None:
                    raise SessionAlreadyOwnedError(
                        f"Login client already exists for +{clean_phone}"
                    )
    
                info.client = client

                info.proxy_url = (
                    proxy_record.get("url")
                    if proxy_record
                    else None
                )
                
                info.proxy_id = (
                    proxy_record.get("__proxy_id")
                    if proxy_record
                    else None
                )
                
                info.proxy_lease_id = (
                    proxy_record.get("__lease_id")
                    if proxy_record
                    else None
                )

                info.client_id = str(id(client))
                info.connection_ts = time.time()
                info.last_used_ts = time.time()
                info.client_building = False
    
                self._active_count += 1
    
            return client
    
        except BaseException:
            if client is not None:
                await self._safe_disconnect_client(client)
    
            if (
                proxy_record
                and self.proxy_lease_manager is not None
            ):
                proxy_url = proxy_record.get("url")
                proxy_id = proxy_record.get("__proxy_id")
                proxy_lease_id = proxy_record.get("__lease_id")

                if proxy_url or proxy_id:
                    try:
                        await self.proxy_lease_manager.release_proxy(
                            proxy_url=proxy_url,
                            proxy_id=proxy_id,
                            lease_id=proxy_lease_id,
                            phone=clean_phone,
                        )
                    except Exception as exc:
                        logger.error(
                            "LOGIN_BUILD_PROXY_ROLLBACK_FAILED | "
                            "phone=%s | error=%s",
                            clean_phone,
                            exc,
                        )
    
            raise
    
        finally:
            async with self._lock:
                info = self._sessions.get(clean_phone)
                if info and info.owner == owner_key:
                    info.client_building = False

    async def is_owned(self, phone: str) -> bool:
        """
        True if ``phone`` is currently owned by THIS SessionManager instance:
        i.e. BUSY/RESERVED by a worker, or held by a login/OTP/2FA reservation.
        Used by background loops (auditor/recovery) to guarantee they never touch
        an account another module is actively using.
        """
        clean_phone = self._session_key(phone)
        async with self._lock:
            info = self._sessions.get(clean_phone)
            if not info:
                return False
            return info.lifecycle in (
                SessionLifecycleState.BUSY,
                SessionLifecycleState.RESERVED,
                SessionLifecycleState.LOGIN_PENDING,
                SessionLifecycleState.OTP_WAITING,
                SessionLifecycleState.TWOFA_WAITING,
            )

    async def _rollback_reservation(
        self,
        clean_phone: str,
        owner_key: str,
        reservation_id: Optional[str] = None,
    ) -> None:
        """
        Roll back a reservation that never acquired a client/proxy.
        """
        async with self._lock:
            info = self._sessions.get(clean_phone)
    
            if not info:
                return
    
            if (
                info.owner != owner_key
                or (
                    reservation_id is not None
                    and info.reservation_id != reservation_id
                )
            ):
                return
    
            # Never overwrite terminal/quarantined states.
            if info.lifecycle in (
                SessionLifecycleState.QUARANTINED,
                SessionLifecycleState.TERMINAL,
            ):
                info.owner = None
                info.worker_id = None
                info.reservation_id = None
                info.lease_id = None
                return
    
            info.lifecycle = SessionLifecycleState.AVAILABLE
            info.owner = None
            info.worker_id = None
            info.reservation_id = None
            info.lease_id = None
            info.last_used_ts = time.time()

    async def _rollback_acquire_failure(
        self,
        *,
        clean_phone: str,
        owner_key: str,
        reservation_id: Optional[str],
        proxy_record: Optional[dict],
        client: Optional[TelegramClient],
    ) -> None:
        """
        Roll back resources acquired before a SessionLease was successfully
        yielded to the caller.
    
        This is the critical pre-yield cleanup path.
        """
        was_counted = False
    
        async with self._lock:
            info = self._sessions.get(clean_phone)
    
            if (
                info
                and info.owner == owner_key
                and info.reservation_id == reservation_id
            ):
                was_counted = info.client is not None
    
                info.client = None
                info.proxy_url = None
                info.proxy_id = None
                info.proxy_lease_id = None
                info.owner = None
                info.worker_id = None
                info.reservation_id = None
                info.lease_id = None
                info.client_id = None
                info.connection_ts = None
                info.last_used_ts = time.time()
    
                if info.lifecycle not in (
                    SessionLifecycleState.QUARANTINED,
                    SessionLifecycleState.TERMINAL,
                ):
                    info.lifecycle = SessionLifecycleState.AVAILABLE
    
            if was_counted:
                self._active_count = max(
                    0,
                    self._active_count - 1,
                )
    
        # Never hold SessionManager lock during I/O.
        if client is not None:
            await self._safe_disconnect_client(client)
    
        if (
            proxy_record
            and self.proxy_lease_manager is not None
        ):
            proxy_url = proxy_record.get("url")
            proxy_id = proxy_record.get("__proxy_id")
            proxy_lease_id = proxy_record.get("__lease_id")

            if proxy_url or proxy_id:
                try:
                    await self.proxy_lease_manager.release_proxy(
                        proxy_url=proxy_url,
                        proxy_id=proxy_id,
                        lease_id=proxy_lease_id,
                        phone=clean_phone,
                    )
                except Exception as exc:
                    logger.error(
                        "ACQUIRE_ROLLBACK_PROXY_RELEASE_FAILED | "
                        "phone=%s | error=%s",
                        clean_phone,
                        exc,
                    )


    async def _cancel_rollback(
        self,
        clean_phone: str,
        owner_key: str,
        reservation_id: Optional[str],
        proxy_record: Optional[dict],
        client: Optional[TelegramClient],
    ) -> None:
        """Backward-compatible cancellation rollback wrapper."""
        await self._rollback_acquire_failure(
            clean_phone=clean_phone,
            owner_key=owner_key,
            reservation_id=reservation_id,
            proxy_record=proxy_record,
            client=client,
        )

    async def _release_lease(
        self,
        phone_key: str,
        owner_key: str,
        lease_id: Optional[str] = None,
    ) -> None:
        """
        Fully release a runtime session.
    
        Ownership state, client lifetime, and proxy lifetime are released together.
    
        No external I/O occurs while self._lock is held.
        """
        client_to_disconnect: Optional[TelegramClient] = None
        proxy_url: Optional[str] = None
        proxy_id: Optional[str] = None
        proxy_lease_id: Optional[str] = None
        lifecycle_after_release = SessionLifecycleState.AVAILABLE
        client_was_active = False
    
        async with self._lock:
            info = self._sessions.get(phone_key)
    
            if not info:
                return
    
            # Strong ownership check.
            if info.owner != owner_key:
                logger.warning(
                    "LEASE_RELEASE_OWNER_MISMATCH | "
                    "phone=%s | expected=%s | actual=%s",
                    phone_key,
                    owner_key,
                    info.owner,
                )
                return
    
            # When a lease_id is available, require an exact match.
            if (
                lease_id is not None
                and info.lease_id != lease_id
            ):
                logger.warning(
                    "LEASE_RELEASE_ID_MISMATCH | "
                    "phone=%s | owner=%s | expected_lease=%s | "
                    "actual_lease=%s",
                    phone_key,
                    owner_key,
                    lease_id,
                    info.lease_id,
                )
                return
    
            # Preserve terminal/quarantine state.
            if info.lifecycle in (
                SessionLifecycleState.QUARANTINED,
                SessionLifecycleState.TERMINAL,
            ):
                lifecycle_after_release = info.lifecycle
    
            client_to_disconnect = info.client
            proxy_url = info.proxy_url
            proxy_id = info.proxy_id
            proxy_lease_id = info.proxy_lease_id
    
            if client_to_disconnect is not None:
                client_was_active = True
                self._active_count = max(
                    0,
                    self._active_count - 1,
                )
    
            # Clear runtime ownership.
            info.client = None
            
            info.proxy_url = None
            info.proxy_id = None
            info.proxy_lease_id = None
            
            info.owner = None
            info.worker_id = None
            info.reservation_id = None
            info.lease_id = None
            
            info.client_id = None
            info.connection_ts = None
            info.last_used_ts = time.time()
            info.lifecycle = lifecycle_after_release
    
        # ----------------------------------------------------------
        # I/O OUTSIDE LOCK
        # ----------------------------------------------------------
        if client_to_disconnect is not None:
            await self._safe_disconnect_client(
                client_to_disconnect
            )
    
        if (
            proxy_url
            or proxy_id
        ) and self.proxy_lease_manager is not None:
            try:
                await self.proxy_lease_manager.release_proxy(
                    proxy_url=proxy_url,
                    proxy_id=proxy_id,
                    lease_id=proxy_lease_id,
                    phone=phone_key,
                )
            except Exception as exc:
                logger.error(
                    "SESSION_PROXY_RELEASE_FAILED | "
                    "phone=%s | proxy=%s | error=%s",
                    phone_key,
                    self._safe_proxy_label(proxy_url),
                    exc,
                )
    
        await self._log_lifecycle(
            "SESSION_RELEASED",
            phone=phone_key,
            module=owner_key,
            client_id="",
            proxy_url=proxy_url or "",
            extra={
                "client_was_active": client_was_active,
                "active_count": self._active_count,
            },
        )

    async def release_lease(
        self,
        lease: Optional[SessionLease],
    ) -> None:
        """
        Public idempotent lease-release API.

        Ownership is validated BEFORE any mutation: a wrong-owner or stale
        lease is rejected without touching lifecycle state, the client, the
        proxy lease, or any counter. The lease object is only marked released
        once its ownership is confirmed, so legitimate release attempts are
        never blocked by a rejected forgery of the same lease.
        """
        if lease is None:
            return
    
        if lease.released:
            logger.debug(
                "DOUBLE_RELEASE_ATTEMPT | phone=%s | lease_id=%s",
                self._session_key(lease.phone),
                lease.lease_id,
            )
            return
    
        phone_key = self._session_key(lease.phone)
    
        # Validate ownership + lease epoch before any mutation.
        async with self._lock:
            info = self._sessions.get(phone_key)
    
            if not info:
                lease.released = True
                return
    
            if info.owner != lease.owner:
                logger.warning(
                    "LEASE_RELEASE_OWNER_MISMATCH | "
                    "phone=%s | expected=%s | actual=%s",
                    phone_key,
                    lease.owner,
                    info.owner,
                )
                return
    
            if (
                lease.lease_id is not None
                and info.lease_id != lease.lease_id
            ):
                logger.warning(
                    "LEASE_RELEASE_ID_MISMATCH | "
                    "phone=%s | owner=%s | expected_lease=%s | "
                    "actual_lease=%s",
                    phone_key,
                    lease.owner,
                    lease.lease_id,
                    info.lease_id,
                )
                return
    
            # Ownership confirmed: guard against concurrent duplicate releases.
            lease.released = True
    
        await self._release_lease(
            phone_key,
            lease.owner,
            lease.lease_id,
        )

    async def mark_quarantined(
        self,
        phone: str,
        reason: str,
        category: ErrorCategory,
    ) -> None:
        """
        Mark a session as terminal/quarantined and immediately release
        its live client/proxy resources.
    
        Terminal state is NEVER changed back to AVAILABLE by normal release.
        """
        clean_phone = self._session_key(phone)
    
        client_to_disconnect: Optional[TelegramClient] = None
        proxy_url: Optional[str] = None
        proxy_id: Optional[str] = None
        proxy_lease_id: Optional[str] = None
        had_client = False
        session_fp = ""
    
        async with self._lock:
            info = self._sessions.get(clean_phone)
    
            if info:
                session_fp = info.session_fingerprint
    
                info.lifecycle = SessionLifecycleState.QUARANTINED
                info.last_error = (
                    f"{category.value}: {reason}"
                )
    
                client_to_disconnect = info.client
                proxy_url = info.proxy_url
                proxy_id = info.proxy_id
                proxy_lease_id = info.proxy_lease_id
    
                had_client = client_to_disconnect is not None
    
                info.client = None
                info.proxy_url = None
                info.proxy_id = None
                info.proxy_lease_id = None
                info.owner = None
                info.worker_id = None
                info.lease_id = None
                info.client_id = None
    
                if had_client:
                    self._active_count = max(
                        0,
                        self._active_count - 1,
                    )
    
        # I/O outside lock.
        if client_to_disconnect is not None:
            await self._safe_disconnect_client(
                client_to_disconnect
            )
    
        if (
            proxy_url
            or proxy_id
        ) and self.proxy_lease_manager is not None:
            try:
                await self.proxy_lease_manager.release_proxy(
                    proxy_url=proxy_url,
                    proxy_id=proxy_id,
                    lease_id=proxy_lease_id,
                    phone=clean_phone,
                )
            except Exception as exc:
                logger.error(
                    "QUARANTINE_PROXY_RELEASE_FAILED | "
                    "phone=%s | error=%s",
                    clean_phone,
                    exc,
                )
    
        category_to_db_status = {
            ErrorCategory.AUTH_KEY_DUPLICATED: "auth_key_duplicated",
            ErrorCategory.SESSION_REVOKED: "revoked",
            ErrorCategory.AUTH_KEY_UNREGISTERED: "revoked",
            ErrorCategory.ACCOUNT_BANNED: "banned",
            ErrorCategory.UNAUTHORIZED: "revoked",
        }
    
        db_status = category_to_db_status.get(
            category,
            "permanently_failed",
        )
    
        try:
            await asyncio.to_thread(
                self._update_db_status_sync,
                clean_phone,
                db_status,
                reason,
            )
        except Exception as exc:
            logger.error(
                "QUARANTINE_DB_UPDATE_FAILED | "
                "phone=%s | error=%s",
                clean_phone,
                exc,
            )
    
        await self._log_lifecycle(
            "SESSION_QUARANTINED",
            phone=clean_phone,
            session_fp=session_fp,
            module="session_manager",
            error=(
                f"category={category.value}; "
                f"reason={reason}"
            ),
        )

    async def release(
        self,
        phone: str,
        module: str,
    ) -> None:
        """
        Backward-compatible release API.
    
        New code must use release_lease(SessionLease).
        """
        clean_phone = self._session_key(phone)
    
        logger.warning(
            "DEPRECATED_SESSION_RELEASE_API | "
            "phone=%s | module=%s",
            clean_phone,
            module,
        )
    
        async with self._lock:
            info = self._sessions.get(clean_phone)
    
            if not info:
                return
    
            if info.owner != module:
                logger.warning(
                    "RELEASE_OWNER_MISMATCH | "
                    "phone=%s | expected=%s | actual=%s",
                    clean_phone,
                    module,
                    info.owner,
                )
                return
    
            lease_id = info.lease_id
    
        await self._release_lease(
            clean_phone,
            module,
            lease_id,
        )

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

    def create_client(
        self,
        *,
        session_str: str,
        api_id: int = 0,
        api_hash: str = "",
        device: Optional[dict] = None,
        proxy: Optional[dict] = None,
    ) -> TelegramClient:
        """
        PUBLIC factory — the only sanctioned way to obtain a TelegramClient for a
        user session OUTSIDE the acquire()/lease lifecycle (e.g. the deprecated
        backward-compat wrappers or one-off diagnostic construction).

        Does NOT reserve/lease the phone; callers that perform work on an account
        MUST use acquire()/managed_client() so ownership is enforced. This is a
        thin pass-through to the single private factory (no second construction
        path), kept public so no module reaches the private _create_client.
        """
        api_id = api_id or CONFIG["API_ID"]
        api_hash = api_hash or CONFIG["API_HASH"]
        return self._create_client(
            session_str=session_str,
            api_id=api_id,
            api_hash=api_hash,
            device=device or {},
            proxy=proxy,
        )

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
        """
        Disconnect every tracked client and release proxy ownership.

        Ordered shutdown (never hold the lock during I/O):

          stop new work       -> snapshot all tracked sessions under the lock
          clear ownership     -> atomically clear client/proxy/owner fields and
                                 set _active_count to zero
          disconnect clients  -> I/O outside the lock
          release proxies     -> I/O outside the lock, using the precise
                                 proxy_id / lease_id captured per session

        Terminal/quarantined lifecycles are preserved (never flipped back to
        AVAILABLE). Idempotent: a second call has nothing left to release.
        """
        snapshots: list = []

        async with self._lock:
            for phone_key, info in list(self._sessions.items()):
                snapshots.append(
                    (
                        phone_key,
                        info.client,
                        info.proxy_url,
                        info.proxy_id,
                        info.proxy_lease_id,
                    )
                )

                if info.lifecycle not in (
                    SessionLifecycleState.QUARANTINED,
                    SessionLifecycleState.TERMINAL,
                ):
                    info.lifecycle = SessionLifecycleState.DISCONNECTED

                info.client = None
                info.proxy_url = None
                info.proxy_id = None
                info.proxy_lease_id = None
                info.owner = None
                info.worker_id = None
                info.lease_id = None
                info.client_id = None
                info.connection_ts = None
                info.last_used_ts = time.time()

            self._active_count = 0

        # I/O outside the lock: disconnect clients, then release matching
        # proxy leases using the exact proxy_id / lease_id captured above.
        for (
            phone_key,
            client,
            proxy_url,
            proxy_id,
            proxy_lease_id,
        ) in snapshots:
            if client is not None:
                await self._safe_disconnect_client(client)

            if (proxy_url or proxy_id) and self.proxy_lease_manager is not None:
                try:
                    await self.proxy_lease_manager.release_proxy(
                        proxy_url=proxy_url,
                        proxy_id=proxy_id,
                        lease_id=proxy_lease_id,
                        phone=phone_key,
                    )
                except Exception as exc:
                    logger.error(
                        "DISCONNECT_ALL_PROXY_RELEASE_FAILED | "
                        "phone=%s | error=%s",
                        phone_key,
                        exc,
                    )

        return len(snapshots)

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total_tracked = len(self._sessions)
    
            terminal = sum(
                1
                for s in self._sessions.values()
                if s.lifecycle in (
                    SessionLifecycleState.TERMINAL,
                    SessionLifecycleState.QUARANTINED,
                )
            )
    
            busy = sum(
                1
                for s in self._sessions.values()
                if s.lifecycle in (
                    SessionLifecycleState.BUSY,
                    SessionLifecycleState.RESERVED,
                    SessionLifecycleState.LOGIN_PENDING,
                    SessionLifecycleState.OTP_WAITING,
                    SessionLifecycleState.TWOFA_WAITING,
                )
            )
    
            return {
                "total_tracked": total_tracked,
                "active_clients": self._active_count,
                "busy": busy,
                "quarantined": terminal,
                "max_clients": self._max_active_clients,
            }

    async def validate_invariants(self) -> Dict[str, Any]:
        """
        Internal consistency check.
    
        Intended for tests/diagnostics.
        """
        async with self._lock:
            live_clients = 0
            owned_sessions = 0
            violations = []
    
            for phone, info in self._sessions.items():
    
                if info.client is not None:
                    live_clients += 1
    
                if info.owner is not None:
                    owned_sessions += 1
    
                if (
                    info.lifecycle == SessionLifecycleState.BUSY
                    and info.client is None
                ):
                    violations.append(
                        f"{phone}: BUSY without client"
                    )
    
                if (
                    info.client is None
                    and info.proxy_url is not None
                    and info.lifecycle != SessionLifecycleState.LOGIN_PENDING
                    and info.lifecycle != SessionLifecycleState.OTP_WAITING
                    and info.lifecycle != SessionLifecycleState.TWOFA_WAITING
                ):
                    violations.append(
                        f"{phone}: proxy exists without client"
                    )
    
                if (
                    info.owner is None
                    and info.lease_id is not None
                ):
                    violations.append(
                        f"{phone}: lease_id without owner"
                    )
    
                if (
                    info.owner is None
                    and info.lifecycle == SessionLifecycleState.BUSY
                ):
                    violations.append(
                        f"{phone}: BUSY without owner"
                    )
    
            if live_clients != self._active_count:
                violations.append(
                    "active_count mismatch: "
                    f"counter={self._active_count}, "
                    f"actual={live_clients}"
                )
    
            return {
                "ok": not violations,
                "active_count": self._active_count,
                "live_clients": live_clients,
                "owned_sessions": owned_sessions,
                "tracked_sessions": len(self._sessions),
                "violations": violations,
            }
        
    async def cleanup_idle_sessions(self) -> int:
        """
        Retire idle AVAILABLE clients without creating a window where another
        worker can create a second client for the same session.
        """
        now = time.time()
        retiring = []
    
        async with self._lock:
            for phone_key, info in list(
                self._sessions.items()
            ):
                if (
                    info.lifecycle == SessionLifecycleState.AVAILABLE
                    and info.client is not None
                    and (
                        now - info.last_used_ts
                        > self._session_idle_ttl
                    )
                ):
                    cleanup_owner = (
                        f"__cleanup__:"
                        f"{uuid.uuid4().hex[:8]}"
                    )
    
                    info.lifecycle = SessionLifecycleState.RESERVED
                    info.owner = cleanup_owner
                    info.lease_id = None
    
                    retiring.append(
                        (
                            phone_key,
                            cleanup_owner,
                            info.client,
                        )
                    )
    
        removed = 0
    
        for phone_key, cleanup_owner, client in retiring:
    
            await self._safe_disconnect_client(client)
    
            async with self._lock:
                info = self._sessions.get(phone_key)
    
                if (
                    info
                    and info.owner == cleanup_owner
                    and info.lifecycle == SessionLifecycleState.RESERVED
                ):
                    info.client = None
                    info.proxy_url = None
                    info.owner = None
                    info.worker_id = None
                    info.lease_id = None
                    info.client_id = None
                    info.lifecycle = (
                        SessionLifecycleState.DISCONNECTED
                    )
                    info.last_used_ts = time.time()
    
                    self._active_count = max(
                        0,
                        self._active_count - 1,
                    )
    
                    removed += 1
    
        return removed


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
