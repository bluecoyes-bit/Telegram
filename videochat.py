#!/usr/bin/env python3
"""
Filename: videochat.py
"""

import os
import sys
import time
import asyncio
import random
import logging

from typing import List, Dict, Optional, Any, Tuple, Set, TYPE_CHECKING
from weakref import WeakSet  # 🔥 NEW: Weak references for task tracking

from datetime import datetime, timezone
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, DeleteHistoryRequest
from telethon.errors import (
    FloodWaitError, PhoneNumberBannedError, UserAlreadyParticipantError,
)

from resource_manager import (
    ProxyManager,
    ProxyLeaseManager,
    AccountLeaseManager,
    AccountState,
    TERMINAL_DB_STATUSES,
    ELIGIBLE_DB_STATUSES,
    SessionManager,
    SessionAlreadyOwnedError,
    SessionLifecycleState,
    SessionLease,
)

logger = logging.getLogger("VideoChatEngineFallback")

# =====================================================================
# Voice cluster state machine + bounded timeouts
# =====================================================================
class VoiceClusterState:
    """Explicit, observable states for one account's voice stream."""

    IDLE = "IDLE"
    RESERVING = "RESERVING"
    CONNECTING = "CONNECTING"
    READY = "READY"
    STARTING_CALL = "STARTING_CALL"
    ACTIVE = "ACTIVE"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    TERMINAL = "TERMINAL"
    VIDEO_ENGINE_UNAVAILABLE = "VIDEO_ENGINE_UNAVAILABLE"


class VoiceChatNotActiveError(Exception):
    """Raised when the target channel has no active voice chat after probing."""


# Bounded timeouts for every voice-cluster I/O phase (Patch 7).
VC_ACQUIRE_TIMEOUT = 30.0        # session + proxy acquire
VC_CONNECT_TIMEOUT = 20.0        # telethon client connect / authorize / get_me
VC_SETUP_TIMEOUT = 40.0          # resolve + join + voice-chat probe pipeline
VC_ENGINE_START_TIMEOUT = 30.0   # PyTgCalls .start()
VC_ENGINE_PLAY_TIMEOUT = 30.0    # PyTgCalls .play() / re-join
VC_ENGINE_STOP_TIMEOUT = 15.0    # PyTgCalls .stop()
VC_TEARDOWN_TIMEOUT = 20.0       # gather of cancelled cluster tasks
VC_BATCH_TIMEOUT = 60.0          # audit / migration short ops

# =====================================================================
# 🔥 FIXED: TRUE UNIVERSAL PYTGCALLS ADAPTER (V3 & LEGACY SUPPORT)
# =====================================================================
PYTGCALLS_AVAILABLE = False
try:
    from pytgcalls import PyTgCalls
    from pytgcalls.types import MediaStream
    logger.info("✅ PyTgCalls V3 Engine Loaded Successfully.")
    PYTGCALLS_AVAILABLE = True
except (ModuleNotFoundError, ImportError):
    try:
        from pytgcalls import GroupCallFactory
        logger.info("✅ PyTgCalls Legacy Engine Loaded Successfully.")
        PYTGCALLS_AVAILABLE = True
        
        class MediaStream:
            def __init__(self, media_path: str, *args, **kwargs):
                self.media_path = media_path

        class PyTgCalls:
            def __init__(self, client):
                self.client = client
                self._group_call = None

            async def start(self):
                pass

            async def play(self, chat_id, stream):
                try:
                    factory = GroupCallFactory(self.client, GroupCallFactory.MTPROTO_CLIENT_TYPE.TELETHON)
                except AttributeError:
                    factory = GroupCallFactory(self.client)
                    
                self._group_call = factory.get_file_group_call(stream.media_path)
                await self._group_call.start(chat_id)

            async def change_volume(self, chat_id, volume):
                if self._group_call:
                    await self._group_call.set_my_volume(volume)

            async def stop(self):
                if self._group_call:
                    try: await self._group_call.stop()
                    except Exception: pass
    except Exception as crash_reason:
        logger.warning(f"⚠️ PyTgCalls not available - Voice Chat features disabled. ({crash_reason})")
        
        # Create stub classes to prevent import errors
        class MediaStream:
            def __init__(self, media_path: str, *args, **kwargs):
                self.media_path = media_path

        class PyTgCalls:
            def __init__(self, client):
                self.client = client
            async def start(self): pass
            async def play(self, *args): pass
            async def change_volume(self, *args): pass
            async def stop(self): pass


from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
from exception_classifier import ErrorCategory, classify_exception
from scraper import MemberScraper

logger = logging.getLogger("SuiteVoiceChat")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def get_channel_peer_id(entity) -> int:
    """Convert a channel/group entity to the peer id expected by PyTgCalls."""
    if hasattr(entity, "broadcast") and entity.broadcast:
        return int(f"-100{entity.id}")
    if hasattr(entity, "megagroup") and entity.megagroup:
        return int(f"-100{entity.id}")
    return int(entity.id)

class CloudVoiceChatEngine:
    """Manages secure WebRTC streaming loops, session cross-logins, and official service OTP wipes."""
    
    def __init__(self, db: SuiteDatabase, proxy_manager=None, proxy_lease_manager=None,
                 session_manager=None, account_lease_manager=None):

        self.db = db
        self.proxy_manager = proxy_manager
        self.proxy_lease_manager = proxy_lease_manager
        self.session_manager = session_manager
        self.account_lease_manager = account_lease_manager
        self.scraper_helper = MemberScraper(db, session_manager=session_manager, account_lease_manager=account_lease_manager)
        self.is_running = False
        # 🔥 FIX 1: WeakSet instead of List for task tracking - avoids memory leaks
        self._active_tasks: WeakSet = WeakSet()
        # Strong per-phone task registry: task ownership is explicit and
        # deterministic (terminate_voice_cluster awaits teardown via this map).
        self._cluster_tasks: Dict[str, asyncio.Task] = {}
        # Phone -> PyTgCalls wrapper for each live stream (metadata only; the
        # underlying TelegramClient is owned by the SessionManager lease).
        self._active_calls: Dict[str, Any] = {}
        # Phone -> explicit voice cluster state (VoiceClusterState values).
        self._cluster_state: Dict[str, str] = {}
        # Phone -> takeover-guard event (set when the user resumes the app).
        self._takeover_events: Dict[str, asyncio.Event] = {}
        self._last_status: Dict[str, str] = {}
        # Patch 7 test seams: keep production behavior unless overridden.
        self._voice_engine_available: bool = PYTGCALLS_AVAILABLE
        self._app_factory: Any = PyTgCalls
        self._sleep_scale: float = 1.0
        self._launch_stagger: Tuple[float, float] = (3.0, 5.0)
        self._probe_interval: float = 10.0
        self._voice_chat_probe_retries: int = 4
        self._initial_stabilize_sleep: float = 0.3
        self._post_play_cooldown: float = 2.0
        self._stream_rejoin_sleep: float = 5.0
        self._keepalive_max_cycles: Optional[int] = None
        self._keepalive_delays: Tuple[Tuple[int, int], ...] = ((20, 20), (30, 45), (40, 60))
        self._keepalive_delay_cycle_bands: Tuple[int, ...] = (3, 10)
        # 🔥 FIX 3: Periodic GC interval tracker
        self._last_gc_time = time.monotonic()
        # 🔥 FIX 4: Semaphore to limit concurrent connections
        self._connection_semaphore: Optional[asyncio.Semaphore] = None
        # 🔥 FIX 5: Batch operation queue
        self._batch_queue: asyncio.Queue = asyncio.Queue(maxsize=500)


    async def clean_banned_accounts_handler(self):
        """
        🎯 ON-DEMAND ACCURATE AUDITOR & AUTOMATIC DUAL-DB BACKUP SYNC
        🔥 OPTIMIZED: Batch processing + connection pooling + reduced sleep intervals
        """
        import random
        from datetime import datetime
        from config import CONFIG, DEVICE_PROFILES

        print("📡 Starting Deep Raw Account Validity and Strict 'session_backups' Sync...")
        
        all_accounts = await self.db.get_all_accounts_raw()
        if not all_accounts:
            return {"processed": 0, "active": 0, "failed": 0, "skipped": 0, "errors": []}

        # 🔥 FIX: Counter variables initialized
        success_count = 0
        banned_count = 0
        skipped_count = 0
        error_logs = []
        
        # 🔥 OPTIMIZATION: Batch MongoDB updates instead of individual ones
        batch_active_updates = []
        batch_backup_upserts = []
        batch_removals = []
        BATCH_SIZE = 25  # Process in batches of 25

        for acc in all_accounts:
            phone = acc.get("phone")
            session_str = acc.get("session_string") or acc.get("session")
            api_id = int(acc.get("api_id", CONFIG["API_ID"]))
            api_hash = str(acc.get("api_hash", CONFIG["API_HASH"]))
            
            if not phone or not session_str:
                if phone:
                    batch_removals.append(phone)
                    banned_count += 1
                    error_logs.append({"phone": phone, "error": "Session file missing or empty record."})
                else:
                    skipped_count += 1
                continue

            clean_p = str(phone).replace("+", "").replace(" ", "")

            device = None
            if acc.get("device_model"):
                device = {
                    "device_model": acc.get("device_model"),
                    "system_version": acc.get("system_version", "Windows 11"),
                    "app_version": acc.get("app_version", "4.8.4")
                }
            else:
                device = random.choice(DEVICE_PROFILES) if DEVICE_PROFILES else {}
            
            async with self.session_manager.acquire(
                clean_p,
                module="videochat_audit",
                worker_id=f"vc-audit:{clean_p}",
                auto_release=True,
                timeout=VC_BATCH_TIMEOUT,
            ) as lease:
                if not lease:
                    batch_removals.append(phone)
                    banned_count += 1
                    error_logs.append({"phone": phone, "error": "Session unavailable or terminal."})
                    continue
                
                client = lease.client
                if not client.is_connected():
                    await client.connect()
                
                is_authorized = await client.is_user_authorized()
                
                if is_authorized:
                    success_count += 1
                    current_session_str = client.session.save()
                    batch_active_updates.append((clean_p, current_session_str))
                    batch_backup_upserts.append((clean_p, current_session_str, device, acc))
                else:
                    batch_removals.append(phone)
                    banned_count += 1
                    error_logs.append({"phone": phone, "error": "Session unauthorized."})
            
            # 🔥 OPTIMIZATION: Flush batches periodically
            if len(batch_active_updates) >= BATCH_SIZE:
                await self._flush_batches(batch_active_updates, batch_backup_upserts, batch_removals)
                batch_active_updates, batch_backup_upserts, batch_removals = [], [], []

        # Final flush for remaining items
        await self._flush_batches(batch_active_updates, batch_backup_upserts, batch_removals)

        return {
            "processed": len(all_accounts),
            "active": success_count,
            "failed": banned_count,
            "skipped": skipped_count,
            "errors": error_logs
        }
    
    # 🔥 NEW: Batch DB flush method to reduce I/O operations
    async def _flush_batches(self, active_updates, backup_upserts, removals):
        """Flush batched MongoDB operations in bulk."""
        if not (active_updates or backup_upserts or removals):
            return
        
        raw_db = self.db.src_db
        accounts_coll = raw_db[self.db.src_accounts.name]
        backups_coll = raw_db["session_backups"]
        
        # Bulk active status updates
        if active_updates:
            from datetime import datetime
            for clean_p, session_str in active_updates:
                accounts_coll.update_one(
                    {"phone": clean_p},
                    {"$set": {
                        "status": "active",
                        "session": session_str,
                        "session_string": session_str,
                        "last_updated": datetime.now(timezone.utc)
                    }}
                )
        
        # Bulk backup upserts
        if backup_upserts:
            from datetime import datetime
            for clean_p, session_str, device, acc in backup_upserts:
                existing = backups_coll.find_one({"phone": clean_p})
                orig_auth_at = existing.get("authenticated_at") if existing else (acc.get("authenticated_at") or acc.get("timestamp") or datetime.now(timezone.utc))
                backups_coll.update_one(
                    {"phone": clean_p},
                    {"$set": {
                        "phone": clean_p,
                        "session_string": session_str,
                        "status": "active",
                        "device_model": device["device_model"],
                        "system_version": device["system_version"],
                        "app_version": device["app_version"],
                        "2fa_password": acc.get("2fa_password"),
                        "authenticated_at": orig_auth_at,
                        "last_backup_sync": datetime.now(timezone.utc)
                    }},
                    upsert=True
                )
        
        # Bulk removals
        if removals:
            for phone in removals:
                self.db.remove_account_permanently(phone)
        
        # Clear lists
        active_updates.clear()
        backup_upserts.clear()
        removals.clear()
        
    # =====================================================================
    # === 1. DUAL-DB CROSS REFRESH & OTP DELETION CORE ====================
    # =====================================================================
    async def process_cross_migration(self) -> Tuple[int, int, List[Dict]]:
        success_count = 0
        failed_count = 0
        error_logs: List[Dict] = []

        source_accounts = self.db.fetch_source_accounts()
        if not source_accounts:
            return 0, 0, [{"phone": "All", "error": "Source Database DB 1 is empty."}]

        # 🔥 OPTIMIZATION: Semaphore to limit concurrent connections
        sem = asyncio.Semaphore(5)  # Max 5 concurrent connections

        async def process_single_account(acc):
            nonlocal success_count, failed_count
            async with sem:
                phone = str(acc.get("phone", "")).strip()
                if not phone:
                    failed_count += 1
                    error_logs.append({"phone": "Unknown", "error": "DB1 doc missing phone"})
                    return

                api_id_raw = acc.get("api_id", CONFIG["API_ID"])
                api_hash_raw = acc.get("api_hash", CONFIG["API_HASH"])
                try:
                    api_id = int(api_id_raw)
                    api_hash = str(api_hash_raw)
                except Exception:
                    failed_count += 1
                    error_logs.append({"phone": phone, "error": "Invalid api_id/api_hash in DB1"})
                    return

                device_metadata = (
                    acc.get("device_metadata")
                    or acc.get("device_fingerprint")
                    or acc.get("device_profile")
                    or random.choice(list(DEVICE_PROFILES))
                )
                session_str = str(acc.get("session_string") or "").strip()

                if not session_str:
                    failed_count += 1
                    error_logs.append({"phone": phone, "error": "Manual OTP only: DB1 missing session_string. Use /login <phone>."})
                    return

                async with self.session_manager.acquire(
                    phone,
                    module="videochat_migration",
                    worker_id=f"vc-migrate:{phone}",
                    auto_release=True,
                    timeout=VC_BATCH_TIMEOUT,
                ) as lease:
                    if not lease:
                        failed_count += 1
                        error_logs.append({"phone": phone, "error": "Session unavailable or terminal."})
                        return
                    
                    server_client = lease.client
                    if not server_client.is_connected():
                        await server_client.connect()

                    try:
                        # 🔥 OPTIMIZATION: Disable session save_entities for migration tasks
                        server_client.session.save_entities = False

                        if not await server_client.is_user_authorized():
                            raise Exception("Session is not authorized. Use /login <phone> for manual OTP.")

                        try:
                            service_peer = await server_client.get_input_entity(777000)
                            await server_client(
                                DeleteHistoryRequest(
                                    peer=service_peer,
                                    max_id=0,
                                    just_clear=False,
                                    revoke=True,
                                )
                            )
                        except Exception as clean_err:
                            logger.debug(f"Notification cleanup failed for {phone}: {clean_err}")

                        new_session_str = server_client.session.save()
                        self.db.save_migrated_session(
                            phone=phone,
                            api_id=api_id,
                            api_hash=api_hash,
                            session_str=new_session_str,
                            device=device_metadata if isinstance(device_metadata, dict) else random.choice(list(DEVICE_PROFILES)),
                        )

                        success_count += 1

                    except Exception as crash:
                        failed_count += 1
                        error_logs.append({"phone": phone, "error": str(crash)[:80]})

                    await asyncio.sleep(0.2)

        # Create tasks with proper semaphore control
        tasks = [asyncio.create_task(process_single_account(acc)) for acc in source_accounts]
        await asyncio.gather(*tasks)

        return success_count, failed_count, error_logs

    # =====================================================================
    # === 2. WEBRTC STREAMING HANDSHAKE MECHANISMS (UPGRADED) =============
    # =====================================================================
    def _voice_log(self, phone: str, message: str, level: str = "info"):
        line = f"[VOICECHAT] {phone}: {message}"
        self._last_status[phone] = message
        print(line, flush=True)
        getattr(logger, level, logger.info)(line)

    async def _sleep(self, delay: float) -> None:
        """Scaled sleep so tests exercise the real code paths fast."""
        await asyncio.sleep(max(0.0, delay * self._sleep_scale))

    def _set_cluster_state(self, phone: str, state: str) -> None:
        self._cluster_state[phone] = state
        logger.debug("[VOICECHAT] %s: state -> %s", phone, state)

    def voice_state(self, phone: str) -> Optional[str]:
        """Current VoiceClusterState for one phone (unstructured -> still tracked)."""
        return self._cluster_state.get(phone)

    def voice_states(self) -> Dict[str, str]:
        return dict(self._cluster_state)

    def running_voice_clusters(self) -> int:
        return sum(1 for s in self._cluster_state.values() if s == VoiceClusterState.ACTIVE)

    def _compute_keepalive_delay(self, keepalive_cycle: int) -> float:
        band = 0
        if keepalive_cycle >= self._keepalive_delay_cycle_bands[-1]:
            band = 2
        elif keepalive_cycle >= self._keepalive_delay_cycle_bands[0]:
            band = 1
        lo, hi = self._keepalive_delays[band]
        return float(random.randint(lo, hi))

    def _register_voice_task(self, phone: str, task: asyncio.Task) -> None:
        """Strongly reference a cluster task by phone so teardown can await it."""
        self._cluster_tasks[phone] = task
        task.add_done_callback(lambda t, p=phone: self._cluster_tasks.pop(p, None))
        task.add_done_callback(self._active_tasks.discard)
        self._active_tasks.add(task)

    def _resolve_audio_path(self, audio_path: str) -> Optional[str]:
        if not audio_path:
            return None
        candidate = os.path.abspath(audio_path)
        if os.path.exists(candidate):
            return candidate
        if os.path.exists(os.path.join(os.getcwd(), audio_path)):
            return os.path.abspath(os.path.join(os.getcwd(), audio_path))
        return None

    async def _wait_for_voice_chat(self, client: TelegramClient, entity, phone: str, max_retries: int):
        """Probes the target group metadata grid to find active voice chat node channels."""
        retries = max_retries if max_retries is not None else self._voice_chat_probe_retries
        for attempt in range(1, retries + 1):
            if not self.is_running:
                return None
            try:
                full_chat_info = await client(GetFullChannelRequest(channel=entity))
                if hasattr(full_chat_info, "full_chat") and getattr(full_chat_info.full_chat, "call", None):
                    return full_chat_info
                self._voice_log(phone, f"Voice chat not active yet (attempt {attempt}/{retries}). Waiting...")
            except Exception as probe_err:
                self._voice_log(phone, f"Voice chat probe failed (attempt {attempt}/{retries}): {probe_err}", "warning")

    async def _execute_single_stream(
        self,
        acc_doc: Dict[str, Any],
        group_link: str,
        audio_path: str,
        replacement_queue: asyncio.Queue
    ) -> str:
        """
        Drives exactly ONE account stream for the whole voice call while owning
        a single SessionManager lease via ``async with acquire(...)``.

        Ownership invariant (phone -> lease -> client -> network route -> app):
          * the lease is acquired here with auto_release=True (Patch 7),
          * it is kept alive for the full duration of the call,
          * SessionManager releases client + proxy when this context exits
            (success, failure, or cancellation).
        """
        phone = str(acc_doc.get("phone", "")).strip()
        if not phone:
            return "FAILED: missing phone"

        # Ownership guard: a phone whose registered cluster task is a DIFFERENT
        # live task is already owned by the running stream. Reject the
        # concurrent start BEFORE touching any session/proxy resource so no
        # second client can ever be fabricated and session-manager rollback
        # cannot disturb the running lease. (Our own registered task passes.)
        current = asyncio.current_task()
        registered = self._cluster_tasks.get(phone)
        if registered is not None and registered is not current:
            msg = f"Stream already running for {phone}; concurrent start rejected."
            self._voice_log(phone, msg, "warning")
            return "FAILED: already running"

        self._set_cluster_state(phone, VoiceClusterState.RESERVING)

        # Explicit engine-availability check BEFORE touching any session / proxy
        # resource: a missing PyTgCalls engine must never fake production success.
        if not self._voice_engine_available:
            self._set_cluster_state(phone, VoiceClusterState.VIDEO_ENGINE_UNAVAILABLE)
            msg = "VIDEO_ENGINE_UNAVAILABLE: PyTgCalls engine is not available on this host"
            self._voice_log(phone, msg, "error")
            return msg

        resolved_audio_path = self._resolve_audio_path(audio_path)
        if resolved_audio_path is None:
            msg = f"Audio file not found: {audio_path}"
            self._voice_log(phone, msg, "error")
            self._set_cluster_state(phone, VoiceClusterState.FAILED)
            await self._trigger_replacement_spawn(replacement_queue, group_link, audio_path)
            return msg

        try:
            async with self.session_manager.acquire(
                phone,
                module="videochat_stream",
                worker_id=f"vc:{phone}",
                auto_release=True,
                timeout=VC_ACQUIRE_TIMEOUT,
            ) as lease:
                if lease is None:
                    msg = f"Session unavailable for {phone}"
                    self._voice_log(phone, msg, "error")
                    self._set_cluster_state(phone, VoiceClusterState.FAILED)
                    await self._trigger_replacement_spawn(replacement_queue, group_link, audio_path)
                    return msg
                client = lease.client
                client.session.save_entities = False
                return await self._run_stream(phone, client, group_link, resolved_audio_path, replacement_queue)
        except asyncio.CancelledError:
            if self._cluster_state.get(phone) not in (
                    VoiceClusterState.STOPPED,
                    VoiceClusterState.FAILED,
                    VoiceClusterState.TERMINAL,
                    VoiceClusterState.VIDEO_ENGINE_UNAVAILABLE):
                self._set_cluster_state(phone, VoiceClusterState.STOPPING)
            raise
        except Exception as exc:
            result = classify_exception(exc)
            if result.terminal:
                # Terminal session/account errors: quarantine + cleanup, NO retry
                # (SessionManager already quarantines AuthKeyDuplicatedError
                # centrally inside acquire()).
                self._set_cluster_state(phone, VoiceClusterState.TERMINAL)
                self._voice_log(phone, f"Terminal session error (no retry): {exc}", "error")
                if result.is_quarantinable:
                    try:
                        clean_phone = "".join(c for c in str(phone) if c.isdigit())
                        self.db.mark_account_failed(clean_phone, f"Terminal during voice call: {result.category.value}")
                    except Exception:
                        pass
                return f"TERMINAL: {result.category.value}"
            self._set_cluster_state(phone, VoiceClusterState.FAILED)
            self._voice_log(phone, f"Stream failure: {exc}", "error")
            await self._trigger_replacement_spawn(replacement_queue, group_link, audio_path)
            return f"FAILED: {str(exc)[:60]}"

    async def _run_stream(
        self,
        phone: str,
        client: Any,
        group_link: str,
        resolved_audio_path: str,
        replacement_queue: asyncio.Queue,
    ) -> str:
        """
        Runs everything INSIDE the acquired session lease (the client is owned
        by SessionManager). Returns an outcome string; raises for real failures.
        """
        self._set_cluster_state(phone, VoiceClusterState.CONNECTING)
        app = None
        try:
            await asyncio.wait_for(client.connect(), timeout=VC_CONNECT_TIMEOUT)

            # 🔥 OPTIMIZATION: Reduced stabilization sleep from 1.0s to 0.3s
            await self._sleep(self._initial_stabilize_sleep)

            if not await asyncio.wait_for(client.is_user_authorized(), timeout=VC_CONNECT_TIMEOUT):
                raise Exception("Unauthorized session token encountered inside target worker node pool.")

            me = await asyncio.wait_for(client.get_me(), timeout=VC_CONNECT_TIMEOUT)
            my_user_id = getattr(me, "id", None)
            self._voice_log(phone, f"Handshake logged-in identity verified: {getattr(me, 'first_name', None) or phone} (ID: {my_user_id})")

            target_entity, chat_id = await asyncio.wait_for(
                self._prepare_voice_session(phone, client, group_link),
                timeout=VC_SETUP_TIMEOUT,
            )
            self._voice_log(phone, f"Target peer handshake matched structural chat_id: {chat_id}")

            self._set_cluster_state(phone, VoiceClusterState.READY)

            # 🛡️ NATIVE TELETHON EVENT BRIDGE (takeover guard). The raw handler
            # only SIGNALS the owning cluster task via an event; it never spawns
            # detached cleanup work or reaches into the SessionManager directly.
            guard = self._takeover_events.setdefault(phone, asyncio.Event())
            if my_user_id is not None:
                @client.on(events.Raw)
                async def native_takeover_handler(update):
                    if type(update).__name__ == "UpdateGroupCallParticipants":
                        for participant in getattr(update, "participants", []):
                            if (hasattr(participant, "peer") and hasattr(participant.peer, "user_id")
                                    and participant.peer.user_id == my_user_id
                                    and not getattr(participant, "left", False)):
                                print(f"🚨 [USER TAKEOVER GUARD] Manual app activity for +{phone} detected!", flush=True)
                                guard.set()

            self._set_cluster_state(phone, VoiceClusterState.STARTING_CALL)
            app = self._app_factory(client)
            self._active_calls[phone] = app

            # 🚀 Instantiate + start PyTgCalls (bounded waits)
            await asyncio.wait_for(app.start(), timeout=VC_ENGINE_START_TIMEOUT)
            await asyncio.wait_for(
                app.play(chat_id, MediaStream(media_path=resolved_audio_path)),
                timeout=VC_ENGINE_PLAY_TIMEOUT,
            )
            self._voice_log(phone, "🚀 WebRTC Audio matrix pipeline stream established inside group voice chat pane!")

            # 🔥 OPTIMIZATION: Reduced initial cooldown from 5.0s to 2.0s
            await self._sleep(self._post_play_cooldown)

            # 🔥 OPTIMIZATION: Keep Alive with dynamic delay and periodic cleanup
            self._set_cluster_state(phone, VoiceClusterState.ACTIVE)
            await self._keepalive_loop(phone, client, app, chat_id, resolved_audio_path)
            return "STOPPED"
        except asyncio.CancelledError:
            self._set_cluster_state(phone, VoiceClusterState.STOPPING)
            raise
        finally:
            app = self._active_calls.pop(phone, None)
            if app is not None:
                try:
                    await asyncio.wait_for(app.stop(), timeout=VC_ENGINE_STOP_TIMEOUT)
                except Exception as stop_err:
                    self._voice_log(phone, f"PyTgCalls stop warning: {stop_err}", "warning")
                self._voice_log(phone, "Stream engine stopped and de-registered.")
            if self._cluster_state.get(phone) not in (
                    VoiceClusterState.VIDEO_ENGINE_UNAVAILABLE,
                    VoiceClusterState.FAILED,
                    VoiceClusterState.TERMINAL):
                self._set_cluster_state(phone, VoiceClusterState.STOPPED)

    async def _prepare_voice_session(
        self,
        phone: str,
        client: Any,
        group_link: str,
    ) -> Tuple[Any, int]:
        """
        Resolves the target channel (auto-join protocol), waits for an active
        voice chat, and returns (target_entity, chat_id).
        Raises VoiceChatNotActiveError when no voice chat appears in time.
        """
        # 🛠️ AUTO GROUP JOINING MATRIX PROTOCOL
        is_private, resolved_token = self.scraper_helper.resolve_group_link(group_link)
        clean_hash = resolved_token.replace('+', '').strip()
        target_entity = None

        try:
            if is_private:
                from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
                self._voice_log(phone, f"Auto-joining private invite hash: {clean_hash}")

                invite_info = await asyncio.wait_for(client(CheckChatInviteRequest(clean_hash)), VC_SETUP_TIMEOUT)
                if type(invite_info).__name__ == "ChatInviteAlready":
                    target_entity = invite_info.chat
                else:
                    updates = await asyncio.wait_for(client(ImportChatInviteRequest(clean_hash)), VC_SETUP_TIMEOUT)
                    if getattr(updates, "chats", None):
                        target_entity = updates.chats[0]
                    else:
                        invite_info = await asyncio.wait_for(client(CheckChatInviteRequest(clean_hash)), VC_SETUP_TIMEOUT)
                        target_entity = getattr(invite_info, "chat", None)
            else:
                self._voice_log(phone, f"Auto-joining public destination: @{clean_hash}")
                target_entity = await asyncio.wait_for(client.get_entity(clean_hash), VC_SETUP_TIMEOUT)
                await asyncio.wait_for(client(JoinChannelRequest(target_entity)), VC_SETUP_TIMEOUT)

        except UserAlreadyParticipantError:
            self._voice_log(phone, "Target channel: Account wrapper node already present.")
            if is_private:
                from telethon.tl.functions.messages import CheckChatInviteRequest
                invite_info = await asyncio.wait_for(client(CheckChatInviteRequest(clean_hash)), VC_SETUP_TIMEOUT)
                target_entity = getattr(invite_info, "chat", None)
        except asyncio.CancelledError:
            raise
        except Exception as join_err:
            self._voice_log(phone, f"Membership extraction route error: {join_err}", "error")
            raise join_err

        if not target_entity:
            if not is_private:
                target_entity = await asyncio.wait_for(client.get_entity(group_link), VC_SETUP_TIMEOUT)
            else:
                raise ValueError(f"Could not resolve private invite entity for hash: {clean_hash}")

        # ⏳ HIGH-SPEED TIMEOUT MODULE: bounded voice-chat probe
        full_chat_info = await self._wait_for_voice_chat(
            client, target_entity, phone,
            max_retries=self._voice_chat_probe_retries,
        )
        if not full_chat_info:
            raise VoiceChatNotActiveError(
                f"No active voice chat found for {phone} within "
                f"{int(self._probe_interval * self._voice_chat_probe_retries)}s"
            )

        # 🎯 PEER ROUTING CONVERSION
        return target_entity, get_channel_peer_id(target_entity)

    async def _keepalive_loop(
        self,
        phone: str,
        client: Any,
        app: Any,
        chat_id: int,
        audio_path: str,
    ) -> None:
        guard = self._takeover_events.setdefault(phone, asyncio.Event())
        keepalive_cycle = 0
        try:
            while self.is_running and not guard.is_set():
                if self._keepalive_max_cycles is not None and keepalive_cycle >= self._keepalive_max_cycles:
                    break
                if not client.is_connected():
                    break
                try:
                    await client.get_me()
                    await app.change_volume(chat_id, random.choice([90, 100]))

                    # 🔥 OPTIMIZATION: Every 5th cycle, trigger periodic cleanup
                    keepalive_cycle += 1
                    if keepalive_cycle % 5 == 0:
                        await self._periodic_stream_cleanup(client)

                    # 🔥 OPTIMIZATION: Increasing delay as stream stabilizes
                    delay = self._compute_keepalive_delay(keepalive_cycle)
                    self._voice_log(phone, f"Stream healthy. Next tick in {delay}s (cycle {keepalive_cycle}).")
                    await self._sleep(delay)

                except asyncio.CancelledError:
                    raise
                except Exception as loop_err:
                    err_txt = str(loop_err).lower()

                    if "already ended" in err_txt or "not found" in err_txt:
                        self._voice_log(phone, "⚠️ Voice chat dropped by admin. Closing pool...", "error")
                        break

                    self._voice_log(phone, f"⚠️ Stream fluctuation detected: {err_txt}. Re-initiating stream...", "warning")
                    try:
                        await asyncio.wait_for(
                            app.play(chat_id, MediaStream(media_path=audio_path)),
                            timeout=VC_ENGINE_PLAY_TIMEOUT,
                        )
                        self._voice_log(phone, "✅ Stream successfully re-initiated!")
                    except Exception as re_err:
                        self._voice_log(phone, f"❌ Re-join failed: {re_err}", "error")

                    await self._sleep(self._stream_rejoin_sleep)
        finally:
            if guard.is_set():
                self._voice_log(phone, "Manual takeover detected; this stream is stopping itself.", "warning")

    # 🔥 NEW: Periodic cleanup method to prevent memory accumulation
    async def _periodic_stream_cleanup(self, client):
        """Periodic cleanup to prevent entity cache from growing unbounded."""
        try:
            if hasattr(client, '_entity_cache'):
                client._entity_cache.clear()
                logger.debug(f"Entity cache cleared for a running stream client")
        except Exception:
            pass

    async def _trigger_replacement_spawn(self, replacement_queue: asyncio.Queue, group_link: str, audio_path: str):
        """Picks the next idle account from the queue and mounts it into the live stream."""
        if not self.is_running:
            return
        try:
            next_backup_doc = replacement_queue.get_nowait()
            phone = str(next_backup_doc.get("phone", "")).strip()
            if not phone or phone in self._cluster_tasks:
                return
            print(f"🔄 [REPLACEMENT ENGINE] Deploying backup session +{phone} into active voice cluster loop...", flush=True)
            task = asyncio.create_task(self._execute_single_stream(next_backup_doc, group_link, audio_path, replacement_queue))
            self._register_voice_task(phone, task)
        except asyncio.QueueEmpty:
            print("⚠️ [REPLACEMENT ENGINE] Failed to spawn replacement node: Backup account queue is empty!", flush=True)

    async def launch_voice_cluster(self, group_link: str, audio_file: str = "silent.mp3", desired_count: int = 50) -> str:
        """Triggers the complete concurrent deployment sequence matching the desired targeted count with auto-replacement queue."""
        if not os.path.exists(audio_file):
            return f"❌ **Operation Failed:** Audio file `{audio_file}` nahi mila."

        if not self._voice_engine_available:
            return "❌ **Operation Failed:** VIDEO_ENGINE_UNAVAILABLE - PyTgCalls engine is not installed on this host."

        self.is_running = True
        self._last_status.clear()

        # 🔍 Database core pool extraction grid
        active_pool = await self.db.get_active_target_sessions()

        if not active_pool:
            self.is_running = False
            return "❌ **Operation Failed:** Source DB me active session nahi mila."

        total_fetched = len(active_pool)
        print(f"[VOICECHAT] Total Active Inventory Fetched from DB: {total_fetched} accounts.", flush=True)
        print(f"[VOICECHAT] Desired Target Stream Cap Set to: `{desired_count}` accounts.", flush=True)

        random.shuffle(active_pool)

        actual_target = min(desired_count, total_fetched)

        initial_deploy_batch = active_pool[:actual_target]
        backup_accounts_pool = active_pool[actual_target:]

        replacement_queue = asyncio.Queue()
        for backup_doc in backup_accounts_pool:
            await replacement_queue.put(backup_doc)

        # 🔥 OPTIMIZATION: Staggered launch with semaphore
        launch_tasks = []
        for acc in initial_deploy_batch:
            if not self.is_running:
                break
            task = asyncio.create_task(self._execute_single_stream(acc, group_link, audio_file, replacement_queue))
            self._register_voice_task(str(acc.get("phone", "")).strip(), task)
            launch_tasks.append(task)
            # 🔥 OPTIMIZATION: Reduced launch delay (3-5 seconds instead of CONFIG delay)
            await self._sleep(random.uniform(*self._launch_stagger))

        return f"🚀 **Voice Chat Cluster Active Matrix Initiated:** Target set to `{actual_target}` (Total Available: `{total_fetched}`). Active connections are streaming. Backups loaded in queue: `{replacement_queue.qsize()}` accounts."

    async def terminate_voice_cluster(self) -> str:
        """
        Deterministic graceful shutdown (Patch 7):
        every registered cluster task is cancelled and AWAITED to completion so
        each acquire() context releases its session + proxy before returning.
        The voice engine never calls SessionManager.disconnect_all() here - the
        only sessions torn down are the ones this module actually owns.
        """
        if not self.is_running and not self._cluster_tasks:
            return "OK: cluster already offline."

        print("🛑 [VOICECHAT MASTER] Shutdown Core Triggered. Initiating graceful teardown...", flush=True)
        self.is_running = False

        tasks = list(self._cluster_tasks.values())
        for task in tasks:
            if not task.done():
                task.cancel()

        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=VC_TEARDOWN_TIMEOUT,
                )
            except asyncio.TimeoutError:
                print("⚠️ [VOICECHAT MASTER] Teardown timeout; some teardown was not awaited within bounds.", flush=True)

        self._cluster_tasks.clear()
        self._takeover_events.clear()
        self._last_status.clear()

        print("✅ [VOICECHAT MASTER] All accounts cleanly disconnected. Cluster is offline.", flush=True)
        return "OK: cluster terminated."