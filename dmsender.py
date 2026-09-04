#!/usr/bin/env python3
"""
Ultimate Enterprise Telegram Suite - DM Sender Engine (Database Integrated)
Filename: dmsender.py

🔥 NEW: Proxy-Driven Dynamic Rolling Batch Architecture
"""

import os
import asyncio
import logging
import random
from datetime import datetime
from typing import Dict, Any, Optional

from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio, InputPeerUser
from telethon.errors import (
    PeerIdInvalidError, FloodWaitError, UserBannedInChannelError,
    UserDeactivatedError, AuthKeyUnregisteredError, SessionRevokedError,
    UserIsBlockedError, UserPrivacyRestrictedError, PeerFloodError,
    AuthKeyDuplicatedError,
)
from pymongo import MongoClient

from config import CONFIG, DEVICE_PROFILES
from exception_classifier import ErrorCategory, classify_exception
from session_manager import SessionAlreadyOwnedError

logger = logging.getLogger("DMSenderEngine")

# Priority Override: ADMIN_ID mapped to environment variable as per system rules
ADMIN_ID = os.environ.get("ADMIN_ID")


def compute_dm_worker_capacity(
    num_accounts: int,
    available_proxies: int,
    session_capacity: int,
    configured_limit: int,
) -> int:
    """Phase 13 resource-aware worker capacity.

    effective_capacity = min(eligible_accounts, available_network_capacity,
                             available_session_capacity, configured_limit).

    At least 1 worker is returned when accounts exist so the engine blocks on
    proxy/session acquisition (WAITING_FOR_PROXY / WAITING_FOR_ACCOUNT) instead
    of silently dropping valid work. Returns 0 when no accounts are eligible.
    """
    if num_accounts <= 0:
        return 0
    return max(
        1,
        min(num_accounts, available_proxies, session_capacity, configured_limit),
    )


class EnterpriseDMSender:
    def __init__(self, db, proxy_lease_manager=None, session_manager=None, account_lease_manager=None):
        self.db = db
        self.proxy_lease_manager = proxy_lease_manager  # 🔥 NEW: Lease manager integration
        self.session_manager = session_manager
        self.account_lease_manager = account_lease_manager
        self.is_running = False
        self.active_task = None
        self.wizard_state: Dict[int, Dict[str, Any]] = {}
        self.stats = {
            "total_sent": 0,
            "failed": 0,
            "accounts_used": 0,
            "accounts_down": 0,
            "total_targets": 0
        }

        # P0-CLOSEOUT: DM lifecycle instrumentation
        self.lifecycle_events: list = []        # structured event log (capped)
        self.max_lifecycle_events = 500
        self.worker_states: Dict[int, str] = {}  # worker_id -> current waiting/state label
        self._dm_stall_timeout = 60.0           # worker must never wait silently longer than this

    def _emit(self, event: str, worker: Optional[int] = None,
              phone: Optional[str] = None, detail: Optional[str] = None) -> None:
        """Append a structured DM lifecycle event (capped)."""
        self.lifecycle_events.append({
            "ts": datetime.utcnow().isoformat() + "Z",
            "event": event,
            "worker": worker,
            "phone": phone,
            "detail": detail,
        })
        if len(self.lifecycle_events) > self.max_lifecycle_events:
            self.lifecycle_events = self.lifecycle_events[-self.max_lifecycle_events:]
        logger.debug(f"DM_EVENT | {event} | worker={worker} | phone={phone} | {detail}")

    def get_lifecycle_events(self) -> list:
        """Return a copy of the structured event log for tests/status."""
        return list(self.lifecycle_events)

    async def _generate_live_status(self) -> str:
        """LIVE STATUS generated for reporters/UI. Previously MISSING — root cause
        of the 'DM Engine Started' then silent-stop bug."""
        import time
        elapsed = max(1, int(time.time() - getattr(self, '_campaign_start_time', time.time())))
        elapsed_min = elapsed / 60
        rate = round(self.stats['total_sent'] / elapsed_min, 1) if elapsed_min > 0.1 else 0

        remaining = max(0, self.stats['total_targets'] - self.stats['total_sent'] - self.stats['failed'])
        if rate > 0:
            eta_min = round(remaining / rate)
            eta_str = f"{eta_min // 60}h {(eta_min % 60)}m" if eta_min > 60 else f"{eta_min}m"
        else:
            eta_str = "Calculating..."

        active_workers = sum(1 for t in getattr(self, '_active_workers', []) if not t.done())

        proxy_stats = ""
        if self.proxy_lease_manager:
            try:
                stats = self.proxy_lease_manager.get_stats()
                proxy_stats = (
                    f"🛡️ **PROXY POOL**\n"
                    f"   🟢 Available: `{stats['available_proxies']}`\n"
                    f"   🔒 Leased: `{stats['current_active_leases']}`\n"
                    f"   🧊 In Cooldown: `{stats['proxies_in_cooldown']}`\n"
                )
            except Exception:
                proxy_stats = "🛡️ **PROXY POOL** (unavailable)\n"

        stalled = sum(1 for s in self.worker_states.values()
                      if s in ("WAITING_FOR_PROXY", "WAITING_FOR_ACCOUNT", "SESSION_BUSY"))
        state_line = ""
        if self.worker_states:
            grp = {}
            for s in self.worker_states.values():
                grp[s] = grp.get(s, 0) + 1
            state_line = " | ".join(f"{k}={v}" for k, v in grp.items())

        return (
            f"📊 **LIVE DM CAMPAIGN DASHBOARD**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📨 Sent: `{self.stats['total_sent']}` | Failed: `{self.stats['failed']}`\n"
            f"🎯 Targets: `{self.stats['total_targets']}`\n"
            f"   Progress: `{round((self.stats['total_sent'] + self.stats['failed']) / max(1, self.stats['total_targets']) * 100, 1)}%`\n"
            f"⚡ Rate: `{rate} msgs/min` | Runtime: `{elapsed // 60}m {elapsed % 60}s` | ETA: `{eta_str}`\n"
            f"👷 Active Workers: `{active_workers}` | Stalled: `{stalled}`\n"
            f"👥 Accounts: used=`{self.stats['accounts_used']}` down=`{self.stats['accounts_down']}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{proxy_stats}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔄 Worker States: `{state_line or 'idle'}`\n"
            f"🔄 *Auto-updates every 8s • `/dm_status` for manual check*"
        )
        # PATCH FIX: Remove hardcoded DB names and directly use initialized DB layer references
        try:
            # self.db.scraped_members pehle hi database.py me single DB 1 par mapped hai
            print("🎯 [DM Engine] Database Connected with Single-DB Unified Mapping.")
        except Exception as e:
            logger.error(f"❌ Fatal Mapping Fault in Database Router: {e}")

    @staticmethod
    async def _force_cleanup_client(client: Optional[TelegramClient]) -> None:
        """
        🔥 ROBUST CLIENT CLEANUP (prevents ghost tasks & Future exception spam)
        Deeply terminates Telethon client, cancelling internal sender loops and closing raw sockets.
        """
        if not client:
            return
        try:
            sender = getattr(client, '_sender', None)
            if sender:
                sender._connecting = False
                
                # Cancel MTProtoSender loops
                for loop_name in ['_recv_loop', '_send_loop', '_ping_loop']:
                    task = getattr(sender, loop_name, None)
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task  # Explicitly retrieve exception to silence event loop
                        except (asyncio.CancelledError, Exception):
                            pass
                
                # Cancel Connection loops (stops "Task was destroyed" spam)
                connection = getattr(sender, '_connection', None)
                if connection:
                    for loop_name in ['_recv_loop', '_send_loop', '_ping_loop']:
                        task = getattr(connection, loop_name, None)
                        if task and not task.done():
                            task.cancel()
                            try:
                                await task
                            except (asyncio.CancelledError, Exception):
                                pass
                
                # Force close raw transport/socket
                transport = getattr(sender, '_transport', None)
                if transport:
                    try:
                        await asyncio.wait_for(transport.close(), timeout=1.0)
                    except Exception:
                        pass
            
            if client.is_connected():
                await asyncio.wait_for(client.disconnect(), timeout=2.0)
        except Exception:
            pass

    def reset_stats(self):
        self.stats = {
            "total_sent": 0,
            "failed": 0,
            "accounts_used": 0,
            "accounts_down": 0,
            "total_targets": 0
        }

    def halt_campaign(self):
        """Stops the DM campaign immediately."""
        self.is_running = False
        if self.active_task:
            self.active_task.cancel()

    async def execute_dm_campaign(self, target_list: list, message_text: str, media_path: str, limit: int, ui_callback):
        """
        🔥 Proxy-Driven Dynamic Rolling Batch DM Campaign Engine
        
        Golden Rule: If an account hits a ban/limit, its proxy goes to cooldown,
        and the worker immediately picks the NEXT available account to complete
        the SAME target. Process never halts waiting for other accounts.
        
        Parallel Execution: If N proxies are available, N workers run in parallel.
        If a proxy dies, worker instantly swaps to next available proxy.
        """
        self.is_running = True
        self.reset_stats()
        import time as _t
        self._campaign_start_time = _t.time()
        self._emit("campaign_start", detail=f"targets={len(target_list) if limit > 0 else len(target_list)}")

        # Clean safe text representation
        final_text = str(message_text).strip() if message_text else ""
        if final_text.lower() == "skip" or not final_text:
            final_text = None

        if not final_text and (not media_path or not os.path.exists(str(media_path))):
            await ui_callback("❌ **Campaign Aborted:** Both Text and Media payload cannot be empty. Setup aborted.")
            self.is_running = False
            return

        all_accounts = await self.db.get_active_target_sessions()
        if not all_accounts:
            await ui_callback("❌ **Campaign Aborted:** Koi active verified session nahi mila.")
            self.is_running = False
            return


        return await self._dynamic_rolling_worker(
            target_list, final_text, media_path, limit, ui_callback, all_accounts
        )

    def _generate_detailed_status(self) -> str:
        """
        🔥 COMPREHENSIVE LIVE STATUS - Shows exactly what's happening
        """
        import time
        
        # Calculate runtime & rate
        elapsed = max(1, int(time.time() - getattr(self, '_campaign_start_time', time.time())))
        elapsed_min = elapsed / 60
        rate = round(self.stats['total_sent'] / elapsed_min, 1) if elapsed_min > 0.1 else 0
        
        # ETA calculation
        remaining = max(0, self.stats['total_targets'] - self.stats['total_sent'] - self.stats['failed'])
        if rate > 0:
            eta_min = round(remaining / rate)
            eta_str = f"{eta_min // 60}h {(eta_min % 60)}m" if eta_min > 60 else f"{eta_min}m"
        else:
            eta_str = "Calculating..."
        
        # Proxy stats (if lease manager available)
        proxy_stats = ""
        if self.proxy_lease_manager:
            stats = self.proxy_lease_manager.get_stats()
            proxy_stats = (
                f"🛡️ **PROXY POOL**\n"
                f"   🟢 Available: `{stats['available_proxies']}`\n"
                f"   🔒 Leased: `{stats['current_active_leases']}`\n"
                f"   🧊 In Cooldown: `{stats['proxies_in_cooldown']}`\n"
                f"   📊 Total Acquires: `{stats['total_acquires']}`\n"
            )
        
        # Worker activity indicator
        active_workers = sum(1 for t in getattr(self, '_active_workers', []) if not t.done())
        
        # Status emoji based on health
        if self.stats['accounts_down'] > self.stats['accounts_used'] * 0.5:
            health_icon = "🔴"
            health_text = "CRITICAL - Many accounts down"
        elif self.stats['accounts_down'] > self.stats['accounts_used'] * 0.2:
            health_icon = "🟠"
            health_text = "WARNING - Accounts dropping"
        elif rate > 0:
            health_icon = "🟢"
            health_text = "HEALTHY - Sending normally"
        else:
            health_icon = "🟡"
            health_text = "WAITING - No proxies available"
        
        return (
            f"📊 **LIVE DM CAMPAIGN DASHBOARD**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{health_icon} **System Health:** {health_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📨 **MESSAGING METRICS**\n"
            f"   ✅ Sent: `{self.stats['total_sent']}`\n"
            f"   ❌ Failed: `{self.stats['failed']}`\n"
            f"   🎯 Total Targets: `{self.stats['total_targets']}`\n"
            f"   📈 Progress: `{round((self.stats['total_sent'] + self.stats['failed']) / max(1, self.stats['total_targets']) * 100, 1)}%`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ **PERFORMANCE**\n"
            f"   🚀 Rate: `{rate} msgs/min`\n"
            f"   ⏱️ Runtime: `{elapsed // 60}m {elapsed % 60}s`\n"
            f"   🕒 ETA: `{eta_str}`\n"
            f"   👷 Active Workers: `{active_workers}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 **ACCOUNT POOL**\n"
            f"   🟢 Total Accounts: `{self.stats['accounts_used']}`\n"
            f"   💀 Banned/Dropped: `{self.stats['accounts_down']}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{proxy_stats}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔄 *Auto-updates every 8s • `/dm_status` for manual check*"
        )    

    async def _dynamic_rolling_worker(
        self, target_list: list, final_text: Optional[str], media_path: str,
        limit: int, ui_callback, all_accounts: list
    ):
        """
        🔥 Dynamic Rolling Batch Worker using ProxyLeaseManager
        
        Each worker:
        1. Picks a target from queue
        2. Acquires proxy lease (blocks efficiently if none available)
        3. Connects account with leased proxy
        4. Executes DM action
        5. Releases proxy (with cooldown if FloodWait/PeerFlood/Ban occurred)
        6. Immediately picks next target - never waits for other accounts
        """
        targets = target_list[:limit] if limit > 0 else target_list
        self.stats["total_targets"] = len(targets)
        self.stats["accounts_used"] = len(all_accounts)
        self._emit("target_queue_created", detail=f"targets={len(targets)}")
        
        await ui_callback(f"🚀 **DM Engine Started (Dynamic Rolling Batch)!**\n"
                         f"Targets: `{len(targets)}`, Accounts: `{len(all_accounts)}`\n"
                         f"Concurrency dictated by available proxies.")

        target_queue = asyncio.Queue()
        for t in targets:
            await target_queue.put(t)

        last_ui_update = datetime.now()
        active_workers = []
        self._active_workers = active_workers
        # 🔥 FIX: Round-robin account index prevents the stuck DM engine
        # Old code: account_queue drained to empty → workers re-queued targets with no accounts
        # New code: _account_rr_idx cycles through all_accounts indefinitely
        _account_rr_lock = asyncio.Lock()
        _account_rr_idx = 0
    
        # 🔥 NEW: Independent Reporter Task for Live UI Updates
        async def _reporter():
            while self.is_running and active_workers:
                if any(not w.done() for w in active_workers):
                    try:
                        await ui_callback(await self._generate_live_status())
                    except Exception:
                        pass
                await asyncio.sleep(8)
    
        reporter_task = asyncio.create_task(_reporter())
        _campaign_start_loop = asyncio.get_event_loop().time()

        
        async def dm_worker(worker_id: int):
            nonlocal last_ui_update
            current_client = None
            current_phone = None
            current_proxy_url = None
            consecutive_failures = 0
            stall_count = 0
            self.worker_states[worker_id] = "STARTED"
            
            try:
                while self.is_running and not target_queue.empty():
                    # Step 1: Get next target
                    try:
                        target_data = target_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    # Step 2: Get next account via round-robin (cycles indefinitely)
                    async with _account_rr_lock:
                        nonlocal _account_rr_idx
                        if not all_accounts:
                            await target_queue.put(target_data)
                            self.worker_states[worker_id] = "WAITING_FOR_ACCOUNT"
                            self._emit("WAITING_FOR_ACCOUNT", worker=worker_id, detail="no accounts available")
                            break
                        account_doc = all_accounts[_account_rr_idx % len(all_accounts)]
                        _account_rr_idx = (_account_rr_idx + 1) % len(all_accounts)

                    phone = account_doc.get("phone")
                    if not phone:
                        continue

                    clean_phone = str(phone).replace("+", "")
                    self._emit("account_selected", worker=worker_id, phone=clean_phone)
                    self.worker_states[worker_id] = "ACCOUNT_SELECTED"

                    # Step 3: Acquire session + proxy via SessionManager
                    # This ensures ONE SESSION → ONE CLIENT (prevents AuthKeyDuplicatedError)
                    # and handles proxy leasing automatically.
                    try:
                        self._emit("session_acquire_start", worker=worker_id, phone=clean_phone)
                        self._emit("proxy_acquire_start", worker=worker_id, phone=clean_phone)
                        self.worker_states[worker_id] = "WAITING_FOR_ACCOUNT"
                        try:
                            session_ctx = self.session_manager.acquire(
                                clean_phone,
                                module=f"dm_worker_{worker_id}",
                                auto_release=True,
                                timeout=10.0,
                            )
                        except AttributeError:
                            # session_manager not wired (defensive)
                            raise
                        async with session_ctx as lease:
                            if not lease:
                                self._emit("SESSION_BUSY", worker=worker_id, phone=clean_phone, detail="no lease")
                                self.worker_states[worker_id] = "SESSION_BUSY"
                                stall_count += 1
                                if stall_count >= 3:
                                    self.stats["failed"] += 1
                                    self._emit("send_failure", worker=worker_id, phone=clean_phone,
                                               detail="no available sessions after 3 stalls")
                                    logger.warning(
                                        f"DM_WORKER_STALL | worker={worker_id} | "
                                        f"stall_count={stall_count} | dropping target (no available sessions)"
                                    )
                                    break
                                # Session unavailable — re-queue target and retry
                                self._emit("session_acquired", worker=worker_id, phone=clean_phone,
                                           detail="lease=None (retry)")
                                await target_queue.put(target_data)
                                await asyncio.sleep(1.0)
                                continue
                            stall_count = 0
                            self._emit("session_acquired", worker=worker_id, phone=clean_phone)
                            self._emit("proxy_acquired", worker=worker_id, phone=clean_phone,
                                       detail=getattr(lease, "proxy_url", None))
                            self._emit("client_created", worker=worker_id, phone=clean_phone)
                            self.worker_states[worker_id] = "SESSION_ACQUIRED"

                            client = lease.client

                            # Step 4: Connect
                            self._emit("connect_start", worker=worker_id, phone=clean_phone)
                            self.worker_states[worker_id] = "CONNECTING"
                            if not client.is_connected():
                                await client.connect()
                            self._emit("connected", worker=worker_id, phone=clean_phone)
                            if not await client.is_user_authorized():
                                self._emit("authorized", worker=worker_id, phone=clean_phone, detail="NOT authorized")
                                self.worker_states[worker_id] = "TERMINAL_ACCOUNT"
                                raise AuthKeyUnregisteredError(request=None)
                            self._emit("authorized", worker=worker_id, phone=clean_phone)

                            # Step 5: Execute DM action
                            entity = None
                            if isinstance(target_data, dict):
                                user_id = target_data.get("user_id")
                                access_hash = target_data.get("access_hash")
                                username = target_data.get("username")
                                
                                if username and str(username).strip() and str(username).lower() != "none":
                                    u_str = str(username).strip()
                                    entity = u_str if u_str.startswith("@") else f"@{u_str}"
                                elif user_id and access_hash and str(access_hash) != "0":
                                    try:
                                        entity = InputPeerUser(int(user_id), int(access_hash))
                                    except Exception:
                                        entity = None
                                        
                                if not entity and user_id:
                                    entity = int(user_id)
                            else:
                                target_str = str(target_data).strip()
                                if target_str.isdigit():
                                    entity = int(target_str)
                                else:
                                    entity = target_str if target_str.startswith("@") else f"@{target_str}"

                            if not entity:
                                raise ValueError("Could not construct entity tokens.")
                            self._emit("target_resolved", worker=worker_id, phone=clean_phone, detail=str(entity)[:40])

                            self._emit("send_start", worker=worker_id, phone=clean_phone)
                            if media_path and os.path.exists(str(media_path)):
                                is_voice = str(media_path).lower().endswith(('.ogg', '.mp3', '.m4a'))
                                attributes = [DocumentAttributeAudio(voice=True)] if is_voice else None
                                await client.send_file(
                                    entity, str(media_path), caption=final_text,
                                    voice_note=is_voice, attributes=attributes
                                )
                            else:
                                await client.send_message(entity, final_text)

                            self.stats["total_sent"] += 1
                            consecutive_failures = 0
                            current_client = client
                            current_phone = clean_phone
                            self._emit("send_success", worker=worker_id, phone=clean_phone)
                            self.worker_states[worker_id] = "SEND_SUCCESS"

                            # Human-like delay
                            dynamic_delay = max(0.5, 45.0 / max(1, self.proxy_lease_manager.get_available_count()))
                            await asyncio.sleep(random.uniform(dynamic_delay, dynamic_delay + 1.0))

                    except SessionAlreadyOwnedError:
                        stall_count += 1
                        self._emit("SESSION_BUSY", worker=worker_id, phone=clean_phone, detail="SessionAlreadyOwnedError")
                        self.worker_states[worker_id] = "SESSION_BUSY"
                        if stall_count >= 3:
                            self.stats["failed"] += 1
                            self._emit("send_failure", worker=worker_id, phone=clean_phone,
                                       detail="session busy after 3 retries")
                            break
                        continue
                    except (PeerIdInvalidError, ValueError):
                        self.stats["failed"] += 1
                        consecutive_failures = 0
                        self._emit("send_failure", worker=worker_id, phone=clean_phone, detail="PeerIdInvalid/ValueError")

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(await self._generate_live_status())
                            last_ui_update = datetime.now()

                    except (UserIsBlockedError, UserPrivacyRestrictedError):
                        self.stats["failed"] += 1
                        consecutive_failures = 0
                        self._emit("send_failure", worker=worker_id, phone=clean_phone, detail="Blocked/Privacy")

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(await self._generate_live_status())
                            last_ui_update = datetime.now()

                    except (FloodWaitError, PeerFloodError) as e:
                        consecutive_failures += 1
                        self.stats["accounts_down"] += 1
                        self.worker_states[worker_id] = "WAITING_FOR_PROXY"
                        self._emit("send_failure", worker=worker_id, phone=clean_phone,
                                   detail=f"FloodWait/PeerFlood: {e.seconds if hasattr(e, 'seconds') else 'limit'}")
                        # Cooldown the proxy if we have it
                        if lease.proxy_url and self.proxy_lease_manager:
                            await self.proxy_lease_manager.release_proxy(
                                proxy_url=lease.proxy_url, phone=clean_phone,
                                should_cooldown=True,
                                cooldown_reason=f"FloodWait/PeerFlood: {e.seconds if hasattr(e, 'seconds') else 'limit'}",
                            )

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(await self._generate_live_status())
                            last_ui_update = datetime.now()

                    except (AuthKeyUnregisteredError, SessionRevokedError, UserDeactivatedError):
                        self.stats["accounts_down"] += 1
                        self.worker_states[worker_id] = "TERMINAL_ACCOUNT"
                        self._emit("TERMINAL_ACCOUNT", worker=worker_id, phone=clean_phone, detail="revoked/deactivated")
                        # Quarantine the session so no other worker uses it
                        if self.session_manager:
                            await self.session_manager.mark_quarantined(
                                clean_phone,
                                reason="Session revoked/unauthorized in dm_worker",
                                category=ErrorCategory.UNAUTHORIZED,
                            )
                        if hasattr(self.db, "mark_account_revoked"):
                            self.db.mark_account_revoked(clean_phone, "Session revoked/unauthorized")

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(await self._generate_live_status())
                            last_ui_update = datetime.now()

                    except AuthKeyDuplicatedError:
                        self.stats["accounts_down"] += 1
                        self.worker_states[worker_id] = "TERMINAL_ACCOUNT"
                        self._emit("TERMINAL_ACCOUNT", worker=worker_id, phone=clean_phone, detail="auth_key_duplicated")
                        if self.session_manager:
                            await self.session_manager.mark_quarantined(
                                clean_phone,
                                reason="AuthKeyDuplicatedError in dm_worker",
                                category=ErrorCategory.AUTH_KEY_DUPLICATED,
                            )

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(await self._generate_live_status())
                            last_ui_update = datetime.now()
                            
                    except Exception as e:
                        error_str = str(e).lower()
                        if any(x in error_str for x in ["banned", "deactivated", "revoked", "unauthorized"]):
                            self.stats["accounts_down"] += 1
                            self.worker_states[worker_id] = "TERMINAL_ACCOUNT"
                            self._emit("TERMINAL_ACCOUNT", worker=worker_id, phone=clean_phone, detail=error_str[:40])
                            if self.session_manager:
                                await self.session_manager.mark_quarantined(
                                    clean_phone,
                                    reason=f"Runtime drop: {error_str[:40]}",
                                    category=ErrorCategory.UNAUTHORIZED,
                                )
                            if hasattr(self.db, "mark_account_revoked"):
                                self.db.mark_account_revoked(clean_phone, f"Runtime drop: {error_str[:40]}")
                        else:
                            consecutive_failures += 1
                            self._emit("send_failure", worker=worker_id, phone=clean_phone, detail=error_str[:40])
                            if consecutive_failures >= 2:
                                pass  # Allow retry on transient errors

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(await self._generate_live_status())
                            last_ui_update = datetime.now()
                
            except asyncio.CancelledError:
                pass
            finally:
                self.worker_states.pop(worker_id, None)
                if current_phone:
                    self._emit("session_release", worker=worker_id, phone=current_phone)
                    self._emit("proxy_release", worker=worker_id, phone=current_phone)
                    self._emit("account_release", worker=worker_id, phone=current_phone)

        
        # 🔥 RESOURCE-AWARE SCHEDULING (Phase 13)
        # Do NOT create N workers merely because N is the configured maximum.
        # effective_capacity = min(eligible_accounts, available_network_capacity,
        #                          available_session_capacity, configured_limit).
        # At least 1 worker is kept when accounts exist so the engine blocks on
        # proxy/session acquisition (WAITING_FOR_PROXY / WAITING_FOR_ACCOUNT)
        # instead of silently dropping valid work.
        _configured_limit = int(CONFIG.get("DM_MAX_WORKERS", 20))
        _available_proxies = 0
        try:
            _available_proxies = self.proxy_lease_manager.get_available_count()
        except Exception:
            _available_proxies = 0
        _session_capacity = int(CONFIG.get("DM_MAX_WORKERS", 20))
        try:
            if self.session_manager is not None:
                _sm_stats = await self.session_manager.get_stats()
                _session_capacity = max(
                    1,
                    self.session_manager._max_active_clients
                    - int(_sm_stats.get("active_clients", 0)),
                )
        except Exception:
            _session_capacity = int(CONFIG.get("DM_MAX_WORKERS", 20))

        if all_accounts:
            _eff = compute_dm_worker_capacity(
                num_accounts=len(all_accounts),
                available_proxies=_available_proxies,
                session_capacity=_session_capacity,
                configured_limit=_configured_limit,
            )
            num_workers = _eff
        else:
            num_workers = 0
        self._emit("worker_started",
                   detail=f"effective_capacity={num_workers} "
                          f"(accounts={len(all_accounts)}, proxies={_available_proxies}, "
                          f"session_cap={_session_capacity}, limit={_configured_limit})")
        for i in range(num_workers):
            task = asyncio.create_task(dm_worker(i))
            active_workers.append(task)
        
        try:
            await asyncio.gather(*active_workers)
        except asyncio.CancelledError:
            pass
        finally:
            # 🔥 NEW: Cleanup reporter task
            reporter_task.cancel()
            try:
                await reporter_task
            except asyncio.CancelledError:
                pass
        
        self.is_running = False
        final_msg = "✅ **DM CAMPAIGN COMPLETED** ✅\n" if target_queue.empty() else "⚠️ **DM CAMPAIGN HALTED** ⚠️\n"
        await ui_callback(final_msg + await self._generate_live_status())
        
        if media_path and os.path.exists(str(media_path)):
            try:
                os.remove(str(media_path))
            except Exception:
                pass


def setup_dmsender_handlers(bot: TelegramClient, db, proxy_manager=None,
                           proxy_lease_manager=None, session_manager=None,
                           account_lease_manager=None):
    sender_engine = EnterpriseDMSender(db, proxy_lease_manager, session_manager, account_lease_manager)

    def is_admin(sender_id):
        if ADMIN_ID:
            return str(sender_id) == str(ADMIN_ID)
        return True

    @bot.on(events.NewMessage(pattern='/send_dmsender'))
    async def wizard_start(event):
        if not is_admin(event.sender_id): return
        # Flush stale state data for this user to reclaim RAM
        sender_engine.wizard_state.pop(event.sender_id, None)
        
        if sender_engine.is_running:
            await event.reply("⚠️ **Engine Occupied:** Campaign background me active hai.")
            return

        try:
            pipeline = [{"$group": {"_id": "$source_group", "count": {"$sum": 1}}}]
            group_stats = await sender_engine.db.get_group_stats()
            
            if not group_stats:
                msg = "📊 `scraped_data` collection is empty. \n\n👉 Direct single profile target karne ke liye `@username` type karein."
                group_list = []
            else:
                msg = "📊 **Scraped Database Summary:**\n━━━━━━━━━━━━━━━━━━━━━━\n"
                total_users = 0
                group_list = []
                for stat in group_stats:
                    grp = stat["_id"] if stat["_id"] else "Unknown Group"
                    cnt = stat["count"]
                    total_users += cnt
                    group_list.append(str(grp))
                    msg += f"🔹 `{grp}` : **{cnt} users**\n"
                
                msg += f"━━━━━━━━━━━━━━━━━━━━━━\n✨ **Total Available Users:** `{total_users}`\n\n"
                msg += "👉 **Type the EXACT Group Name** to fetch and send messages.\n"
                msg += "👉 **OR Type specific username/ID** to send an individual message."

            sender_engine.wizard_state[event.sender_id] = {
                "step": "AWAITING_TARGET_SELECTION",
                "available_groups": group_list,
                "targets": [],
                "text": "",
                "media": None,
                "limit": 0
            }
            await event.reply(msg)

        except Exception as e:
            await event.reply(f"❌ **Database Connection Error:** {e}")

    @bot.on(events.NewMessage)
    async def wizard_steps(event):
        if not is_admin(event.sender_id): return
        uid = event.sender_id
        if uid not in sender_engine.wizard_state:
            return
            
        if event.text and event.text.startswith('/'):
            return
            
        state = sender_engine.wizard_state[uid]
        step = state["step"]

        if step == "AWAITING_TARGET_SELECTION":
            inp = event.text.strip()
            extracted_targets = []

            if inp in state.get("available_groups", []):
                cursor = await sender_engine.db.get_targets_by_group(inp)
                for doc in cursor:
                    extracted_targets.append({
                        "user_id": doc.get("user_id"),
                        "access_hash": doc.get("access_hash"),
                        "username": doc.get("username"),
                        "phone": doc.get("phone")
                    })
                
                if not extracted_targets:
                    await event.reply("❌ Is group me valid schema lines nahi mili. Phir se chunein.")
                    return
                
                state["targets"] = extracted_targets
                state["step"] = "AWAITING_LIMIT"
                await event.reply(
                    f"✅ **{len(state['targets'])} Users extracted mapping metadata structural array successfully!**\n\n"
                    f"Kitne logo ko message bhejna chahte hain? (Number daalein ya `all` likhein):"
                )
            else:
                state["targets"] = [inp]
                state["limit"] = 1
                state["step"] = "AWAITING_TEXT"
                await event.reply(
                    f"🎯 **Targeting individual:** {inp}\n\n"
                    "📝 Apna Promotional Message bhejein jo user ko DM karna hai.\n"
                    "*(Agar sirf media bhejna hai bina text ke, toh reply me `skip` type karein)*"
                )

        elif step == "AWAITING_LIMIT":
            inp = event.text.strip().lower()
            if inp == "all":
                limit = len(state['targets'])
            elif inp.isdigit():
                limit = int(inp)
                if limit <= 0:
                    await event.reply("❌ Valid number daaliye.")
                    return
            else:
                await event.reply("❌ Invalid format. Number me type karein ya 'all' likhein.")
                return

            state["limit"] = limit
            state["step"] = "AWAITING_TEXT"
            await event.reply(
                f"⚙️ **Target Limit Set to:** {limit}\n\n"
                "📝 Ab apna Promotional Message bhejein jo users ko DM karna hai.\n"
                "*(Agar sirf media bhejna hai bina text ke, toh reply me `skip` type karein)*"
            )

        elif step == "AWAITING_TEXT":
            msg_text = event.text.strip()
            state["text"] = msg_text
            
            state["step"] = "AWAITING_MEDIA"
            await event.reply(
                "🖼️ **Message Template Cached!**\n\n"
                "Ab media upload karein (Image/Video/Voice Note) ya aage badhne ke liye `skip` type karein:"
            )

        elif step == "AWAITING_MEDIA":
            if event.text and event.text.strip().lower() == "skip":
                state["media"] = None
            elif event.media:
                media_path = await bot.download_media(event.media)
                state["media"] = media_path
            else:
                await event.reply("❌ Media context not found. Re-send or type `skip`.")
                return

            ui_msg = await event.reply("⚡ Deploying DM Cluster Resources... Connecting to Accounts...")
            
            async def update_ui_status(text_payload):
                try: await ui_msg.edit(text_payload)
                except Exception: pass

            target_list = state["targets"]
            msg_txt = state["text"]
            media_pth = state["media"]
            limit_val = state["limit"]
            
            sender_engine.wizard_state.pop(uid)
            
            sender_engine.active_task = asyncio.create_task(
                sender_engine.execute_dm_campaign(target_list, msg_txt, media_pth, limit_val, update_ui_status)
            )

    @bot.on(events.NewMessage(pattern='/dm_status'))
    async def dm_status_check(event):
        if not is_admin(event.sender_id): return
        if not sender_engine.is_running:
            await event.reply("ℹ️ **No DM campaign is currently running.**\n\nUse `/send_dmsender` to start a new campaign.")
            return
        
        # Generate and send detailed status
        status_msg = sender_engine._generate_detailed_status()
        await event.reply(status_msg)

    @bot.on(events.NewMessage(pattern='/stop_dmsender'))
    async def wizard_stop(event):
        if not is_admin(event.sender_id): return
        if not sender_engine.is_running:
            await event.reply("ℹ️ Koi DM process running nahi hai.")
            return
        sender_engine.halt_campaign()
        await event.reply("🛑 **Emergency Brake Engaged!** Engine fully stopped.")

    return sender_engine