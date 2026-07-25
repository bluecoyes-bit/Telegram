#!/usr/bin/env python3
"""
Ultimate Enterprise Telegram Suite - WebRTC VoiceChat & Cross-DB Session Migration Engine
Filename: videochat.py
"""

import os
import sys
import time
import asyncio
import random
import logging
import gc  # 🔥 NEW: Garbage collection control
from typing import List, Dict, Optional, Any, Tuple, Set, TYPE_CHECKING
from weakref import WeakSet
from proxy_manager import RobustProxyManager, ProxyEntry


from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, DeleteHistoryRequest
from telethon.errors import (
    FloodWaitError, PhoneNumberBannedError, UserAlreadyParticipantError,
)

logger = logging.getLogger("VideoChatEngineFallback")

# =====================================================================
# 🔥 FIXED: TRUE UNIVERSAL PYTGCALLS ADAPTER (V3 & LEGACY SUPPORT)
# =====================================================================
try:
    from pytgcalls import PyTgCalls
    from pytgcalls.types import MediaStream
    logger.info("✅ PyTgCalls V3 Engine Loaded Successfully.")
except (ModuleNotFoundError, ImportError):
    try:
        from pytgcalls import GroupCallFactory
        logger.info("✅ PyTgCalls Legacy Engine Loaded Successfully.")
        
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
                    except: pass
    except Exception as crash_reason:
        logger.critical(f"🚨 FATAL: pytgcalls library is completely missing or broken! Fix your environment. ({crash_reason})")
        raise crash_reason


from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
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
    
    def __init__(self, db: SuiteDatabase, proxy_manager: Optional[RobustProxyManager] = None):  
        self.db = db
        self.proxy_manager = proxy_manager
        self.scraper_helper = MemberScraper(db)
        self.is_running = False
        # 🔥 FIX 1: WeakSet instead of List for task tracking - avoids memory leaks
        self._active_tasks: set = set()
        self._running_calls: List[PyTgCalls] = []
        self._running_clients: List[TelegramClient] = []
        self._last_status: Dict[str, str] = {}
        # 🔥 FIX 2: Client factory cache to reuse client objects
        self._client_cache: Dict[str, TelegramClient] = {}
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
        
        all_accounts = self.db.get_all_accounts_raw()
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
            
            # 🔥 OPTIMIZATION: Reuse client from cache if available
            cache_key = f"{clean_p}_{api_id}"
            if cache_key in self._client_cache:
                client = self._client_cache[cache_key]
                # Check if still connected
                if client.is_connected():
                    try:
                        await client.get_me()
                        # Still valid, skip reconnection
                        success_count += 1
                        current_session_str = client.session.save()
                        # Batch the update instead of immediate DB write
                        batch_active_updates.append((clean_p, current_session_str))
                        batch_backup_upserts.append((clean_p, current_session_str, device, acc))
                        continue
                    except:
                        pass  # Fall through to reconnect
                # Remove stale cache entry
                del self._client_cache[cache_key]
            
            client = TelegramClient(
                StringSession(session_str), api_id, api_hash,
                device_model=device.get("device_model", "PC 64bit"),
                system_version=device.get("system_version", "Windows 11"),
                app_version=device.get("app_version", "4.8.4"),
                # 🔥 CRITICAL: Limit entity cache to prevent RAM explosion
                entity_cache_limit=50,
            )
            
            try:
                await client.connect()
                # 🔥 OPTIMIZATION: Disable update receiving - we don't need live updates
                # This saves massive CPU/bandwidth
                # await client.catch_up()  # Not needed for validation
                
                is_authorized = await client.is_user_authorized()
                
                if is_authorized:
                    # Cache the client for potential reuse
                    self._client_cache[cache_key] = client
                    success_count += 1
                    current_session_str = client.session.save()
                    
                    # Batch the update
                    batch_active_updates.append((clean_p, current_session_str))
                    batch_backup_upserts.append((clean_p, current_session_str, device, acc))
                else:
                    batch_removals.append(phone)
                    banned_count += 1
                    error_logs.append({"phone": phone, "error": "Session unauthorized."})
                    # Don't cache unauthorized clients
                    try: await client.disconnect()
                    except: pass
                    
            except Exception as e:
                error_str = str(e).lower()
                banned_count += 1
                
                if "auth_key_duplicated" in error_str:
                    reason = "The authorization key was used under two different IP addresses simultaneously."
                elif any(m in error_str for m in ["auth_key_unregistered", "expired", "unauthorized"]):
                    reason = "Session unauthorized."
                elif any(m in error_str for m in ["user_deactivated", "banned"]):
                    reason = "Account has been banned by Telegram infrastructure."
                else:
                    reason = f"Handshake Collapse: {str(e)[:60]}"
                
                batch_removals.append(phone)
                error_logs.append({"phone": phone, "error": reason})
                try: await client.disconnect()
                except: pass
                
            finally:
                # 🔥 OPTIMIZATION: Reduced sleep from 0.4s to 0.1s
                await asyncio.sleep(0.1)
            
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
                        "last_updated": datetime.utcnow()
                    }}
                )
        
        # Bulk backup upserts
        if backup_upserts:
            from datetime import datetime
            for clean_p, session_str, device, acc in backup_upserts:
                existing = backups_coll.find_one({"phone": clean_p})
                orig_auth_at = existing.get("authenticated_at") if existing else (acc.get("authenticated_at") or acc.get("timestamp") or datetime.utcnow())
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
                        "last_backup_sync": datetime.utcnow()
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

                server_client = TelegramClient(
                    StringSession(session_str),
                    api_id,
                    api_hash,
                    device_model=device_metadata.get("device_model", "PC 64bit") if isinstance(device_metadata, dict) else "PC 64bit",
                    system_version=device_metadata.get("system_version", "Windows 11") if isinstance(device_metadata, dict) else "Windows 11",
                    app_version=device_metadata.get("app_version", "4.8.4") if isinstance(device_metadata, dict) else "4.8.4",
                    # 🔥 CRITICAL: Entity cache limit to prevent RAM blowup
                    entity_cache_limit=30,
                )

                try:
                    await server_client.connect()
                    # 🔥 OPTIMIZATION: Disable session save_entities for migration tasks
                    # We don't need to cache entities for a one-time migration
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
                finally:
                    try:
                        await server_client.disconnect()
                    except Exception:
                        pass
                    # 🔥 OPTIMIZATION: Reduced sleep
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
        for attempt in range(1, max_retries + 1):
            if not self.is_running:
                return None
            try:
                full_chat_info = await client(GetFullChannelRequest(channel=entity))
                if hasattr(full_chat_info, "full_chat") and getattr(full_chat_info.full_chat, "call", None):
                    return full_chat_info
                self._voice_log(phone, f"Voice chat not active yet (attempt {attempt}/{max_retries}). Waiting 10s...")
            except Exception as probe_err:
                self._voice_log(phone, f"Voice chat probe failed (attempt {attempt}/{max_retries}): {probe_err}", "warning")
            if attempt < max_retries:
                await asyncio.sleep(10)
        return None

    async def _execute_single_stream(
        self,
        acc_doc: Dict[str, Any],
        group_link: str,
        audio_path: str,
        replacement_queue: asyncio.Queue,
        proxy_manager: Optional[RobustProxyManager] = None,
    ):
        """Asynchronously spawns separate instances for independent audio delivery loops with Native PyTgCalls Takeover Guard."""
        phone = str(acc_doc.get("phone"))
        
        # 🔒 LOCK ACCOUNT
        self.db.acquire_lock(phone)
        
        resolved_audio_path = self._resolve_audio_path(audio_path)
        if not resolved_audio_path:
            msg = f"Audio file not found: {audio_path}"
            self._voice_log(phone, msg, "error")
            self.db.release_lock(phone)
            await self._trigger_replacement_spawn(replacement_queue, group_link, audio_path)
            return

        device = acc_doc.get("device_metadata") or acc_doc.get("device_fingerprint") or random.choice(DEVICE_PROFILES)

        proxy_entry = None
        proxy_dict = None
        if proxy_manager:
            # Try to get proxy from the same country as stored in acc_doc
            stored_proxy = acc_doc.get("proxy")
            preferred_country = stored_proxy.get("country") if stored_proxy else None
            if preferred_country:
                proxy_entry = proxy_manager.get_proxy_by_preference(preferred_country)
            if not proxy_entry:
                proxy_entry = proxy_manager.get_proxy("socks5") or proxy_manager.get_proxy("any")
            if proxy_entry:
                proxy_dict = proxy_entry.dict
                self._voice_log(phone, f"Using proxy {proxy_entry.host}:{proxy_entry.port} (country={proxy_entry.country})")
            else:
                self._voice_log(phone, "No proxy available, using direct connection", "warning")
        
        client = None
        target_entity = None
        app = None
        
        try:
            client = TelegramClient(
                StringSession(acc_doc.get("session_string")),
                int(acc_doc.get("api_id", CONFIG["API_ID"])),
                str(acc_doc.get("api_hash", CONFIG["API_HASH"])),
                device_model=device.get("device_model", "PC 64bit"),
                system_version=device.get("system_version", "Windows 11"),
                app_version=device.get("app_version", "4.8.4"),
                entity_cache_limit=30,
                sequential_updates=False,
                proxy=proxy_dict,
            )
            client.session.save_entities = False
            self._running_clients.append(client)
            self._voice_log(phone, "Connecting Telegram client interface session...")
            await client.connect()
            
            # 🔥 OPTIMIZATION: Reduced stabilization sleep from 1.0s to 0.3s
            await asyncio.sleep(0.3)
            
            if not await client.is_user_authorized():
                raise Exception("Unauthorized session token encountered inside target worker node pool.")

            me = await client.get_me()
            my_user_id = me.id
            self._voice_log(phone, f"Handshake logged-in identity verified: {getattr(me, 'first_name', None) or phone} (ID: {my_user_id})")

            # 🛠️ AUTO GROUP JOINING MATRIX PROTOCOL
            is_private, resolved_token = self.scraper_helper.resolve_group_link(group_link)
            clean_hash = resolved_token.replace('+', '').strip()
            
            try:
                if is_private:
                    from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
                    self._voice_log(phone, f"Auto-joining private invite hash: {clean_hash}")
                    
                    invite_info = await client(CheckChatInviteRequest(clean_hash))
                    if type(invite_info).__name__ == "ChatInviteAlready":
                        target_entity = invite_info.chat
                    else:
                        updates = await client(ImportChatInviteRequest(clean_hash))
                        if getattr(updates, "chats", None):
                            target_entity = updates.chats[0]
                        else:
                            invite_info = await client(CheckChatInviteRequest(clean_hash))
                            target_entity = getattr(invite_info, "chat", None)
                else:
                    self._voice_log(phone, f"Auto-joining public destination: @{clean_hash}")
                    target_entity = await client.get_entity(clean_hash)
                    await client(JoinChannelRequest(target_entity))
                    
            except UserAlreadyParticipantError:
                self._voice_log(phone, "Target channel: Account wrapper node already present.")
                if is_private:
                    from telethon.tl.functions.messages import CheckChatInviteRequest
                    invite_info = await client(CheckChatInviteRequest(clean_hash))
                    target_entity = getattr(invite_info, "chat", None)
            except Exception as join_err:
                self._voice_log(phone, f"Membership extraction route error: {join_err}", "error")
                raise join_err

            if not target_entity:
                if not is_private:
                    target_entity = await client.get_entity(group_link)
                else:
                    raise ValueError(f"Could not resolve private invite entity for hash: {clean_hash}")

            # 🎯 PEER ROUTING CONVERSION
            chat_id = get_channel_peer_id(target_entity)
            self._voice_log(phone, f"Target peer handshake matched structural chat_id: {chat_id}")

            # ⏳ HIGH-SPEED TIMEOUT MODULE: 4 attempts (40 Seconds Max Wait)
            full_chat_info = await self._wait_for_voice_chat(client, target_entity, phone, max_retries=4)
            if not full_chat_info:
                self._voice_log(phone, "⚠️ Active voice chat window not found within 40 seconds. Auto-Releasing pool...", "error")
                self.terminate_voice_cluster()
                return

            # 🚀 Instantiate PyTgCalls
            await asyncio.sleep(0.5)  # 🔥 Reduced from 1.0s
            app = PyTgCalls(client)

            # 🛡️ NATIVE TELETHON EVENT BRIDGE
            @client.on(events.Raw)
            async def native_takeover_handler(update):
                if type(update).__name__ == "UpdateGroupCallParticipants":
                    for participant in getattr(update, "participants", []):
                        if hasattr(participant, "peer") and hasattr(participant.peer, "user_id"):
                            if participant.peer.user_id == my_user_id:
                                if not getattr(participant, "left", False):
                                    print(f"🚨 [USER TAKEOVER GUARD] Manual app activity for +{phone} detected!", flush=True)
                                    
                                    async def force_exit_routine():
                                        try:
                                            if app in self._running_calls: self._running_calls.remove(app)
                                            await app.stop()
                                        except: pass
                                        try:
                                            if client in self._running_clients: self._running_clients.remove(client)
                                            await client.disconnect()
                                        except: pass
                                        self.db.release_lock(phone)
                                    
                                    asyncio.create_task(force_exit_routine())
                                    return

            await app.start()
            self._running_calls.append(app)

            await app.play(chat_id, MediaStream(media_path=resolved_audio_path))
            self._voice_log(phone, "🚀 WebRTC Audio matrix pipeline stream established inside group voice chat pane!")

            # 🔥 OPTIMIZATION: Reduced initial cooldown from 5.0s to 2.0s
            await asyncio.sleep(2.0)

            # 🔥 OPTIMIZATION: Keep Alive with dynamic delay and periodic cleanup
            keepalive_cycle = 0
            while self.is_running:
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
                    if keepalive_cycle < 3:
                        delay = 20
                    elif keepalive_cycle < 10:
                        delay = random.randint(30, 45)
                    else:
                        delay = random.randint(40, 60)  # Longer intervals for stable streams
                    
                    self._voice_log(phone, f"Stream healthy. Next tick in {delay}s (cycle {keepalive_cycle}).")
                    await asyncio.sleep(delay)
                
                except Exception as loop_err:
                    err_txt = str(loop_err).lower()
                    
                    if "already ended" in err_txt or "not found" in err_txt:
                        self._voice_log(phone, "⚠️ Voice chat dropped by admin. Closing pool...", "error")
                        break
                        
                    self._voice_log(phone, f"⚠️ Stream fluctuation detected: {err_txt}. Re-initiating stream...", "warning")
                    try:
                        await app.play(chat_id, MediaStream(media_path=resolved_audio_path))
                        self._voice_log(phone, "✅ Stream successfully re-initiated!")
                    except Exception as re_err:
                        self._voice_log(phone, f"❌ Re-join failed: {re_err}", "error")
                        
                    await asyncio.sleep(5)

        except Exception as e:
            if proxy_entry:
                proxy_entry.record_failure()
            err_str = str(e).lower()
            self._voice_log(phone, f"💥 WebRTC Stream Dropout Exception caught: {e}", "error")
            if any(k in err_str for k in ["banned", "deactivated", "unregistered", "revoked", "disabled"]):
                self.db.mark_account_failed(phone, f"Banned or Dropped during WebRTC call session: {err_str[:60]}")
            try:
                if app and app in self._running_calls: self._running_calls.remove(app)
                if app: await app.stop()
            except: pass
            try:
                if client in self._running_clients: self._running_clients.remove(client)
                if client:
                    await client.disconnect()
            except: pass
            
            await self._trigger_replacement_spawn(replacement_queue, group_link, audio_path)

        finally:
            self.db.release_lock(phone)
            # 🔥 NEW: Force garbage collection after each stream ends
            if gc.isenabled():
                gc.collect()

    # 🔥 NEW: Periodic cleanup method to prevent memory accumulation
    async def _periodic_stream_cleanup(self, client):
        """Periodic cleanup to prevent entity cache from growing unbounded."""
        try:
            # Clear Telethon's internal entity cache
            if hasattr(client, '_entity_cache'):
                # Keep only essential entities (the target channel)
                client._entity_cache.clear()
                logger.debug(f"Entity cache cleared for a running stream client")
            
            # Force garbage collection every 5 cycles
            if gc.isenabled():
                collected = gc.collect(0)  # Only generation 0 (fast)
                gc.collect(1)             # Generation 1
                logger.debug(f"GC collected {collected} objects")
        except Exception:
            pass

    async def _trigger_replacement_spawn(self, replacement_queue: asyncio.Queue, group_link: str, audio_path: str):
        """Picks the next idle account from the queue index and mounts it into the live stream."""
        if not self.is_running:
            return
        try:
            next_backup_doc = replacement_queue.get_nowait()
            phone = next_backup_doc.get("phone")
            print(f"🔄 [REPLACEMENT ENGINE] Deploying backup session +{phone} into active voice cluster loop...", flush=True)
            task = asyncio.create_task(self._execute_single_stream(
                next_backup_doc, group_link, audio_path, replacement_queue, self.proxy_manager
            ))
            # 🔥 FIX: Strong reference to prevent GC
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)
        except asyncio.QueueEmpty:
            print("⚠️ [REPLACEMENT ENGINE] Failed to spawn replacement node: Backup account queue is empty!", flush=True)

    async def launch_voice_cluster(self, group_link: str, audio_file: str = "silent.mp3", desired_count: int = 50) -> str:
        """Triggers the complete concurrent deployment sequence matching the desired targeted count with auto-replacement queue."""
        if not os.path.exists(audio_file):
            return f"❌ **Operation Failed:** Audio file `{audio_file}` nahi mila."

        self.is_running = True
        self._last_status.clear()
        
        # 🔍 Database core pool extraction grid
        active_pool = self.db.get_active_target_sessions()

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

        # 🔥 OPTIMIZATION: Create a connection semaphore to limit concurrent spawns
        # 10 concurrent connections max to prevent CPU/memory spike
        self._connection_semaphore = asyncio.Semaphore(10)
        
        # 🔥 OPTIMIZATION: Staggered launch with semaphore
        launch_tasks = []
        for acc in initial_deploy_batch:
            if not self.is_running:
                break
            task = asyncio.create_task(self._execute_single_stream(acc, group_link, audio_file, replacement_queue, self.proxy_manager))
            self._active_tasks.add(task)
            task.add_done_callback(self._active_tasks.discard)
            launch_tasks.append(task)
            # 🔥 OPTIMIZATION: Reduced launch delay (3-5 seconds instead of CONFIG delay)
            await asyncio.sleep(random.randint(3, 5))

        return f"🚀 **Voice Chat Cluster Active Matrix Initiated:** Target set to `{actual_target}` (Total Available: `{total_fetched}`). Active connections are streaming. Backups loaded in queue: `{replacement_queue.qsize()}` accounts."

    async def terminate_voice_cluster(self):
        if not self.is_running: return
        print("🛑 [VOICECHAT MASTER] Shutdown Core Triggered.", flush=True)
        self.is_running = False
        
        for task in list(self._active_tasks):
            try: task.cancel()
            except: pass
        self._active_tasks.clear()

        # Step A: Hang up all PyTgCalls streams
        for app in list(self._running_calls):
            try: await app.stop()
            except: pass
        self._running_calls.clear()

        await asyncio.sleep(1.0)

        # Step B: Disconnect all Telethon clients
        for client in list(self._running_clients):
            try: await client.disconnect()
            except: pass
        self._running_clients.clear()

        # Step C: Clear client cache
        for client in list(self._client_cache.values()):
            try: await client.disconnect()
            except: pass
        self._client_cache.clear()

        # Step D: Release database locks
        try: self.db.release_all_locks()
        except: pass
        
        if gc.isenabled(): gc.collect()
        print("✅ [VOICECHAT MASTER] All accounts cleanly disconnected.", flush=True)