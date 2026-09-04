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

        # 🔥 NEW: Use dynamic rolling batch based on available proxies
        use_lease_manager = self.proxy_lease_manager is not None and self.proxy_lease_manager._is_running
        
        if use_lease_manager:
            # Dynamic rolling batch mode - let lease manager dictate concurrency
            return await self._dynamic_rolling_worker(
                target_list, final_text, media_path, limit, ui_callback, all_accounts
            )
        
        # Legacy mode (fallback if lease manager not available)
        account_pool = []
        for acc in all_accounts:
            phone = acc.get("phone")
            if phone:
                self.db.acquire_lock(phone)
            
            session_str = acc.get("session_string") or acc.get("session")
            api_id = int(acc.get("api_id", CONFIG["API_ID"]))
            api_hash = str(acc.get("api_hash", CONFIG["API_HASH"]))
            device = acc.get("device_metadata") or random.choice(DEVICE_PROFILES)
            
            client = self.session_manager._create_client(
                session_str=session_str,
                api_id=api_id,
                api_hash=api_hash,
                device=device,
            )
            account_pool.append({
                "client": client,
                "phone": phone,
                "consecutive_failures": 0,
                "is_connected": False
            })

        self.stats["accounts_used"] = len(account_pool)
        targets = target_list[:limit] if limit > 0 else target_list
        self.stats["total_targets"] = len(targets)
        
        await ui_callback(f"🚀 **DM Engine Started!**\nConnecting `{len(account_pool)}` distributed worker accounts...")

        pool_idx = 0
        target_idx = 0
        last_ui_update = datetime.now()

        try:
            while target_idx < len(targets) and self.is_running and len(account_pool) > 0:
                target_data = targets[target_idx]
                worker = account_pool[pool_idx]
                client = worker["client"]
                phone = worker["phone"]

                if not worker["is_connected"]:
                    try:
                        await client.connect()
                        is_auth = await client.is_user_authorized()
                        if not is_auth:
                            raise AuthKeyUnregisteredError(request=None)
                        worker["is_connected"] = True
                    except Exception as e:
                        error_str = str(e).lower()
                        if any(x in error_str for x in ["unregistered", "deactivated", "banned", "revoked"]):
                            if hasattr(self.db, "mark_account_revoked"):
                                self.db.mark_account_revoked(phone, f"Auth Failed: {error_str[:40]}")
                            else:
                                self.db.update_session_status(phone, "revoked")
                            self.stats["accounts_down"] += 1
                        try: await client.disconnect() 
                        except: pass
                        account_pool.pop(pool_idx)
                        if not account_pool: break
                        pool_idx = pool_idx % len(account_pool)
                        continue

                entity = None
                try:
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

                    if media_path and os.path.exists(str(media_path)):
                        is_voice = str(media_path).lower().endswith(('.ogg', '.mp3', '.m4a'))
                        attributes = [DocumentAttributeAudio(voice=True)] if is_voice else None
                        await client.send_file(
                            entity, 
                            str(media_path), 
                            caption=final_text,
                            voice_note=is_voice,
                            attributes=attributes
                        )
                    else:
                        await client.send_message(entity, final_text)

                    self.stats["total_sent"] += 1
                    worker["consecutive_failures"] = 0
                    target_idx += 1
                    
                    dynamic_delay = max(0.5, 45.0 / max(1, len(account_pool)))
                    await asyncio.sleep(random.uniform(dynamic_delay, dynamic_delay + 1.0))

                except (PeerIdInvalidError, ValueError) as e:
                    try:
                        if isinstance(target_data, dict) and target_data.get("user_id"):
                            resolved_peer = await client.get_input_entity(int(target_data.get("user_id")))
                            if media_path and os.path.exists(str(media_path)):
                                await client.send_file(resolved_peer, str(media_path), caption=final_text)
                            else:
                                await client.send_message(resolved_peer, final_text)
                            self.stats["total_sent"] += 1
                            worker["consecutive_failures"] = 0
                            target_idx += 1
                            continue
                    except Exception:
                        pass
                    
                    self.stats["failed"] += 1
                    target_idx += 1

                except (UserIsBlockedError, UserPrivacyRestrictedError):
                    self.stats["failed"] += 1
                    target_idx += 1

                except FloodWaitError as e:
                    worker["consecutive_failures"] += 1
                    if e.seconds > 300 or worker["consecutive_failures"] >= 2:
                        await client.disconnect()
                        account_pool.pop(pool_idx)
                        if not account_pool: break
                        pool_idx = pool_idx % len(account_pool)
                        continue
                    else:
                        await asyncio.sleep(e.seconds + 1)

                except Exception as e:
                    error_str = str(e).lower()
                    if any(x in error_str for x in ["banned", "deactivated", "unregistered", "revoked", "mute"]):
                        if hasattr(self.db, "mark_account_revoked"):
                            self.db.mark_account_revoked(phone, f"Runtime Drop: {error_str[:40]}")
                        else:
                            self.db.update_session_status(phone, "revoked")
                            
                        self.stats["accounts_down"] += 1
                        account_pool.pop(pool_idx)
                        if not account_pool: break
                        pool_idx = pool_idx % len(account_pool)
                        continue
                    else:
                        worker["consecutive_failures"] += 1
                        if worker["consecutive_failures"] >= 2:
                            try: await client.disconnect() 
                            except: pass
                            account_pool.pop(pool_idx)
                            if not account_pool: break
                            pool_idx = pool_idx % len(account_pool)
                            continue
                            
                if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                    await ui_callback(self._generate_live_status())
                    last_ui_update = datetime.now()

                if len(account_pool) > 0:
                    pool_idx = (pool_idx + 1) % len(account_pool)

        except asyncio.CancelledError:
            pass
        finally:
            # Cleanup allocated locks and connections
            for acc in all_accounts:
                try:
                    phone_num = acc.get("phone")
                    if phone_num:
                        self.db.release_lock(phone_num)
                except:
                    pass

            for worker in account_pool:
                if worker.get("is_connected"):
                    try: await worker["client"].disconnect()
                    except: pass

            self.is_running = False
            final_msg = "✅ **DM CAMPAIGN COMPLETED** ✅\n" if target_idx >= len(targets) else "⚠️ **DM CAMPAIGN HALTED** ⚠️\n"
            await ui_callback(final_msg + self._generate_live_status())

            if media_path and os.path.exists(str(media_path)):
                try: os.remove(str(media_path))
                except: pass

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
        
        await ui_callback(f"🚀 **DM Engine Started (Dynamic Rolling Batch)!**\n"
                         f"Targets: `{len(targets)}`, Accounts: `{len(all_accounts)}`\n"
                         f"Concurrency dictated by available proxies.")

        target_queue = asyncio.Queue()
        for t in targets:
            await target_queue.put(t)

        last_ui_update = datetime.now()
        active_workers = []
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
                        await ui_callback(self._generate_live_status())
                    except Exception:
                        pass
                await asyncio.sleep(8)
    
        reporter_task = asyncio.create_task(_reporter())
        
        
        async def dm_worker(worker_id: int):
            current_client = None
            current_phone = None
            current_proxy_url = None
            consecutive_failures = 0
            
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
                            break
                        account_doc = all_accounts[_account_rr_idx % len(all_accounts)]
                        _account_rr_idx = (_account_rr_idx + 1) % len(all_accounts)

                    phone = account_doc.get("phone")
                    if not phone:
                        continue

                    clean_phone = str(phone).replace("+", "")

                    # Step 3: Acquire session + proxy via SessionManager
                    # This ensures ONE SESSION → ONE CLIENT (prevents AuthKeyDuplicatedError)
                    # and handles proxy leasing automatically.
                    try:
                        async with self.session_manager.acquire(
                            clean_phone,
                            module=f"dm_worker_{worker_id}",
                            auto_release=True,
                            timeout=10.0,
                        ) as lease:
                            if not lease:
                                # Proxy unavailable or session terminal — re-queue target
                                await target_queue.put(target_data)
                                await asyncio.sleep(1.0)
                                continue

                            client = lease.client

                            # Step 4: Connect
                            if not client.is_connected():
                                await client.connect()
                            if not await client.is_user_authorized():
                                raise AuthKeyUnregisteredError(request=None)

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

                            # Human-like delay
                            dynamic_delay = max(0.5, 45.0 / max(1, self.proxy_lease_manager.get_available_count()))
                            await asyncio.sleep(random.uniform(dynamic_delay, dynamic_delay + 1.0))

                    except SessionAlreadyOwnedError:
                        # Another worker is using this account — re-queue and try next
                        await target_queue.put(target_data)
                        continue
                    except (PeerIdInvalidError, ValueError):
                        self.stats["failed"] += 1
                        consecutive_failures = 0

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(self._generate_live_status())
                            last_ui_update = datetime.now()

                    except (UserIsBlockedError, UserPrivacyRestrictedError):
                        self.stats["failed"] += 1
                        consecutive_failures = 0

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(self._generate_live_status())
                            last_ui_update = datetime.now()

                    except (FloodWaitError, PeerFloodError) as e:
                        consecutive_failures += 1
                        self.stats["accounts_down"] += 1
                        # Cooldown the proxy if we have it
                        if lease.proxy_url and self.proxy_lease_manager:
                            await self.proxy_lease_manager.release_proxy(
                                clean_phone, lease.proxy_url,
                                should_cooldown=True,
                                cooldown_reason=f"FloodWait/PeerFlood: {e.seconds if hasattr(e, 'seconds') else 'limit'}",
                            )

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(self._generate_live_status())
                            last_ui_update = datetime.now()

                    except (AuthKeyUnregisteredError, SessionRevokedError, UserDeactivatedError):
                        self.stats["accounts_down"] += 1
                        # Quarantine the session so no other worker uses it
                        if self.session_manager:
                            await self.session_manager.mark_quarantined(
                                clean_phone,
                                reason="Session revoked/unauthorized in dm_worker",
                                category=ErrorCategory.AUTH_ERROR,
                            )
                        if hasattr(self.db, "mark_account_revoked"):
                            self.db.mark_account_revoked(clean_phone, "Session revoked/unauthorized")

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(self._generate_live_status())
                            last_ui_update = datetime.now()

                    except AuthKeyDuplicatedError:
                        self.stats["accounts_down"] += 1
                        if self.session_manager:
                            await self.session_manager.mark_quarantined(
                                clean_phone,
                                reason="AuthKeyDuplicatedError in dm_worker",
                                category=ErrorCategory.AUTH_KEY_DUPLICATED,
                            )

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(self._generate_live_status())
                            last_ui_update = datetime.now()
                            
                    except Exception as e:
                        error_str = str(e).lower()
                        if any(x in error_str for x in ["banned", "deactivated", "revoked", "unauthorized"]):
                            self.stats["accounts_down"] += 1
                            if self.session_manager:
                                await self.session_manager.mark_quarantined(
                                    clean_phone,
                                    reason=f"Runtime drop: {error_str[:40]}",
                                    category=ErrorCategory.AUTH_ERROR,
                                )
                            if hasattr(self.db, "mark_account_revoked"):
                                self.db.mark_account_revoked(clean_phone, f"Runtime drop: {error_str[:40]}")
                        else:
                            consecutive_failures += 1
                            if consecutive_failures >= 2:
                                pass  # Allow retry on transient errors

                        if (datetime.now() - last_ui_update).seconds >= 8 or self.stats["total_sent"] % 10 == 0:
                            await ui_callback(self._generate_live_status())
                            last_ui_update = datetime.now()
                
            except asyncio.CancelledError:
                pass
            finally:
                # Final cleanup
                if current_client:
                    await self._force_cleanup_client(current_client)
        
        # Launch workers concurrently
        num_workers = min(len(all_accounts), 20)  # Cap at 20 concurrent workers
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
        await ui_callback(final_msg + self._generate_live_status())
        
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