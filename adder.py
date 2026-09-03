#!/usr/bin/env python3
"""
Ultimate Enterprise Telegram Suite - High-Performance Multi-Account Rotating Member Adder
Filename: adder.py
"""

import os
import sys
import time
import asyncio
import random
import logging
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser
from telethon.errors import (
    UserPrivacyRestrictedError, UserAlreadyParticipantError,
    FloodWaitError, PeerFloodError, UserIdInvalidError, MessageNotModifiedError
)

from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
from proxy_manager import ProxyManager
from scraper import MemberScraper

logger = logging.getLogger("SuiteAdder")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ==========================================
# 🛠️ PATCH 1: The Tracker & Variables
# ==========================================
class AdderState:
    def __init__(self, total_target, max_workers):
        self.start_time = time.time()
        self.is_running = True
        
        # Queue metrics
        self.completed = 0
        self.skipped = 0
        self.total_target = total_target
        
        # Infra metrics
        self.active_workers = 0
        self.max_workers = max_workers
        self.failures = 0
        
        # Performance metrics
        self.total_delay_sum = 0.0 # Track total delay to calculate average
        self.status_msg = "Running" # Can change to "Completed", "Paused", etc.
        
    def stop(self):
        self.is_running = False
        self.status_msg = "Completed"


# ==========================================
# 🛠️ PATCH 2: The UI Message Generator Function
# ==========================================
def generate_status_ui(state: AdderState) -> str:
    # 1. Calculate Runtime
    elapsed_seconds = max(1, int(time.time() - state.start_time))
    runtime_str = str(timedelta(seconds=elapsed_seconds))
    
    # 2. Queue Math
    remaining = max(0, state.total_target - state.completed - state.skipped)
    completion_pct = 0.0
    if state.total_target > 0:
         completion_pct = (state.completed + state.skipped) / state.total_target * 100

    # 3. Health Math
    health = "Excellent"
    if state.failures > (state.max_workers * 2):
        health = "Warning ⚠️"
    if state.failures > (state.max_workers * 5):
        health = "Critical ❌"

    worker_health_pct = max(0, 100 - (state.failures * 2)) 

    # 4. Performance Math (Throughput & Delay)
    elapsed_minutes = elapsed_seconds / 60
    throughput = round(state.completed / elapsed_minutes, 1) if elapsed_minutes > 0 else 0
    
    avg_delay = round(state.total_delay_sum / state.completed, 1) if state.completed > 0 else 0.0

    # 5. ETA Math
    eta_str = "Calculating..."
    if throughput > 0:
        eta_minutes = remaining / throughput
        eta_td = timedelta(minutes=eta_minutes)
        hours, remainder = divmod(eta_td.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        eta_str = f"{hours:02}h {minutes:02}m"
        if eta_td.days > 0:
            eta_str = f"{eta_td.days}d " + eta_str

    # 6. Formatting the Exact UI Structure
    ui = f"""⚙️ Adder Status:
📊 Live Tracking: {state.completed} members added.

🚀 **ENTERPRISE MEMBER ADDER**

────────────────────────────────
⚡ **SYSTEM STATUS**

⏱️ **Runtime**            {runtime_str}
🟢 **Status**            {state.status_msg}
🛡️ **Health**           {health}
────────────────────────────────

🎯 **QUEUE MANAGEMENT**

✅ **Completed**           {state.completed}
⏭️ **Skipped**              {state.skipped}
⏳ **Remaining**            {remaining}

📈 **Completion**           {completion_pct:.1f}%
────────────────────────────────

🏗️ **INFRASTRUCTURE**

👥 **Worker Pool**          {state.active_workers} / {state.max_workers}
🟢 **Worker Health**        {worker_health_pct}%
⚠️ **Failures**             {state.failures}
────────────────────────────────

🚀 **PERFORMANCE**

⚡ **Throughput**           {throughput} members/min
⏱️ **Average Delay**        {avg_delay} sec
🕒 **Est. Finish**     {eta_str}
────────────────────────────────
🔄 *Updated just now*"""

    return ui


# ==========================================
# 🛠️ PATCH 3: The Background Monitor Task
# ==========================================
async def status_updater_loop(client, chat_id, message_id, state: AdderState):
    """
    Yeh independent background task hai.
    Main script freeze ho ya block ho, yeh UI refresh karta rahega.
    """
    while state.is_running:
        try:
            new_text = generate_status_ui(state)
            
            # Send the edit request (Update interval: 10 seconds to avoid flood waits)
            await client.edit_message(chat_id, message_id, new_text)
            
        except MessageNotModifiedError:
            # Telegram throws this if the message hasn't changed. Ignore it safely.
            pass
        except Exception as e:
            # Agar network issue hai toh yahan catch hoga, par loop break nahi hoga.
            pass
            
        await asyncio.sleep(10) # ⏳ Wait 10 seconds before next refresh

    # Final UI update immediately after the adder loop finishes
    try:
        final_text = generate_status_ui(state)
        # Update 'Updated just now' to exact completion time
        final_text = final_text.replace("Updated just now", f"Completed at {datetime.now().strftime('%H:%M:%S')}")
        await client.edit_message(chat_id, message_id, final_text)
    except Exception:
        pass


class EnterpriseMemberAdder:
    """Manages multi-account smart rotation loops, safe bursts padding, and anti-ban tracking matrix."""
    
    def __init__(self, db: SuiteDatabase, proxy_manager: Optional[ProxyManager] = None, 
                 proxy_lease_manager=None):
        self.db = db
        self.proxy_manager = proxy_manager
        self.proxy_lease_manager = proxy_lease_manager  # 🔥 NEW: Lease manager integration
        self.scraper_helper = MemberScraper(db)
        self.is_running = False
        self.adder_state: Optional[AdderState] = None # Added for state tracking
        
        # Telemetry metrics trace trackers
        self.total_added = 0
        self.accounts_down = 0
        self.privacy_skips = 0

    async def execute_adding_pipeline(self, target_group_link: str, update_callback, adder_state: Optional[AdderState] = None) -> str:
        """
        Executes structural lookups from Scraped DB pool, starts multiple account workers,
        and updates progress states back to the live central Telegram Bot UI dashboard.
        """
        self.is_running = True
        self.adder_state = adder_state
        self.total_added = 0
        self.accounts_down = 0
        self.privacy_skips = 0

        # Pull available unprocessed targeted members list from DB 2 Cloud Cache Repo
        scraped_pool = await self.db.fetch_unprocessed_scraped_pool()
        if not scraped_pool:
            self.is_running = False
            return "⚠️ **Operation Aborted:** Scraped members database empty ya already processed hai! Pehle `/scrape` commands run karein."

        # Pull fully authorized sessions list from source DB1 storage.
        active_accounts = await self.db.get_active_target_sessions()
        if not active_accounts:
            self.is_running = False
            return "❌ **Operation Failed:** Source DB (`source_accounts`) me active sessions nahi mile. Pehle `/reload_accounts`, `/login`, ya `/refresh_accounts` run karein."

        is_private, resolved_token = self.scraper_helper.resolve_group_link(target_group_link)
        target_entity_identifier = resolved_token if is_private else target_group_link

        MAX_WORKER_SESSIONS = min(len(active_accounts), CONFIG.get("ADDER_MAX_WORKER_SESSIONS", 10))
        HUMAN_ADD_INTERVAL = tuple(CONFIG.get("ADDER_HUMAN_ADD_INTERVAL", (8, 14)))
        BURST_ADD_LIMIT = int(CONFIG.get("ADDER_BURST_ADD_LIMIT", 6))
        BURST_COOLDOWN_TIME = tuple(CONFIG.get("ADDER_BURST_COOLDOWN_TIME", (30, 50)))
        PROGRESS_UPDATE_INTERVAL = int(CONFIG.get("ADDER_PROGRESS_UPDATE_INTERVAL", 10))

        members_queue = asyncio.Queue()
        PAGE_SIZE = 500
        offset = 0
        while True:
            page = await self.db.fetch_unprocessed_scraped_pool_paginated(offset, PAGE_SIZE)
            if not page:
                break
            for member in page:
                await members_queue.put(member)
            offset += PAGE_SIZE
            if len(page) < PAGE_SIZE:
                break

        # Dynamically set target for the UI state tracker
        if self.adder_state:
            self.adder_state.total_target = members_queue.qsize()
            self.adder_state.max_workers = MAX_WORKER_SESSIONS
            self.adder_state.active_workers = 0

        accounts_queue = asyncio.Queue()
        for acc_doc in active_accounts:
            await accounts_queue.put(acc_doc)

        async def initialize_account(acc_doc: dict):
            phone = str(acc_doc.get("phone"))
            clean_phone = phone.replace("+", "")
            self.db.acquire_lock(phone) # 🔒 Lock account instantly so auditor ignores it
            
            session_str = acc_doc.get("session_string") or acc_doc.get("session")
            device = acc_doc.get("device_metadata") or random.choice(DEVICE_PROFILES)
            
            # 🔥 NEW: Use ProxyLeaseManager if available for dynamic rolling batch
            use_lease_manager = (self.proxy_lease_manager is not None and 
                                hasattr(self.proxy_lease_manager, '_is_running') and 
                                self.proxy_lease_manager._is_running)
            
            if use_lease_manager:
                # Dynamic rolling batch mode - acquire proxy via lease manager
                proxy_dict = await self.proxy_lease_manager.acquire_proxy(clean_phone)
                if not proxy_dict:
                    logger.warning(f"⚠️ Lease manager returned no proxy for {phone}")
                    self.db.release_lock(phone)
                    return None
                
                client = TelegramClient(
                    StringSession(session_str),
                    int(acc_doc.get("api_id", CONFIG["API_ID"])),
                    str(acc_doc.get("api_hash", CONFIG["API_HASH"])),
                    device_model=device.get("device_model", "PC 64bit"),
                    system_version=device.get("system_version", "Windows 11"),
                    app_version=device.get("app_version", "4.8.4"),
                    proxy=proxy_dict
                )
                
                try:
                    await client.connect()
                    if await client.is_user_authorized():
                        return {
                            "phone": phone,
                            "clean_phone": clean_phone,
                            "client": client,
                            "proxy_url": proxy_dict.get("url", "") or f"{proxy_dict.get('addr')}:{proxy_dict.get('port')}",
                            "burst_count": 0,
                        }
                    else:
                        raise ValueError("Session Unauthorized/Dead")
                except Exception as e:
                    logger.debug(f"🔄 Connect failed for {phone}: {e}")
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    # Release proxy on failure
                    proxy_url = proxy_dict.get("url", "") or f"{proxy_dict.get('addr')}:{proxy_dict.get('port')}"
                    await self.proxy_lease_manager.release_proxy(
                        proxy_url, clean_phone,
                        should_cooldown=False, cooldown_reason="Connection failed"
                    )
                    self.db.release_lock(phone)
                    return None
            
            # Legacy mode (fallback if lease manager not available)
            # 🚀 PATCH: 5 Attempts with Strict Proxy Rotation (No Direct Connection)
            max_attempts = 5
            client = None
            is_connected = False
            
            for attempt in range(1, max_attempts + 1):
                proxy_node = None
                # Fetch fresh proxy on EVERY attempt
                if self.proxy_manager and self.proxy_manager.working_count > 0:
                    raw_proxy = self.proxy_manager.get_proxy()
                    if raw_proxy:
                        proxy_node = {
                            "proxy_type": raw_proxy.get("type", "socks5"),
                            "addr": raw_proxy.get("host"),
                            "port": raw_proxy.get("port"),
                            "username": raw_proxy.get("username"),
                            "password": raw_proxy.get("password"),
                            "rdns": True
                        }
                
                # Strict check: Agar proxy nahi mili, toh wait and retry. Direct connection NAHI karni.
                if not proxy_node:
                    logger.warning(f"⚠️ No active proxies available for {phone} (Attempt {attempt}). Waiting...")
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                    continue

                # Initialize client inside loop to apply new proxy dynamically
                client = TelegramClient(
                    StringSession(session_str),
                    int(acc_doc.get("api_id", CONFIG["API_ID"])),
                    str(acc_doc.get("api_hash", CONFIG["API_HASH"])),
                    device_model=device.get("device_model", "PC 64bit"),
                    system_version=device.get("system_version", "Windows 11"),
                    app_version=device.get("app_version", "4.8.4"),
                    proxy=proxy_node # 🔥 Strict proxy integration
                )

                try:
                    await client.connect()
                    # Double check if session is still alive after connecting
                    if await client.is_user_authorized():
                        is_connected = True
                        break # ✅ Success! Break the retry loop
                    else:
                        raise ValueError("Session Unauthorized/Dead")
                        
                except Exception as e:
                    logger.debug(f"🔄 Proxy/Connect attempt {attempt} failed for {phone}: {e}")
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    
                    # Background delay before trying the next proxy
                    if attempt < max_attempts:
                        await asyncio.sleep(random.uniform(1.5, 3.5)) 
            
            # Agar 5 attempts ke baad bhi fail ho gaya, toh account ko safe mark karke drop karo
            if not is_connected or not client:
                self.db.release_lock(phone) # 🔓 Unlock safely
                return None

            try:
                target_entity = None
                
                try:
                    if is_private:
                        # Capture updates to resolve private entity accurately
                        updates = await client(ImportChatInviteRequest(resolved_token))
                        if getattr(updates, "chats", None):
                            target_entity = updates.chats[0]
                    else:
                        await client(JoinChannelRequest(resolved_token))
                except UserAlreadyParticipantError:
                    pass
                except Exception:
                    pass

                # Fallback for standard entities if not caught via private routing
                if not target_entity:
                    target_entity = await client.get_entity(resolved_token if is_private else target_entity_identifier)

                target_peer = InputPeerChannel(target_entity.id, target_entity.access_hash)
                
                return {
                    "phone": phone,
                    "client": client,
                    "target_peer": target_peer,
                    "burst_count": 0,
                }
            except Exception:
                try:
                    await client.disconnect()
                except Exception:
                    pass
                self.db.release_lock(phone) # 🔓 Unlock immediately if entity resolution fails
                return None

        async def worker_loop():
            worker_account = None
            try:
                while self.is_running:
                    if worker_account is None:
                        try:
                            account_doc = accounts_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        worker_account = await initialize_account(account_doc)
                        if worker_account is None:
                            self.accounts_down += 1
                            if self.adder_state:
                                self.adder_state.failures += 1
                            continue
                        
                        if self.adder_state:
                            self.adder_state.active_workers += 1

                    try:
                        member = members_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    uname = str(member.get("username", "")).strip()
                    uid = str(member.get("user_id", "")).strip()
                    access_hash = str(member.get("access_hash", "0")).strip()
                    identity = uname if (uname and uname != "None" and uname != "") else uid

                    try:
                        if uname and uname != "None" and uname != "":
                            target_user = await worker_account["client"].get_input_entity(uname)
                        elif uid and access_hash and access_hash != "0":
                            target_user = InputPeerUser(int(uid), int(access_hash))
                        else:
                            if self.adder_state: self.adder_state.skipped += 1
                            self.db.log_addition_state(uid, uname, "invalid_identity")
                            continue

                        api_start_time = time.time()
                        await worker_account["client"](InviteToChannelRequest(worker_account["target_peer"], [target_user]))
                        api_delay = time.time() - api_start_time

                        self.total_added += 1
                        worker_account["burst_count"] += 1
                        self.db.log_addition_state(uid, uname, "success_added")
                        
                        if self.adder_state:
                            self.adder_state.completed += 1
                            self.adder_state.total_delay_sum += api_delay

                        if self.total_added % PROGRESS_UPDATE_INTERVAL == 0:
                            # Default callback only triggers if enterprise monitor is not initialized
                            if not self.adder_state:
                                await update_callback(f"📊 **Live Tracking:** `{self.total_added}` members added.")

                        if worker_account["burst_count"] >= BURST_ADD_LIMIT:
                            sleep_time = random.uniform(*BURST_COOLDOWN_TIME)
                            if self.adder_state: self.adder_state.total_delay_sum += sleep_time
                            await asyncio.sleep(sleep_time)
                            worker_account["burst_count"] = 0
                        else:
                            sleep_time = random.uniform(*HUMAN_ADD_INTERVAL)
                            if self.adder_state: self.adder_state.total_delay_sum += sleep_time
                            await asyncio.sleep(sleep_time)

                    except UserPrivacyRestrictedError:
                        self.privacy_skips += 1
                        if self.adder_state: self.adder_state.skipped += 1
                        self.db.log_addition_state(uid, uname, "privacy_restricted")
                        
                        sleep_time = random.uniform(3, 6)
                        if self.adder_state: self.adder_state.total_delay_sum += sleep_time
                        await asyncio.sleep(sleep_time)

                    except UserAlreadyParticipantError:
                        if self.adder_state: self.adder_state.skipped += 1
                        self.db.log_addition_state(uid, uname, "already_member")
                        
                        sleep_time = random.uniform(1.5, 3.5)
                        if self.adder_state: self.adder_state.total_delay_sum += sleep_time
                        await asyncio.sleep(sleep_time)

                    except (PeerFloodError, FloodWaitError) as e:
                        self.accounts_down += 1
                        if self.adder_state:
                            self.adder_state.failures += 1
                            self.adder_state.active_workers = max(0, self.adder_state.active_workers - 1)
                        await members_queue.put(member) # 🔥 Repopulate queue on drop
                        
                        # 🔥 NEW: Release proxy with cooldown if using lease manager
                        if use_lease_manager and worker_account.get("proxy_url"):
                            await self.proxy_lease_manager.release_proxy(
                                worker_account["proxy_url"], worker_account["clean_phone"],
                                should_cooldown=True,
                                cooldown_reason=f"FloodWait/PeerFlood: {e.seconds if hasattr(e, 'seconds') else 'limit'}"
                            )
                        
                        # Cleanup client with robust method
                        await self._force_cleanup_client(worker_account["client"])
                        self.db.release_lock(worker_account["phone"]) # 🔓 Unlock dropped account
                        worker_account = None
                        continue

                    except (UserIdInvalidError, ValueError):
                        if self.adder_state: self.adder_state.skipped += 1
                        self.db.log_addition_state(uid, uname, "invalid_identity")
                        continue

                    except Exception as crash:
                        err_msg = str(crash).lower()
                        if any(k in err_msg for k in ["banned", "deactivated", "revoked", "disabled"]):
                            self.accounts_down += 1
                            if self.adder_state:
                                self.adder_state.failures += 1
                                self.adder_state.active_workers = max(0, self.adder_state.active_workers - 1)
                                
                            if hasattr(self.db, "mark_account_failed"):
                                self.db.mark_account_failed(worker_account["phone"], f"Banned at runtime: {str(crash)[:80]}")
                            else:
                                self.db.mark_account_revoked(worker_account["phone"], f"Banned at runtime: {str(crash)[:80]}")
                            
                            # 🔥 NEW: Release proxy with cooldown if using lease manager
                            if use_lease_manager and worker_account.get("proxy_url"):
                                await self.proxy_lease_manager.release_proxy(
                                    worker_account["proxy_url"], worker_account["clean_phone"],
                                    should_cooldown=True,
                                    cooldown_reason=f"Banned/Deactivated: {err_msg[:40]}"
                                )
                            
                            try:
                                await worker_account["client"].disconnect()
                            except Exception:
                                pass
                            self.db.release_lock(worker_account["phone"]) # 🔓 Unlock banned account
                            worker_account = None
                            continue
                            
                        sleep_time = random.uniform(8, 12)
                        if self.adder_state: self.adder_state.total_delay_sum += sleep_time
                        await asyncio.sleep(sleep_time)
                        
            finally:
                # Loop khatam hone ke baad final cleanup
                if worker_account is not None:
                    # 🔥 NEW: Release proxy without cooldown on normal exit
                    if use_lease_manager and worker_account.get("proxy_url"):
                        await self.proxy_lease_manager.release_proxy(
                            worker_account["proxy_url"], worker_account["clean_phone"],
                            should_cooldown=False
                        )
                    
                    if self.adder_state:
                        self.adder_state.active_workers = max(0, self.adder_state.active_workers - 1)
                    try:
                        await worker_account["client"].disconnect()
                    except Exception:
                        pass
                    self.db.release_lock(worker_account["phone"]) # 🔓 Unlock safely at the end

        # 🔥 FIX: Launch workers concurrently and await execution
        self.active_workers = [asyncio.create_task(worker_loop()) for _ in range(MAX_WORKER_SESSIONS)]
        try:
            await asyncio.gather(*self.active_workers)
        except asyncio.CancelledError:
            pass

        # Stop tracker gracefully after gathering workers
        if self.adder_state:
            self.adder_state.stop()

        if self.accounts_down >= len(active_accounts) and not members_queue.empty():
            return (
                f"⚠️ **All Active Workers Stopped!** Limit reached or sessions blocked. Try again later.\n\n📊 **Final Metrics Summary:**\n- Total Added: `{self.total_added}`\n- Banned/Down Nodes: `{self.accounts_down}`"
            )

        return (
            f"🏁 **Adding Process Completed Successfully!**\n\n📊 **Final Session Summary Details:**\n- Total New Inhabitants: `{self.total_added}`\n- Total Filtered Skips: `{self.privacy_skips}`\n- Restructured Accounts Down: `{self.accounts_down}`"
        )

    def halt_engine(self):
        """Kills active loop variables instantly safely."""
        self.is_running = False
        if hasattr(self, 'adder_state') and self.adder_state:
            self.adder_state.stop()
        if hasattr(self, 'active_workers'):
            for worker in self.active_workers:
                worker.cancel()