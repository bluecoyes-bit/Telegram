#!/usr/bin/env python3
"""
Filename: adder.py
"""

import os
import sys
import time
import asyncio
import random
import logging
from contextlib import asynccontextmanager
from typing import List, Dict, Optional, Any, Tuple, AsyncIterator
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser
from telethon.errors import (
    UserPrivacyRestrictedError, UserAlreadyParticipantError,
    FloodWaitError, PeerFloodError, UserIdInvalidError, MessageNotModifiedError
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

from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
from exception_classifier import ErrorCategory, classify_exception
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
    
    def __init__(self, db: SuiteDatabase, proxy_manager: Optional[ProxyManager] = None,
                 proxy_lease_manager=None, session_manager=None, account_lease_manager=None):
        self.db = db
        self.proxy_manager = proxy_manager
        self.proxy_lease_manager = proxy_lease_manager  # 🔥 NEW: Lease manager integration
        self.session_manager = session_manager
        self.account_lease_manager = account_lease_manager
        self.scraper_helper = MemberScraper(db, session_manager=session_manager, account_lease_manager=account_lease_manager)
        self.is_running = False
        self.adder_state: Optional[AdderState] = None # Added for state tracking

        # Telemetry metrics trace trackers
        self.total_added = 0
        self.accounts_down = 0
        self.session_contention = 0  # lease contention — NOT real downtime
        self.privacy_skips = 0

    # ──────────────────────────────────────────────
    # 🔥 ROBUST CLIENT CLEANUP (prevents ghost tasks & Future exception spam)
    # NOTE: intentionally unused — SessionManager owns client teardown; this
    # helper is kept only for emergency manual debugging.
    # ──────────────────────────────────────────────
    @staticmethod
    async def _force_cleanup_client(client: Optional[TelegramClient]) -> None:
        if not client:
            return
        try:
            sender = getattr(client, '_sender', None)
            if sender:
                sender._connecting = False
                
                # 🔥 CRITICAL FIX 1: Cancel MTProtoSender loops
                for loop_name in ['_recv_loop', '_send_loop', '_ping_loop']:
                    task = getattr(sender, loop_name, None)
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task  # Explicitly retrieve exception to silence event loop
                        except (asyncio.CancelledError, Exception):
                            pass
                
                # 🔥 CRITICAL FIX 2: Cancel Connection loops (stops "Task was destroyed" spam)
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
                
                # 🔥 CRITICAL FIX 3: Force close raw transport/socket
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

    async def execute_adding_pipeline(self, target_group_link: str, update_callback, adder_state: Optional[AdderState] = None) -> str:
        """Public entry point: guarantees is_running is reset no matter how the
        run ends (completion, halt, cancellation, or unexpected crash) —
        otherwise the auditor's auto-pause would stay engaged forever."""
        try:
            return await self._execute_adding_pipeline_impl(target_group_link, update_callback, adder_state)
        finally:
            self.is_running = False

    async def _execute_adding_pipeline_impl(self, target_group_link: str, update_callback, adder_state: Optional[AdderState] = None) -> str:

        self.is_running = True
        self.adder_state = adder_state
        self.total_added = 0
        self.accounts_down = 0
        self.session_contention = 0
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

        # 🔥 RESOURCE-AWARE SCHEDULING (Phase 15): the worker pool is not a
        # hard-coded batch. Bounded by eligible accounts, the configured operation
        # limit, and available proxy/network capacity — whichever is smallest
        # (at least 1 so workers can wait for capacity instead of dropping work).
        _adder_configured_limit = int(CONFIG.get("ADDER_MAX_WORKER_SESSIONS", 10))
        _adder_available_proxies = 0
        if self.proxy_lease_manager is not None:
            try:
                _adder_available_proxies = max(
                    0, self.proxy_lease_manager.get_available_count()
                )
            except Exception:
                _adder_available_proxies = 0
        if _adder_available_proxies > 0:
            MAX_WORKER_SESSIONS = max(
                1,
                min(len(active_accounts), _adder_configured_limit, _adder_available_proxies),
            )
        else:
            MAX_WORKER_SESSIONS = max(1, min(len(active_accounts), _adder_configured_limit))
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

        @asynccontextmanager
        async def account_context(acc_doc: dict) -> AsyncIterator[Optional[dict]]:
            phone = str(acc_doc.get("phone", "")).strip()
            clean_phone = phone.replace("+", "")

            if not clean_phone:
                yield None
                return

            try:
                async with self.session_manager.acquire(
                    clean_phone,
                    module="adder",
                    worker_id=f"adder:{clean_phone}",
                    auto_release=True,
                ) as lease:
                    if lease is None:
                        yield None
                        return

                    client = lease.client

                    # ------------------------------------------
                    # Connect
                    # ------------------------------------------
                    if not client.is_connected():
                        await client.connect()

                    # ------------------------------------------
                    # Authorization
                    # ------------------------------------------
                    if not await client.is_user_authorized():
                        raise ValueError("Session Unauthorized/Dead")

                    # ------------------------------------------
                    # Prepare target
                    # ------------------------------------------
                    target_entity = None

                    try:
                        if is_private:
                            updates = await client(ImportChatInviteRequest(resolved_token))
                            if getattr(updates, "chats", None):
                                target_entity = updates.chats[0]
                        else:
                            await client(JoinChannelRequest(resolved_token))
                    except UserAlreadyParticipantError:
                        pass
                    except Exception as exc:
                        logger.warning(
                            "ADDER_TARGET_PREPARE_FAILED | phone=%s | error=%s",
                            phone,
                            exc,
                        )

                    # ------------------------------------------
                    # Resolve entity
                    # ------------------------------------------
                    if target_entity is None:
                        target_entity = await client.get_entity(
                            resolved_token if is_private else target_entity_identifier
                        )

                    if not hasattr(target_entity, "access_hash"):
                        raise ValueError("Target entity has no access_hash")

                    target_peer = InputPeerChannel(target_entity.id, target_entity.access_hash)

                    yield {
                        "phone": phone,
                        "clean_phone": clean_phone,
                        "client": client,
                        "target_peer": target_peer,
                        "lease": lease,
                        "proxy_url": lease.proxy_url or "",
                        "burst_count": 0,
                    }

            except SessionAlreadyOwnedError:
                # Routine contention — NOT downtime; tracked separately.
                self.session_contention += 1
                logger.debug("ADDER_SESSION_BUSY | phone=%s", phone)
                yield None

            except Exception as exc:
                self.accounts_down += 1
                logger.warning(
                    "ADDER_ACCOUNT_INIT_FAILED | phone=%s | error=%s",
                    phone,
                    exc,
                )
                yield None

        async def worker_loop():
            while self.is_running:
                try:
                    account_doc = accounts_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                async with account_context(account_doc) as worker_account:
                    if worker_account is None:
                        # Counters (accounts_down / session_contention) are
                        # updated inside account_context by failure type.
                        continue

                    if self.adder_state:
                        self.adder_state.active_workers += 1

                    try:
                        while self.is_running:
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
                                    if self.adder_state:
                                        self.adder_state.skipped += 1
                                    await asyncio.to_thread(
                                        self.db.log_addition_state, uid, uname, "invalid_identity")
                                    continue

                                api_start_time = time.time()
                                await worker_account["client"](
                                    InviteToChannelRequest(worker_account["target_peer"], [target_user])
                                )
                                api_delay = time.time() - api_start_time

                                self.total_added += 1
                                worker_account["burst_count"] += 1
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "success_added")

                                if self.adder_state:
                                    self.adder_state.completed += 1
                                    self.adder_state.total_delay_sum += api_delay

                                if self.total_added % PROGRESS_UPDATE_INTERVAL == 0:
                                    if not self.adder_state:
                                        await update_callback(
                                            f"📊 **Live Tracking:** `{self.total_added}` members added."
                                        )

                                if worker_account["burst_count"] >= BURST_ADD_LIMIT:
                                    sleep_time = random.uniform(*BURST_COOLDOWN_TIME)
                                    if self.adder_state:
                                        self.adder_state.total_delay_sum += sleep_time
                                    await asyncio.sleep(sleep_time)
                                    worker_account["burst_count"] = 0
                                else:
                                    sleep_time = random.uniform(*HUMAN_ADD_INTERVAL)
                                    if self.adder_state:
                                        self.adder_state.total_delay_sum += sleep_time
                                    await asyncio.sleep(sleep_time)

                            except UserPrivacyRestrictedError:
                                self.privacy_skips += 1
                                if self.adder_state:
                                    self.adder_state.skipped += 1
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "privacy_restricted")

                                sleep_time = random.uniform(3, 6)
                                if self.adder_state:
                                    self.adder_state.total_delay_sum += sleep_time
                                await asyncio.sleep(sleep_time)

                            except UserAlreadyParticipantError:
                                if self.adder_state:
                                    self.adder_state.skipped += 1
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "already_member")

                                sleep_time = random.uniform(1.5, 3.5)
                                if self.adder_state:
                                    self.adder_state.total_delay_sum += sleep_time
                                await asyncio.sleep(sleep_time)

                            except (PeerFloodError, FloodWaitError) as fl_err:
                                # Telegram ordered a backoff: NEVER keep adding
                                # with this account. Requeue the member, release
                                # the lease with the error cooldown (proxy rests
                                # 3-5 min, account 15 min via the lease manager),
                                # and stop using this account immediately.
                                self.accounts_down += 1
                                if self.adder_state:
                                    self.adder_state.failures += 1
                                await members_queue.put(member)
                                try:
                                    worker_account["lease"].proxy_should_cooldown = True
                                except Exception:
                                    pass
                                logger.warning(
                                    "ADDER_FLOOD_STOP | account=%s | err=%s | member requeued, account dropped",
                                    worker_account["clean_phone"], type(fl_err).__name__,
                                )
                                break

                            except (UserIdInvalidError, ValueError):
                                if self.adder_state:
                                    self.adder_state.skipped += 1
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "invalid_identity")
                                continue

                            except Exception as crash:
                                result = classify_exception(crash)
                                if result.is_quarantinable:
                                    # Account is dead (banned/deactivated/revoked):
                                    # quarantine via SessionManager — it marks the
                                    # correct terminal DB status, disconnects the
                                    # client, and releases the proxy with cooldown.
                                    # This account is NEVER used again this run.
                                    self.accounts_down += 1
                                    if self.adder_state:
                                        self.adder_state.failures += 1
                                    try:
                                        await self.session_manager.mark_quarantined(
                                            worker_account["clean_phone"],
                                            reason=result.reason[:100],
                                            category=result.category,
                                        )
                                    except Exception:
                                        pass
                                    logger.warning(
                                        "ADDER_ACCOUNT_QUARANTINED | account=%s | reason=%s",
                                        worker_account["clean_phone"], result.reason[:80],
                                    )
                                    break

                                # Transient error: requeue the member for retry,
                                # rest briefly, and drop the account so it is not
                                # hammered — the proxy gets its cooldown window
                                # when the lease is released.
                                await members_queue.put(member)
                                sleep_time = random.uniform(8, 12)
                                if self.adder_state:
                                    self.adder_state.total_delay_sum += sleep_time
                                await asyncio.sleep(sleep_time)
                                try:
                                    worker_account["lease"].proxy_should_cooldown = True
                                except Exception:
                                    pass
                                logger.info(
                                    "ADDER_ACCOUNT_DROPPED_TRANSIENT | account=%s | err=%s",
                                    worker_account["clean_phone"], str(crash)[:80],
                                )
                                break
                    finally:
                        if self.adder_state:
                            self.adder_state.active_workers = max(
                                0,
                                self.adder_state.active_workers - 1,
                            )

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