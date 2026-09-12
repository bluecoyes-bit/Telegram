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
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
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
    notify_auditor_stop,
    notify_auditor_resume,
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
        
    def stop(self, status: str = "Completed"):
        self.is_running = False
        self.status_msg = status


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

    async def execute_adding_pipeline(
        self,
        target_group_link: str,
        update_callback,
        adder_state: Optional[AdderState] = None,
        requested_workers: Optional[int] = None,
    ) -> str:
        """Public entry point: guarantees is_running is reset no matter how the
        run ends (completion, halt, cancellation, or unexpected crash) —
        otherwise the auditor's auto-pause would stay engaged forever.
        The background auditor/recovery is resumed once the adder finishes."""
        try:
            return await self._execute_adding_pipeline_impl(
                target_group_link, update_callback, adder_state, requested_workers
            )
        finally:
            self.is_running = False
            try:
                notify_auditor_resume()
            except Exception:
                pass

    async def _execute_adding_pipeline_impl(
        self,
        target_group_link: str,
        update_callback,
        adder_state: Optional[AdderState] = None,
        requested_workers: Optional[int] = None,
    ) -> str:

        self.is_running = True
        self.adder_state = adder_state
        self.total_added = 0
        self.accounts_down = 0
        self.session_contention = 0
        self.privacy_skips = 0
        self.flood_drops = 0
        self._flood_halted = False
        # Flood circuit breaker: timestamps of recent PeerFlood/FloodWait stops.
        self._flood_events: List[float] = []
        # The adder owns the proxy pool while running: fully stop the auditor.
        notify_auditor_stop()

        # Wait for the proxy pool to stabilize after stopping the auditor. The
        # auditor's cancelled workers release their proxy leases into cooldown
        # (3-5 min), so without this wait the adder starts with 0 usable
        # proxies and every worker spins on acquisition retries.
        if self.proxy_lease_manager is not None:
            try:
                _proxy_wait_start = time.time()
                _proxy_wait_limit = float(CONFIG.get("ADDER_PROXY_STABILIZE_WAIT", 120.0))
                _last_usable = -1
                while time.time() - _proxy_wait_start < _proxy_wait_limit:
                    usable = self.proxy_lease_manager.usable_available_count()
                    if usable > 0:
                        break
                    if usable != _last_usable:
                        logger.info(
                            f"ADDER_PROXY_WAIT | usable={usable} | waiting for proxy pool "
                            f"to stabilize (max {int(_proxy_wait_limit)}s)..."
                        )
                        _last_usable = usable
                    await asyncio.sleep(5.0)
            except Exception as wait_exc:
                logger.debug(f"ADDER_PROXY_WAIT_ERROR | {wait_exc}")

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

        # 🔥 RESOURCE-AWARE SCHEDULING (Phase 15): the worker pool is not a
        # hard-coded batch. Bounded by eligible accounts, the configured operation
        # limit, the operator's optional override, and available proxy/network
        # capacity — whichever is smallest (at least 1 so workers can wait for
        # capacity instead of dropping work).
        _adder_configured_limit = int(CONFIG.get("ADDER_MAX_WORKER_SESSIONS", 10))
        # Honor operator override (e.g. /addmembers <link> 5) but keep it within
        # the configured hard ceiling.
        if requested_workers is not None and requested_workers > 0:
            _adder_configured_limit = min(_adder_configured_limit, requested_workers)
        _adder_available_proxies = 0
        if self.proxy_lease_manager is not None:
            try:
                # Reserve-aware: never count the login-reserve buffer as
                # operation capacity.
                _adder_available_proxies = max(
                    0, self.proxy_lease_manager.usable_available_count()
                )
            except Exception:
                _adder_available_proxies = 0
        if _adder_available_proxies > 0:
            MAX_WORKER_SESSIONS = max(
                1,
                min(len(active_accounts), _adder_configured_limit, _adder_available_proxies),
            )
        else:
            # Even if no proxy is *immediately* free, spawn at least one worker
            # so it can wait on the lease manager instead of doing nothing.
            MAX_WORKER_SESSIONS = max(1, min(len(active_accounts), _adder_configured_limit))
        logger.info(
            f"ADDER_POOL_SIZING | accounts={len(active_accounts)} | usable_proxies="
            f"{_adder_available_proxies} | configured_limit={_adder_configured_limit} "
            f"requested={requested_workers} -> workers={MAX_WORKER_SESSIONS}"
        )
        HUMAN_ADD_INTERVAL = tuple(CONFIG.get("ADDER_HUMAN_ADD_INTERVAL", (8, 14)))
        BURST_ADD_LIMIT = int(CONFIG.get("ADDER_BURST_ADD_LIMIT", 6))
        BURST_COOLDOWN_TIME = tuple(CONFIG.get("ADDER_BURST_COOLDOWN_TIME", (30, 50)))
        PROGRESS_UPDATE_INTERVAL = int(CONFIG.get("ADDER_PROGRESS_UPDATE_INTERVAL", 10))
        # Global flood breaker: this many PeerFlood/FloodWait stops within the
        # window halts the whole run (Telegram-side invite limits — pushing on
        # would only burn the remaining accounts).
        self._flood_breaker_limit = max(2, int(CONFIG.get("ADDER_FLOOD_CIRCUIT_BREAKER", 5)))
        self._flood_window = max(60.0, float(CONFIG.get("ADDER_FLOOD_WINDOW_SECONDS", 600.0)))

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
        async def account_context(acc_doc: dict, failure: Dict[str, str]) -> AsyncIterator[Optional[dict]]:
            phone = str(acc_doc.get("phone", "")).strip()
            clean_phone = phone.replace("+", "")
            failure["reason"] = "unknown"

            if not clean_phone:
                failure["reason"] = "failed: empty phone"
                yield None
                return

            try:
                async with self.session_manager.acquire(
                    clean_phone,
                    module="adder",
                    worker_id=f"adder:{clean_phone}",
                    auto_release=True,
                    timeout=90.0,
                ) as lease:
                    if lease is None:
                        # SessionManager yielded None without raising: the pool
                        # had no free proxy, or the DB record is terminal/missing.
                        # Report the real cause instead of "unknown".
                        try:
                            no_proxy = (
                                self.proxy_lease_manager is not None
                                and self.proxy_lease_manager.usable_available_count() <= 0
                            )
                        except Exception:
                            no_proxy = False
                        failure["reason"] = "no_proxy" if no_proxy else "unavailable"
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
                            # CheckChatInviteRequest resolves the chat for BOTH
                            # states (already joined -> ChatInviteAlready.chat,
                            # not joined -> ChatInvite) and never mangles the
                            # case-sensitive invite hash into a username lookup.
                            invite_info = await client(CheckChatInviteRequest(resolved_token))
                            if type(invite_info).__name__ == "ChatInviteAlready":
                                target_entity = invite_info.chat
                            else:
                                updates = await client(ImportChatInviteRequest(resolved_token))
                                if getattr(updates, "chats", None):
                                    target_entity = updates.chats[0]
                                else:
                                    invite_info = await client(CheckChatInviteRequest(resolved_token))
                                    target_entity = getattr(invite_info, "chat", None)
                        else:
                            # Public link: resolved_token is the clean username
                            # payload (never the full URL).
                            await client(JoinChannelRequest(resolved_token))
                    except UserAlreadyParticipantError:
                        # Account already joined. For private links, resolve the
                        # chat reference via CheckChatInviteRequest (never via
                        # get_entity on the raw hash).
                        if is_private:
                            try:
                                invite_info = await client(CheckChatInviteRequest(resolved_token))
                                target_entity = getattr(invite_info, "chat", None)
                            except Exception as exc:
                                logger.warning(
                                    "ADDER_TARGET_PREPARE_FAILED | phone=%s | error=%s",
                                    phone,
                                    exc,
                                )
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
                        if is_private:
                            # Last-resort: re-check the invite. NEVER pass the
                            # raw hash to get_entity (it resolves usernames).
                            invite_info = await client(CheckChatInviteRequest(resolved_token))
                            target_entity = getattr(invite_info, "chat", None)
                            if target_entity is None:
                                raise ValueError("Could not resolve private invite entity")
                        else:
                            target_entity = await client.get_entity(resolved_token)

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
                # Routine contention — NOT downtime; the worker may retry.
                self.session_contention += 1
                failure["reason"] = "busy"
                logger.debug("ADDER_SESSION_BUSY | phone=%s", phone)
                yield None

            except Exception as exc:
                self.accounts_down += 1
                if self.adder_state:
                    self.adder_state.failures += 1
                failure["reason"] = f"failed: {type(exc).__name__}: {str(exc)[:80]}"
                logger.warning(
                    "ADDER_ACCOUNT_INIT_FAILED | phone=%s | error=%s",
                    phone,
                    exc,
                )
                yield None

        account_retry: Dict[str, int] = {}
        ADDER_LEASE_ATTEMPTS = 3
        # Proxy cooldowns last 3-5 min; give no-proxy waits enough patience to
        # outlive one cooldown window instead of skipping the account.
        NO_PROXY_LEASE_ATTEMPTS = 6

        async def worker_loop():
            while self.is_running:
                try:
                    account_doc = accounts_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                # Track this worker as alive in the UI as soon as it picks an
                # account (even while waiting for a session/proxy).
                if self.adder_state:
                    self.adder_state.active_workers += 1

                try:
                    failure: Dict[str, str] = {}
                    async with account_context(account_doc, failure) as worker_account:
                        if worker_account is None:
                            # No lease this round. Hard failure -> drop. Contention
                            # or cooling proxy pool -> requeue the account for a
                            # bounded number of retries (never lost, never double-
                            # booked: SessionManager enforces single ownership).
                            reason = failure.get("reason", "unknown")
                            key = str(account_doc.get("phone", "")).strip()
                            if reason.startswith("failed") or not self.is_running:
                                logger.warning(
                                    f"ADDER_ACCOUNT_DROPPED | account={key} | reason={reason}"
                                )
                                continue
                            count = account_retry.get(key, 0) + 1
                            no_proxy = reason == "no_proxy"
                            max_attempts = NO_PROXY_LEASE_ATTEMPTS if no_proxy else ADDER_LEASE_ATTEMPTS
                            if count <= max_attempts:
                                account_retry[key] = count
                                await accounts_queue.put(account_doc)
                                logger.info(
                                    f"ADDER_LEASE_RETRY | account={key} | attempt={count}/"
                                    f"{max_attempts} | reason={reason} | requeued"
                                )
                                if no_proxy:
                                    await asyncio.sleep(random.uniform(30, 60))
                                else:
                                    await asyncio.sleep(random.uniform(15, 30))
                            else:
                                logger.warning(
                                    f"ADDER_ACCOUNT_SKIPPED | account={key} | no lease after "
                                    f"{max_attempts} retries | reason={reason}"
                                )
                            continue

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
                                    self.flood_drops += 1
                                    if self.adder_state:
                                        self.adder_state.failures += 1
                                    await members_queue.put(member)
                                    try:
                                        worker_account["lease"].proxy_should_cooldown = True
                                    except Exception:
                                        pass
                                    logger.warning(
                                        "ADDER_FLOOD_STOP | account=%s | err=%s%s | member requeued, account dropped",
                                        worker_account["clean_phone"], type(fl_err).__name__,
                                        f" | wait={getattr(fl_err, 'seconds', '?')}s" if isinstance(fl_err, FloodWaitError) else "",
                                    )
                                    # ── 🔥 GLOBAL FLOOD CIRCUIT BREAKER ──
                                    # Many flood stops in a short window means the
                                    # whole account pool is Telegram flood-limited.
                                    # Continuing would burn every remaining account
                                    # one by one (and deepen each account's flood
                                    # state), so halt the entire run early.
                                    now_ts = time.time()
                                    self._flood_events.append(now_ts)
                                    while self._flood_events and (now_ts - self._flood_events[0]) > self._flood_window:
                                        self._flood_events.pop(0)
                                    if (
                                        len(self._flood_events) >= self._flood_breaker_limit
                                        and not self._flood_halted
                                    ):
                                        self._flood_halted = True
                                        self.is_running = False
                                        if self.adder_state:
                                            self.adder_state.stop("Stopped (Telegram Flood)")
                                        logger.error(
                                            "ADDER_FLOOD_BREAKER | %s flood stops in %ss window — "
                                            "halting run to protect remaining accounts "
                                            "(preserved=%s, members_requeued=%s)",
                                            len(self._flood_events), int(self._flood_window),
                                            accounts_queue.qsize(), members_queue.qsize(),
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
                            # Closes the member-processing try for this account.
                            pass
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
            if self._flood_halted:
                self.adder_state.stop("Stopped (Telegram Flood)")
            else:
                self.adder_state.stop()

        # ── 🔥 FLOOD-HALT REPORT (checked first — never fake "success") ──
        if self._flood_halted:
            return (
                "🛑 **Flood Protection Engaged — Run Stopped Early**\n\n"
                f"Telegram flood-limited (`PeerFlood`) **{self.flood_drops} accounts** "
                "on their invite attempts within a short window. Continuing would "
                "have burned every remaining account and deepened Telegram's "
                "restrictions on each one.\n\n"
                "📊 **Metrics:**\n"
                f"- Total Added: `{self.total_added}`\n"
                f"- Flood-Dropped Accounts: `{self.flood_drops}`\n"
                f"- Accounts Preserved This Run: `{accounts_queue.qsize()}`\n\n"
                "⏳ **PeerFlood usually lasts 12–24h.** Wait before running "
                "`/addmembers` again, and reduce invite speed/burst settings."
            )

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