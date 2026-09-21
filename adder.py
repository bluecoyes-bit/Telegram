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
from collections import deque
from contextlib import asynccontextmanager
from typing import List, Dict, Optional, Any, Tuple, AsyncIterator, Set
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser
from telethon.errors import (
    UserPrivacyRestrictedError, UserAlreadyParticipantError,
    FloodWaitError, PeerFloodError, UserIdInvalidError, MessageNotModifiedError,
    UserNotMutualContactError,
)
from telethon.errors.rpcbaseerrors import RPCError as TelethonRPCError

try:
    from telethon.errors import UserBannedInChannelError
except ImportError:
    UserBannedInChannelError = None
try:
    from telethon.errors import ChatMemberAddFailedError
except ImportError:
    ChatMemberAddFailedError = None
try:
    from telethon.errors import ChannelPrivateError
except ImportError:
    ChannelPrivateError = None


def _invite_error_blob(exc: BaseException) -> str:
    return f"{type(exc).__name__} {exc}".upper()


def _is_chat_member_add_failed(exc: BaseException) -> bool:
    """Telegram 400 CHAT_MEMBER_ADD_FAILED is a per-member skip, not a campaign crash.

    Telethon's class name is ChatMemberAddFailedError; the RPC text is
    CHAT_MEMBER_ADD_FAILED. Matching only the type name used to miss it,
    re-raise, and kill every worker via asyncio.gather.
    """
    blob = _invite_error_blob(exc)
    return (
        "CHAT_MEMBER_ADD_FAILED" in blob
        or "CHATMEMBERADDFAILED" in blob
        or (ChatMemberAddFailedError is not None and isinstance(exc, ChatMemberAddFailedError))
    )


def _batch_letter(index: int) -> str:
    n = max(0, int(index)) + 1
    out = ""
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out or "A"


def _is_user_banned_in_channel(exc: BaseException) -> bool:
    blob = _invite_error_blob(exc)
    return (
        "USER_BANNED_IN_CHANNEL" in blob
        or (UserBannedInChannelError is not None and isinstance(exc, UserBannedInChannelError))
    )


def _is_target_chat_stop(exc: BaseException) -> bool:
    """This account cannot write/add in THIS chat. Session is not dead."""
    if ChannelPrivateError is not None and isinstance(exc, ChannelPrivateError):
        return True
    if _is_user_banned_in_channel(exc):
        return True
    blob = _invite_error_blob(exc)
    compact = blob.replace("_", "").replace(" ", "")
    return any(token in compact for token in (
        "CHANNELPRIVATE", "CHATWRITEFORBIDDEN", "CHATADMINREQUIRED",
    ))


def _is_add_restriction_error(exc: BaseException) -> bool:
    """PeerFlood or chat-ban: retry once, then 24h rest for this account."""
    if isinstance(exc, PeerFloodError):
        return True
    blob = _invite_error_blob(exc)
    if "PEER_FLOOD" in blob or "PEERFLOOD" in blob:
        return True
    return _is_user_banned_in_channel(exc)


def plan_adder_accounts(
    member_count: int,
    available_accounts: int,
    members_per_account: int = 30,
) -> int:
    """Use ceil(members/30) accounts — never more than are available."""
    if member_count <= 0 or available_accounts <= 0:
        return 0
    per = max(1, int(members_per_account))
    needed = (int(member_count) + per - 1) // per
    return min(int(available_accounts), max(1, needed))


RESTRICTION_ERRORS = (PeerFloodError,)
if UserBannedInChannelError is not None:
    RESTRICTION_ERRORS = (PeerFloodError, UserBannedInChannelError)

FLOOD_STOP_ERRORS = RESTRICTION_ERRORS

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
from database import SuiteDatabase, is_spam_park_active, is_module_rest_active
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
        self.parked = 0
        
        # Performance metrics
        self.total_delay_sum = 0.0 # Track total delay to calculate average
        self.status_msg = "Running" # Can change to "Completed", "Paused", etc.
        self.resting: List[str] = []  # phones in 24h add-rest, shown as unavailable
        
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

    parked = int(getattr(state, "parked", 0) or 0)
    failures = int(state.failures or 0)
    max_workers = max(1, int(state.max_workers or 1))
    if failures > max_workers:
        health = "Critical"
    elif failures > 0 or (parked > 0 and state.completed == 0):
        health = "Warning"
    else:
        health = "Excellent"

    if not state.is_running:
        worker_health_pct = 100 if failures == 0 else max(
            0, int(100 * (1 - failures / max_workers))
        )
    else:
        worker_health_pct = int(100 * state.active_workers / max_workers) 

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

    resting_list = list(getattr(state, "resting", None) or [])
    if resting_list:
        rest_lines = "\n".join(f"⏸️ {row}" for row in resting_list[-8:])
        resting_block = f"\n{rest_lines}\n────────────────────────────────"
    else:
        resting_block = ""

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
🅿️ **Parked (this target)** {parked}
⏸️ **Resting 24h (add)**    {len(getattr(state, "resting", None) or [])}
────────────────────────────────{resting_block}

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
        self.accounts_acquired = 0
        self.session_contention = 0  # lease contention — NOT real downtime
        self.privacy_skips = 0
        self.flood_drops = 0
        self.chat_parks = 0
        self.total_skipped = 0
        self._batch_index = 0
        self.batch_reports: List[Dict[str, Any]] = []

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
        self.chat_parks = 0
        self.total_skipped = 0
        self.accounts_acquired = 0
        self._batch_index = 0
        self.batch_reports = []
        self._stop_new_batches = False
        self._first_invite_chat_bans = 0
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
                while self.is_running and time.time() - _proxy_wait_start < _proxy_wait_limit:
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

        if not self.is_running:
            return "🛑 **Member Adder Halted** before workers started."

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
        active_accounts = [
            a for a in active_accounts
            if not is_module_rest_active(a, "adder")
        ]
        if not active_accounts:
            self.is_running = False
            return "❌ **Operation Failed:** Koi adder-free account nahi mila (sab 24h adder rest pe hain)."

        is_private, resolved_token = self.scraper_helper.resolve_group_link(target_group_link)

        # 10 accounts = 1 batch = up to 300 members. At most 2 batches in flight.
        BATCH_SIZE = max(1, int(CONFIG.get("ADDER_BATCH_SIZE", 10)))
        MAX_CONCURRENT_BATCHES = max(1, min(2, int(CONFIG.get("ADDER_MAX_CONCURRENT_BATCHES", 2))))
        MEMBERS_PER_ACCOUNT = max(1, int(CONFIG.get("ADDER_MEMBERS_PER_ACCOUNT", 30)))
        LIVE_CAP = BATCH_SIZE * MAX_CONCURRENT_BATCHES
        SHORT_FLOOD_WAIT = max(1, int(CONFIG.get("ADDER_SHORT_FLOOD_WAIT", 30)))
        ACCOUNT_LAUNCH_DELAY = tuple(CONFIG.get("ADDER_ACCOUNT_LAUNCH_DELAY", (8, 15)))
        HUMAN_ADD_INTERVAL = tuple(CONFIG.get("ADDER_HUMAN_ADD_INTERVAL", (25, 45)))
        JOIN_SETTLE_DELAY = tuple(CONFIG.get("ADDER_JOIN_SETTLE_DELAY", (8, 18)))
        BURST_ADD_LIMIT = int(CONFIG.get("ADDER_BURST_ADD_LIMIT", 6))
        BURST_COOLDOWN_TIME = tuple(CONFIG.get("ADDER_BURST_COOLDOWN_TIME", (30, 50)))
        PROGRESS_UPDATE_INTERVAL = int(CONFIG.get("ADDER_PROGRESS_UPDATE_INTERVAL", 10))
        LEASE_RETRY_DELAY = tuple(CONFIG.get("ADDER_LEASE_RETRY_DELAY", (15, 30)))
        NO_PROXY_RETRY_DELAY = tuple(CONFIG.get("ADDER_NO_PROXY_RETRY_DELAY", (30, 60)))
        self._batch_index = 0
        self.batch_reports = []

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

        n_queued = members_queue.qsize()
        n_members = n_queued if n_queued > 0 else len(scraped_pool)
        accounts_needed = plan_adder_accounts(
            n_members, len(active_accounts), MEMBERS_PER_ACCOUNT,
        )
        if requested_workers is not None and requested_workers > 0:
            accounts_needed = min(accounts_needed, requested_workers)

        _pipeline_cap = LIVE_CAP
        _adder_configured_limit = min(
            int(CONFIG.get("ADDER_MAX_WORKER_SESSIONS", 90)),
            _pipeline_cap,
            LIVE_CAP,
        )
        if requested_workers is not None and requested_workers > 0:
            _adder_configured_limit = min(_adder_configured_limit, requested_workers)
        _adder_available_proxies = 0
        if self.proxy_lease_manager is not None:
            try:
                _adder_available_proxies = max(
                    0, self.proxy_lease_manager.usable_available_count()
                )
            except Exception:
                _adder_available_proxies = 0
        if accounts_needed <= 0:
            MAX_LIVE_ACCOUNTS = 0
        elif _adder_available_proxies > 0:
            MAX_LIVE_ACCOUNTS = max(
                1,
                min(accounts_needed, _adder_configured_limit, _adder_available_proxies),
            )
        else:
            MAX_LIVE_ACCOUNTS = max(1, min(accounts_needed, _adder_configured_limit))

        work_accounts = list(active_accounts[:accounts_needed])
        spare_accounts: deque = deque(active_accounts[accounts_needed:])
        remaining_q: deque = deque(work_accounts)
        batches_required = (
            (accounts_needed + BATCH_SIZE - 1) // BATCH_SIZE if accounts_needed else 0
        )
        logger.info(
            "ADDER_POOL_SIZING | members=%s | per_account=%s | accounts_needed=%s/%s | "
            "unused_spares=%s | batches_required=%s | usable_proxies=%s | "
            "batch_size=%s | concurrent_batches=%s | live_cap=%s | "
            "configured_limit=%s requested=%s -> max_live=%s",
            n_members, MEMBERS_PER_ACCOUNT, accounts_needed, len(active_accounts),
            len(spare_accounts), batches_required, _adder_available_proxies,
            BATCH_SIZE, MAX_CONCURRENT_BATCHES, LIVE_CAP,
            _adder_configured_limit, requested_workers, MAX_LIVE_ACCOUNTS,
        )

        if self.adder_state:
            self.adder_state.total_target = n_queued or n_members
            self.adder_state.max_workers = MAX_LIVE_ACCOUNTS
            self.adder_state.active_workers = 0
            if not getattr(self.adder_state, "resting", None):
                self.adder_state.resting = []

        @asynccontextmanager
        async def account_context(acc_doc: dict, failure: Dict[str, str]) -> AsyncIterator[Optional[dict]]:
            phone = str(acc_doc.get("phone", "")).strip()
            clean_phone = phone.replace("+", "")
            failure["reason"] = "unknown"
            yielded = False

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
                    # Join the target first, then settle, then add.
                    # Never invite without a successful join/resolve.
                    # ------------------------------------------
                    target_entity = None
                    already_in = False
                    join_exc = None

                    try:
                        if is_private:
                            invite_info = await client(CheckChatInviteRequest(resolved_token))
                            if type(invite_info).__name__ == "ChatInviteAlready":
                                target_entity = invite_info.chat
                                already_in = True
                            else:
                                updates = await client(ImportChatInviteRequest(resolved_token))
                                if getattr(updates, "chats", None):
                                    target_entity = updates.chats[0]
                                else:
                                    invite_info = await client(CheckChatInviteRequest(resolved_token))
                                    target_entity = getattr(invite_info, "chat", None)
                        else:
                            await client(JoinChannelRequest(resolved_token))
                    except UserAlreadyParticipantError:
                        already_in = True
                        if is_private:
                            try:
                                invite_info = await client(CheckChatInviteRequest(resolved_token))
                                target_entity = getattr(invite_info, "chat", None)
                            except Exception as exc:
                                join_exc = exc
                    except Exception as exc:
                        join_exc = exc

                    if join_exc is not None:
                        if _is_target_chat_stop(join_exc):
                            failure["reason"] = f"chat_stop: {type(join_exc).__name__}"
                            logger.warning(
                                "ADDER_JOIN_CHAT_STOP | phone=%s | err=%s | not inviting",
                                phone, type(join_exc).__name__,
                            )
                            yield None
                            return
                        logger.warning(
                            "ADDER_TARGET_PREPARE_FAILED | phone=%s | error=%s",
                            phone, join_exc,
                        )
                        raise join_exc

                    if target_entity is None:
                        if is_private:
                            invite_info = await client(CheckChatInviteRequest(resolved_token))
                            target_entity = getattr(invite_info, "chat", None)
                            if target_entity is None:
                                raise ValueError("Could not resolve private invite entity")
                        else:
                            target_entity = await client.get_entity(resolved_token)

                    if not hasattr(target_entity, "access_hash"):
                        raise ValueError("Target entity has no access_hash")

                    target_peer = InputPeerChannel(target_entity.id, target_entity.access_hash)

                    settle = random.uniform(*JOIN_SETTLE_DELAY)
                    logger.info(
                        "ADDER_JOIN_OK | account=%s | already=%s | settle=%.1fs then add",
                        clean_phone, already_in, settle,
                    )
                    if settle > 0:
                        if self.adder_state:
                            self.adder_state.total_delay_sum += settle
                        await asyncio.sleep(settle)

                    yielded = True
                    yield {
                        "phone": phone,
                        "clean_phone": clean_phone,
                        "client": client,
                        "target_peer": target_peer,
                        "lease": lease,
                        "proxy_url": lease.proxy_url or "",
                        "proxy_id": lease.proxy_id or "",
                        "burst_count": 0,
                    }

            except SessionAlreadyOwnedError:
                if yielded:
                    raise
                # Routine contention — NOT downtime; the worker may retry.
                self.session_contention += 1
                failure["reason"] = "busy"
                logger.debug("ADDER_SESSION_BUSY | phone=%s", phone)
                yield None

            except Exception as exc:
                if yielded:
                    raise
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

        deferred_accounts: List[dict] = []
        queue_lock = asyncio.Lock()
        blocked_phones: Set[str] = set()

        def _phone_key(value: Any) -> str:
            if isinstance(value, dict):
                value = value.get("clean_phone") or value.get("phone") or ""
            return str(value).strip().replace(" ", "").replace("+", "")

        def _is_blocked(doc_or_phone: Any) -> bool:
            key = _phone_key(doc_or_phone)
            return bool(key) and key in blocked_phones

        async def _block_for_run(phone: str) -> None:
            key = _phone_key(phone)
            if not key:
                return
            async with queue_lock:
                blocked_phones.add(key)
                kept_rem = deque(d for d in remaining_q if _phone_key(d) != key)
                remaining_q.clear()
                remaining_q.extend(kept_rem)
                deferred_accounts[:] = [
                    d for d in deferred_accounts if _phone_key(d) != key
                ]
                kept_sp = deque(d for d in spare_accounts if _phone_key(d) != key)
                spare_accounts.clear()
                spare_accounts.extend(kept_sp)

        def _note_skip() -> None:
            self.total_skipped += 1
            if self.adder_state:
                self.adder_state.skipped += 1

        def _note_park() -> None:
            if self.adder_state:
                self.adder_state.parked = int(getattr(self.adder_state, "parked", 0) or 0) + 1

        async def _rest_account_24h(phone: str, err_name: str) -> None:
            await _block_for_run(phone)
            note = f"ADDER_REST_24H | {err_name}"
            try:
                park_async = getattr(self.db, "park_module_rest_async", None)
                if callable(park_async):
                    await park_async(phone, "adder", note, 24.0)
                else:
                    park_sync = getattr(self.db, "park_module_rest", None)
                    if callable(park_sync):
                        await asyncio.to_thread(park_sync, phone, "adder", note, 24.0)
                    else:
                        # Older DBs: fall back to shared spam_until (tests).
                        park_legacy = getattr(self.db, "park_spam_limited_async", None)
                        if callable(park_legacy):
                            await park_legacy(phone, note, 24.0)
                        else:
                            park_legacy_sync = getattr(self.db, "park_spam_limited", None)
                            if callable(park_legacy_sync):
                                await asyncio.to_thread(park_legacy_sync, phone, note, 24.0)
            except Exception as rest_exc:
                logger.warning(
                    "ADDER_REST_24H_FAILED | account=%s | %s", phone, rest_exc,
                )
            until_str = (datetime.now() + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M")
            display = f"+{phone} unavailable (24h rest) until {until_str} · {err_name}"
            if self.adder_state is not None:
                resting = getattr(self.adder_state, "resting", None)
                if resting is None:
                    self.adder_state.resting = []
                    resting = self.adder_state.resting
                resting.append(display)
            logger.warning("ADDER_REST_24H | %s", display)
            try:
                await update_callback(
                    f"⏸️ Account +{phone} unavailable for adding · 24h rest · "
                    f"{err_name} · until {until_str}"
                )
            except Exception:
                pass

        async def run_one_account(account_doc: dict) -> Tuple[int, bool]:
            added_here = 0
            needs_replace = False
            if not self.is_running:
                return 0, False
            if _is_blocked(account_doc):
                logger.info(
                    "ADDER_SKIP_RESTING | account=%s | already on 24h rest / parked this run",
                    _phone_key(account_doc),
                )
                return 0, True

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
                        if reason.startswith("chat_stop"):
                            self.chat_parks += 1
                            _note_park()
                            await _block_for_run(key)
                            logger.warning(
                                "ADDER_ACCOUNT_SKIP_CHAT | account=%s | %s | parked this target (not dead)",
                                key, reason,
                            )
                            return 0, True
                        if _is_blocked(key):
                            logger.info(
                                "ADDER_SKIP_RESTING | account=%s | %s",
                                key, reason,
                            )
                            return 0, True
                        try:
                            getter = getattr(self.db, "get_session_by_phone_async", None)
                            rec = await getter(key) if callable(getter) else None
                            if rec and (
                                is_spam_park_active(rec)
                                or is_module_rest_active(rec, "adder")
                            ):
                                await _block_for_run(key)
                                logger.info(
                                    "ADDER_SKIP_RESTING | account=%s | spam_until active",
                                    key,
                                )
                                return 0, True
                        except Exception:
                            pass
                        if reason.startswith("failed") or not self.is_running:
                            logger.warning(
                                f"ADDER_ACCOUNT_DROPPED | account={key} | reason={reason}"
                            )
                            return 0, True
                        count = account_retry.get(key, 0) + 1
                        no_proxy = reason == "no_proxy"
                        max_attempts = NO_PROXY_LEASE_ATTEMPTS if no_proxy else ADDER_LEASE_ATTEMPTS
                        if count <= max_attempts:
                            account_retry[key] = count
                            async with queue_lock:
                                deferred_accounts.append(account_doc)
                            logger.info(
                                f"ADDER_LEASE_RETRY | account={key} | attempt={count}/"
                                f"{max_attempts} | reason={reason} | requeued"
                            )
                            if no_proxy:
                                await asyncio.sleep(random.uniform(*NO_PROXY_RETRY_DELAY))
                            else:
                                await asyncio.sleep(random.uniform(*LEASE_RETRY_DELAY))
                            return 0, False
                        logger.warning(
                            f"ADDER_ACCOUNT_SKIPPED | account={key} | no lease after "
                            f"{max_attempts} retries | reason={reason}"
                        )
                        return 0, True

                    # Account acquired a live session and reached the
                    # invite stage — used to detect "all accounts flood".
                    self.accounts_acquired += 1

                    try:
                        pending_retry = None
                        while self.is_running:
                            is_restriction_retry = False
                            if pending_retry is not None:
                                member = pending_retry
                                pending_retry = None
                                is_restriction_retry = True
                            else:
                                try:
                                    member = members_queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    break

                            uname = str(member.get("username", "")).strip()
                            uid = str(member.get("user_id", "")).strip()
                            access_hash = str(member.get("access_hash", "0")).strip()
                            identity = uid if (uid and uid not in ("None", "0")) else uname
                            hash_ok = bool(access_hash and access_hash not in ("0", "None"))
                            has_username = bool(uname and uname not in ("None", ""))

                            try:
                                if uid and hash_ok:
                                    target_user = InputPeerUser(int(uid), int(access_hash))
                                elif has_username:
                                    target_user = await worker_account["client"].get_input_entity(uname)
                                else:
                                    _note_skip()
                                    await asyncio.to_thread(
                                        self.db.log_addition_state, uid, uname, "invalid_identity")
                                    continue

                                api_start_time = time.time()
                                await worker_account["client"](
                                    InviteToChannelRequest(worker_account["target_peer"], [target_user])
                                )
                                api_delay = time.time() - api_start_time

                                self.total_added += 1
                                added_here += 1
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
                                _note_skip()
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "privacy_restricted")

                                sleep_time = random.uniform(3, 6)
                                if self.adder_state:
                                    self.adder_state.total_delay_sum += sleep_time
                                await asyncio.sleep(sleep_time)

                            except UserAlreadyParticipantError:
                                _note_skip()
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "already_member")

                                sleep_time = random.uniform(1.5, 3.5)
                                if self.adder_state:
                                    self.adder_state.total_delay_sum += sleep_time
                                await asyncio.sleep(sleep_time)

                            except FloodWaitError as fl_err:
                                seconds = int(getattr(fl_err, "seconds", 0) or 0)
                                await members_queue.put(member)
                                if 0 < seconds <= SHORT_FLOOD_WAIT:
                                    wait = seconds + random.uniform(1, 5)
                                    logger.warning(
                                        "ADDER_FLOOD_WAIT | account=%s | sleep=%.0fs then retry same account",
                                        worker_account["clean_phone"], wait,
                                    )
                                    if self.adder_state:
                                        self.adder_state.total_delay_sum += wait
                                    await asyncio.sleep(wait)
                                    continue
                                self.flood_drops += 1
                                _note_park()
                                needs_replace = True
                                await _block_for_run(worker_account["clean_phone"])
                                logger.warning(
                                    "ADDER_FLOOD_STOP | account=%s | proxy_node=%s | err=FloodWaitError(%ss) | account parked this run (not marked failed, lease released)",
                                    worker_account["clean_phone"],
                                    worker_account.get("proxy_id", "?"),
                                    seconds,
                                )
                                break

                            except FLOOD_STOP_ERRORS as fl_err:
                                err_name = type(fl_err).__name__
                                if not is_restriction_retry:
                                    logger.info(
                                        "ADDER_RESTRICT_RETRY | account=%s | err=%s | retrying this add once",
                                        worker_account["clean_phone"], err_name,
                                    )
                                    pending_retry = member
                                    continue
                                await members_queue.put(member)
                                if _is_user_banned_in_channel(fl_err):
                                    self.chat_parks += 1
                                    if worker_account.get("burst_count", 0) == 0:
                                        self._first_invite_chat_bans += 1
                                else:
                                    self.flood_drops += 1
                                _note_park()
                                needs_replace = True
                                logger.warning(
                                    "ADDER_RESTRICT_STOP | account=%s | proxy_node=%s | err=%s | "
                                    "member requeued, account 24h rest (not marked failed)",
                                    worker_account["clean_phone"],
                                    worker_account.get("proxy_id", "?"),
                                    err_name,
                                )
                                await _rest_account_24h(
                                    worker_account["clean_phone"], err_name,
                                )
                                break

                            except (UserIdInvalidError, ValueError):
                                _note_skip()
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "invalid_identity")
                                continue

                            except UserNotMutualContactError:
                                _note_skip()
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "not_mutual_contact")
                                sleep_time = random.uniform(1.5, 3.5)
                                if self.adder_state:
                                    self.adder_state.total_delay_sum += sleep_time
                                await asyncio.sleep(sleep_time)
                                continue

                            except asyncio.CancelledError:
                                try:
                                    await members_queue.put(member)
                                except Exception:
                                    pass
                                raise

                            except TelethonRPCError as rpc_err:
                                if _is_chat_member_add_failed(rpc_err):
                                    _note_skip()
                                    await asyncio.to_thread(
                                        self.db.log_addition_state, uid, uname, "add_failed")
                                    logger.info(
                                        "ADDER_MEMBER_SKIP | CHAT_MEMBER_ADD_FAILED | identity=%s",
                                        identity,
                                    )
                                    sleep_time = random.uniform(1.5, 3.5)
                                    if self.adder_state:
                                        self.adder_state.total_delay_sum += sleep_time
                                    await asyncio.sleep(sleep_time)
                                    continue
                                if _is_add_restriction_error(rpc_err):
                                    err_name = type(rpc_err).__name__
                                    if not is_restriction_retry:
                                        logger.info(
                                            "ADDER_RESTRICT_RETRY | account=%s | err=%s | retrying this add once",
                                            worker_account["clean_phone"], err_name,
                                        )
                                        pending_retry = member
                                        continue
                                    await members_queue.put(member)
                                    self.chat_parks += 1
                                    _note_park()
                                    needs_replace = True
                                    if worker_account.get("burst_count", 0) == 0:
                                        self._first_invite_chat_bans += 1
                                    logger.warning(
                                        "ADDER_RESTRICT_STOP | account=%s | err=%s | "
                                        "member requeued, account 24h rest (not marked failed)",
                                        worker_account["clean_phone"], err_name,
                                    )
                                    await _rest_account_24h(
                                        worker_account["clean_phone"], err_name,
                                    )
                                    break
                                if _is_target_chat_stop(rpc_err):
                                    await members_queue.put(member)
                                    self.chat_parks += 1
                                    _note_park()
                                    needs_replace = True
                                    await _block_for_run(worker_account["clean_phone"])
                                    if worker_account.get("burst_count", 0) == 0:
                                        self._first_invite_chat_bans += 1
                                    logger.warning(
                                        "ADDER_ACCOUNT_SKIP_CHAT | account=%s | err=%s | parked this target (not dead)",
                                        worker_account["clean_phone"],
                                        type(rpc_err).__name__,
                                    )
                                    break
                                _note_skip()
                                await asyncio.to_thread(
                                    self.db.log_addition_state, uid, uname, "rpc_skip")
                                logger.info(
                                    "ADDER_MEMBER_SKIP | rpc=%s | identity=%s",
                                    type(rpc_err).__name__, identity,
                                )
                                continue

                            except Exception as crash:
                                if _is_chat_member_add_failed(crash):
                                    _note_skip()
                                    await asyncio.to_thread(
                                        self.db.log_addition_state, uid, uname, "add_failed")
                                    logger.info(
                                        "ADDER_MEMBER_SKIP | CHAT_MEMBER_ADD_FAILED | identity=%s",
                                        identity,
                                    )
                                    continue
                                if _is_add_restriction_error(crash):
                                    err_name = type(crash).__name__
                                    if not is_restriction_retry:
                                        logger.info(
                                            "ADDER_RESTRICT_RETRY | account=%s | err=%s | retrying this add once",
                                            worker_account["clean_phone"], err_name,
                                        )
                                        pending_retry = member
                                        continue
                                    await members_queue.put(member)
                                    if _is_user_banned_in_channel(crash):
                                        self.chat_parks += 1
                                    else:
                                        self.flood_drops += 1
                                    _note_park()
                                    needs_replace = True
                                    if worker_account.get("burst_count", 0) == 0:
                                        self._first_invite_chat_bans += 1
                                    logger.warning(
                                        "ADDER_RESTRICT_STOP | account=%s | err=%s | "
                                        "member requeued, account 24h rest (not marked failed)",
                                        worker_account["clean_phone"], err_name,
                                    )
                                    await _rest_account_24h(
                                        worker_account["clean_phone"], err_name,
                                    )
                                    break
                                if _is_target_chat_stop(crash):
                                    await members_queue.put(member)
                                    self.chat_parks += 1
                                    _note_park()
                                    needs_replace = True
                                    await _block_for_run(worker_account["clean_phone"])
                                    if worker_account.get("burst_count", 0) == 0:
                                        self._first_invite_chat_bans += 1
                                    logger.warning(
                                        "ADDER_ACCOUNT_SKIP_CHAT | account=%s | err=%s | parked this target (not dead)",
                                        worker_account["clean_phone"],
                                        type(crash).__name__,
                                    )
                                    break
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
                                    needs_replace = True
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

                                # Transient error: requeue the member. Retryable
                                # network/timeout errors also requeue the account
                                # (bounded) instead of dropping it for the whole run.
                                await members_queue.put(member)
                                sleep_time = random.uniform(8, 12)
                                if self.adder_state:
                                    self.adder_state.total_delay_sum += sleep_time
                                await asyncio.sleep(sleep_time)
                                try:
                                    worker_account["lease"].proxy_should_cooldown = True
                                except Exception:
                                    pass
                                if result.retryable:
                                    key = str(account_doc.get("phone", "")).strip()
                                    count = account_retry.get(key, 0) + 1
                                    if count <= ADDER_LEASE_ATTEMPTS:
                                        account_retry[key] = count
                                        async with queue_lock:
                                            deferred_accounts.append(account_doc)
                                        logger.info(
                                            "ADDER_ACCOUNT_RETRY_TRANSIENT | account=%s | attempt=%s/%s | err=%s",
                                            worker_account["clean_phone"], count,
                                            ADDER_LEASE_ATTEMPTS, str(crash)[:80],
                                        )
                                        break
                                logger.info(
                                    "ADDER_ACCOUNT_DROPPED_TRANSIENT | account=%s | err=%s",
                                    worker_account["clean_phone"], str(crash)[:80],
                                )
                                needs_replace = True
                                break
                    finally:
                        # Closes the member-processing try for this account.
                        pass
            except asyncio.CancelledError:
                raise
            except Exception as worker_exc:
                logger.warning(
                    "ADDER_WORKER_SURVIVED | account=%s | err=%s",
                    str(account_doc.get("phone", "")).strip(),
                    worker_exc,
                )
                if self.adder_state:
                    self.adder_state.failures += 1
                needs_replace = True
            finally:
                if self.adder_state:
                    self.adder_state.active_workers = max(
                        0,
                        self.adder_state.active_workers - 1,
                    )
            return added_here, needs_replace

        # Pipelined batches: each batch is BATCH_SIZE accounts. Up to
        # MAX_CONCURRENT_BATCHES run at once. As soon as batch A is running,
        # batch B is armed in the background. New batches stop when live
        # accounts hit the proxy / max_live ceiling. Each account keeps
        # inviting until Telegram returns a stopping error.
        self.active_workers = []
        self._batch_tasks: List[asyncio.Task] = []
        accounts_in_flight = 0
        in_flight_lock = asyncio.Lock()

        async def _pending_accounts() -> bool:
            async with queue_lock:
                if any(not _is_blocked(d) for d in deferred_accounts):
                    return True
                if any(not _is_blocked(d) for d in remaining_q):
                    return True
                return False

        async def _pop_wave(max_n: int) -> List[dict]:
            async with queue_lock:
                wave: List[dict] = []
                while len(wave) < max_n:
                    doc = None
                    if deferred_accounts:
                        doc = deferred_accounts.pop(0)
                    elif remaining_q:
                        doc = remaining_q.popleft()
                    else:
                        break
                    if _is_blocked(doc):
                        continue
                    wave.append(doc)
                return wave

        async def _take_replacement() -> Optional[dict]:
            async with queue_lock:
                while spare_accounts:
                    doc = spare_accounts.popleft()
                    if not _is_blocked(doc):
                        return doc
                while remaining_q:
                    doc = remaining_q.popleft()
                    if not _is_blocked(doc):
                        return doc
                while deferred_accounts:
                    doc = deferred_accounts.pop(0)
                    if not _is_blocked(doc):
                        return doc
                return None

        async def run_slot(account_doc: dict) -> int:
            added = 0
            current: Optional[dict] = account_doc
            while current is not None and self.is_running:
                n, needs_replace = await run_one_account(current)
                added += n
                if not needs_replace or not self.is_running:
                    break
                if members_queue.empty():
                    break
                nxt = await _take_replacement()
                if nxt is None:
                    break
                logger.info(
                    "ADDER_ACCOUNT_REPLACE | rested=%s | next=%s | members_left=%s",
                    str(current.get("phone", "")).strip(),
                    str(nxt.get("phone", "")).strip(),
                    members_queue.qsize(),
                )
                try:
                    await update_callback(
                        f"🔁 Replaced +{current.get('phone', '')} with "
                        f"+{nxt.get('phone', '')} (24h rest / stop)"
                    )
                except Exception:
                    pass
                current = nxt
            return added

        async def run_batch(wave: List[dict], label: str) -> None:
            logger.info(
                "📦 Batch %s starting | accounts=%s | members_left=%s",
                label, len(wave), members_queue.qsize(),
            )
            try:
                await update_callback(
                    f"📦 Batch {label} started | accounts={len(wave)}"
                )
            except Exception:
                pass
            tasks = []
            for i, doc in enumerate(wave):
                if not self.is_running:
                    break
                if i:
                    delay = random.uniform(*ACCOUNT_LAUNCH_DELAY)
                    if delay > 0:
                        await asyncio.sleep(delay)
                task = asyncio.create_task(run_slot(doc))
                tasks.append(task)
                self.active_workers.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
            added_this_batch = 0
            for result in results:
                if isinstance(result, int):
                    added_this_batch += result
            logger.info(
                "📦 Batch %s added %s members (campaign total=%s)",
                label, added_this_batch, self.total_added,
            )
            self.batch_reports.append({
                "label": label,
                "added": added_this_batch,
                "accounts": len(wave),
            })
            try:
                await update_callback(
                    f"📦 Batch {label} added {added_this_batch} members "
                    f"(campaign total={self.total_added})"
                )
            except Exception:
                pass

        async def dispatch_batches() -> None:
            nonlocal accounts_in_flight
            while self.is_running:
                live = [t for t in self._batch_tasks if not t.done()]
                self._batch_tasks = live

                if self._stop_new_batches:
                    if live:
                        await asyncio.gather(*live, return_exceptions=True)
                    break

                if members_queue.empty() and self._batch_index > 0:
                    if live:
                        await asyncio.gather(*live, return_exceptions=True)
                    break

                pending = await _pending_accounts()
                if not pending:
                    if live:
                        await asyncio.wait(live, return_when=asyncio.FIRST_COMPLETED)
                        continue
                    break

                at_batch_cap = len(live) >= MAX_CONCURRENT_BATCHES
                at_proxy_cap = accounts_in_flight >= MAX_LIVE_ACCOUNTS
                if at_batch_cap or at_proxy_cap:
                    if not live:
                        break
                    await asyncio.wait(live, return_when=asyncio.FIRST_COMPLETED)
                    continue

                room = MAX_LIVE_ACCOUNTS - accounts_in_flight
                wave = await _pop_wave(min(BATCH_SIZE, room))
                if not wave:
                    if live:
                        await asyncio.wait(live, return_when=asyncio.FIRST_COMPLETED)
                        continue
                    break

                label = _batch_letter(self._batch_index)
                self._batch_index += 1
                async with in_flight_lock:
                    accounts_in_flight += len(wave)
                wave_size = len(wave)

                async def _guarded(_wave=wave, _label=label, _n=wave_size) -> None:
                    nonlocal accounts_in_flight
                    try:
                        await run_batch(_wave, _label)
                    finally:
                        async with in_flight_lock:
                            accounts_in_flight = max(0, accounts_in_flight - _n)

                task = asyncio.create_task(_guarded())
                self._batch_tasks.append(task)
                # Yield so batch A actually starts running before we arm B.
                await asyncio.sleep(0)

            leftover = [t for t in self._batch_tasks if not t.done()]
            if leftover:
                await asyncio.gather(*leftover, return_exceptions=True)

        try:
            await dispatch_batches()
        except asyncio.CancelledError:
            pending = [
                t for t in list(self.active_workers) + list(self._batch_tasks)
                if t is not None and not t.done()
            ]
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        remaining_members = members_queue.qsize()
        if self.adder_state:
            if remaining_members:
                self.adder_state.stop("Stopped")
            else:
                self.adder_state.stop("Completed")

        parked = int(self.chat_parks) + int(self.flood_drops)
        skipped = int(self.total_skipped)
        summary_tail = (
            f"✅ Added: {self.total_added} • ⏭️ Skipped: {skipped} • "
            f"🅿️ Parked: {parked} • 💀 Dead: {self.accounts_down}"
        )

        if (
            self.accounts_acquired > 0
            and self.total_added == 0
            and self.flood_drops >= self.accounts_acquired
        ):
            return (
                f"🚫 All {self.flood_drops} accounts are flood-limited by Telegram (PeerFlood)\n"
                f"⏳ Wait 12–24h, then try `/addmembers` again"
            )

        if self._stop_new_batches and remaining_members:
            return (
                f"⚠️ Stopped: this target banned further invites "
                f"({self.chat_parks} accounts parked for this group)\n"
                f"{summary_tail} • ⏳ Remaining: {remaining_members}"
            )

        if remaining_members and (self.accounts_down + parked) >= max(1, self.accounts_acquired):
            return (
                f"⚠️ Stopped with members remaining • {summary_tail} • "
                f"⏳ Remaining: {remaining_members}"
            )

        if remaining_members:
            return (
                f"⚠️ Stopped with members remaining • {summary_tail} • "
                f"⏳ Remaining: {remaining_members}"
            )

        return f"{summary_tail}"

    def halt_engine(self):
        """Kills active loop variables instantly safely."""
        self.is_running = False
        if hasattr(self, 'adder_state') and self.adder_state:
            self.adder_state.stop()
        if hasattr(self, 'active_workers'):
            for worker in self.active_workers:
                worker.cancel()
        if hasattr(self, '_batch_tasks'):
            for task in self._batch_tasks:
                task.cancel()