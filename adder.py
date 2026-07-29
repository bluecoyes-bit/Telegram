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

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser
from telethon.errors import (
    UserPrivacyRestrictedError, UserAlreadyParticipantError,
    FloodWaitError, PeerFloodError, UserIdInvalidError
)

from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
from proxy_manager import RobustProxyManager
from scraper import MemberScraper

logger = logging.getLogger("SuiteAdder")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

class EnterpriseMemberAdder:
    """Manages multi-account smart rotation loops, safe bursts padding, and anti-ban tracking matrix."""
    
    def __init__(self, db: SuiteDatabase, proxy_manager: Optional[RobustProxyManager] = None):
        self.db = db
        self.proxy_manager = proxy_manager
        self.scraper_helper = MemberScraper(db)
        self.is_running = False
        
        # Telemetry metrics trace trackers
        self.total_added = 0
        self.accounts_down = 0
        self.privacy_skips = 0

    async def execute_adding_pipeline(self, target_group_link: str, update_callback) -> str:
        """
        Executes structural lookups from Scraped DB pool, starts multiple account workers,
        and updates progress states back to the live central Telegram Bot UI dashboard.
        """
        self.is_running = True
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
        for member in scraped_pool:
            await members_queue.put(member)

        accounts_queue = asyncio.Queue()
        for acc_doc in active_accounts:
            await accounts_queue.put(acc_doc)

        async def initialize_account(acc_doc: dict):
            phone = str(acc_doc.get("phone"))
            self.db.acquire_lock(phone) # 🔒 Lock account instantly so auditor ignores it
            
            session_str = acc_doc.get("session_string") or acc_doc.get("session")
            # Use permanent device metadata if available
            device = acc_doc.get("device_metadata") or random.choice(DEVICE_PROFILES)
            
            # Proxy allocation check integration
            proxy_node = None
            if self.proxy_manager and self.proxy_manager.working_count > 0:
                proxy_node = self.proxy_manager.get_secured_proxy()

            client = TelegramClient(
                StringSession(session_str),
                int(acc_doc.get("api_id", CONFIG["API_ID"])),
                str(acc_doc.get("api_hash", CONFIG["API_HASH"])),
                device_model=device.get("device_model", "PC 64bit"),
                system_version=device.get("system_version", "Windows 11"),
                app_version=device.get("app_version", "4.8.4"),
                proxy=proxy_node
            )

            try:
                await client.connect()
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
                self.db.release_lock(phone) # 🔓 Unlock immediately if initialization fails
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
                            continue

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
                            self.db.log_addition_state(uid, uname, "invalid_identity")
                            continue

                        await worker_account["client"](InviteToChannelRequest(worker_account["target_peer"], [target_user]))
                        self.total_added += 1
                        worker_account["burst_count"] += 1
                        self.db.log_addition_state(uid, uname, "success_added")

                        if self.total_added % PROGRESS_UPDATE_INTERVAL == 0:
                            await update_callback(f"📊 **Live Tracking:** `{self.total_added}` members added.")

                        if worker_account["burst_count"] >= BURST_ADD_LIMIT:
                            await asyncio.sleep(random.uniform(*BURST_COOLDOWN_TIME))
                            worker_account["burst_count"] = 0
                        else:
                            await asyncio.sleep(random.uniform(*HUMAN_ADD_INTERVAL))

                    except UserPrivacyRestrictedError:
                        self.privacy_skips += 1
                        self.db.log_addition_state(uid, uname, "privacy_restricted")
                        await asyncio.sleep(random.uniform(3, 6))

                    except UserAlreadyParticipantError:
                        self.db.log_addition_state(uid, uname, "already_member")
                        await asyncio.sleep(random.uniform(1.5, 3.5))

                    except (PeerFloodError, FloodWaitError):
                        self.accounts_down += 1
                        await members_queue.put(member) # 🔥 Repopulate queue on drop
                        try:
                            await worker_account["client"].disconnect()
                        except Exception:
                            pass
                        self.db.release_lock(worker_account["phone"]) # 🔓 Unlock dropped account
                        worker_account = None
                        continue

                    except (UserIdInvalidError, ValueError):
                        self.db.log_addition_state(uid, uname, "invalid_identity")
                        continue

                    except Exception as crash:
                        err_msg = str(crash).lower()
                        if any(k in err_msg for k in ["banned", "deactivated", "revoked", "disabled"]):
                            self.accounts_down += 1
                            if hasattr(self.db, "mark_account_failed"):
                                self.db.mark_account_failed(worker_account["phone"], f"Banned at runtime: {str(crash)[:80]}")
                            else:
                                self.db.mark_account_revoked(worker_account["phone"], f"Banned at runtime: {str(crash)[:80]}")
                            try:
                                await worker_account["client"].disconnect()
                            except Exception:
                                pass
                            self.db.release_lock(worker_account["phone"]) # 🔓 Unlock banned account
                            worker_account = None
                            continue
                        await asyncio.sleep(random.uniform(8, 12))
            finally:
                # Loop khatam hone ke baad final cleanup
                if worker_account is not None:
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
        if hasattr(self, 'active_workers'):
            for worker in self.active_workers:
                worker.cancel()