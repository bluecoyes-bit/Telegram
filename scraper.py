#!/usr/bin/env python3
"""
Filename: scraper.py
"""

import sys
import os
import re
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import random

from telethon import TelegramClient
from telethon.tl.types import (
    UserStatusOnline, UserStatusRecently, UserStatusLastWeek,
    UserStatusLastMonth, PeerChannel, ChannelParticipantsSearch
)
from telethon.tl.functions.channels import JoinChannelRequest, GetParticipantsRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetHistoryRequest, GetFullChatRequest
from telethon.errors import FloodWaitError, ChatAdminRequiredError, UserAlreadyParticipantError
from telethon.tl.functions.phone import GetGroupParticipantsRequest

from config import CONFIG, DEVICE_PROFILES
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

logger = logging.getLogger("SuiteScraper")

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

class MemberScraper:
    """Handles universal group link decoding, filtering, and hidden participant crawling."""
    
    def __init__(self, db, proxy_manager=None, proxy_lease_manager=None,
                 session_manager=None, account_lease_manager=None):
        self.db = db
        self.proxy_manager = proxy_manager
        self.proxy_lease_manager = proxy_lease_manager
        self.session_manager = session_manager
        self.account_lease_manager = account_lease_manager

    def resolve_group_link(self, link_str: str) -> tuple[bool, str]:
        if not link_str:
            return False, ""
            
        # FIX: Aggressively sanitize any extra characters, brackets, or quotes from copy-paste
        clean_target = str(link_str).replace("<", "").replace(">", "").replace('"', '').replace("'", "").strip()
        clean_target = clean_target.rstrip("/")
        
        # Regex to capture hash from private links format
        private_pattern = re.compile(r'(?:t\.me|telegram\.me)/(?:\+|joinchat/)([a-zA-Z0-9_\-]+)')
        private_match = private_pattern.search(clean_target)
        
        if private_match:
            hash_token = private_match.group(1).strip()
            return True, hash_token
            
        # Fallback for direct hashes without domain
        if not "/" in clean_target and not clean_target.startswith("@"):
            if len(clean_target) >= 12 and re.match(r'^[a-zA-Z0-9_\-]+$', clean_target):
                return True, clean_target

        public_payload = clean_target.split("/")[-1]
        public_payload = public_payload.lstrip("@").strip()
        
        return False, public_payload

    def _determine_activity_status(self, user_obj) -> str:
        """Evaluates active timestamps to map accurate user behavior strings."""
        if not getattr(user_obj, 'status', None):
            return "Offline"
        
        status = user_obj.status
        if isinstance(status, UserStatusOnline):
            return "Online"
        elif isinstance(status, UserStatusRecently):
            return "Recently"
        elif isinstance(status, UserStatusLastWeek):
            return "LastWeek"
        elif isinstance(status, UserStatusLastMonth):
            return "LastMonth"
        return "Offline"

    def _convert_user_to_dict(self, user, status_str: str, source: str) -> Dict[str, Any]:
        """Transforms raw Telethon structures into precise data payloads matching system specs."""
        last_seen = None
        if hasattr(user, 'status'):
            if isinstance(user.status, UserStatusOnline):
                last_seen = datetime.now(timezone.utc).isoformat()
            elif hasattr(user.status, 'was_online') and user.status.was_online:
                last_seen = user.status.was_online.isoformat()

        return {
            "user_id": str(user.id),
            "access_hash": str(getattr(user, 'access_hash', '0')),
            "username": str(user.username) if user.username else "None",
            "first_name": str(user.first_name or ''),
            "last_name": str(user.last_name or ''),
            "activity_status": status_str,
            "last_seen_online": last_seen,
            "is_premium": getattr(user, 'premium', False),
            "scraped_at": int(time.time()),
            "source_group": source
        }

    async def _bind_and_join(self, client: TelegramClient, link_str: str) -> Any:
        """Enforces safe auto-joining mechanics for target group verification."""
        is_private, token = self.resolve_group_link(link_str)
        print(f"  🔄 Processing entity. Private detection flag: {is_private} | Node string: {token}")
        await asyncio.sleep(random.uniform(1.5, 3.0)) 
        
        try:
            if is_private:
                await client(ImportChatInviteRequest(token))
                print("  ✅ Successfully joined private group cluster channel!")
            else:
                await client(JoinChannelRequest(token))
                print(f"  ✅ Successfully joined public target channel: @{token}")
        except UserAlreadyParticipantError:
            print("  ℹ️ System Note: Account wrapper node already a participant in target space.")
        except Exception as e:
            print(f"  ⚠️ Membership routing notice: {str(e)[:60]}")
            
        if is_private:
            return await client.get_entity(link_str.replace("<", "").replace(">", "").replace('"', '').replace("'", "").strip())
        return await client.get_entity(token)

    async def scrape_standard_pool(self, account_doc: Dict, group_link: str, mode: str) -> int:
        """Executes standard lookup operations with active filters (all, 24h, weekly)."""
        phone = str(account_doc.get("phone", ""))

        session_str = account_doc.get("session_string") or account_doc.get("session")
        device = account_doc.get("device_metadata") or random.choice(DEVICE_PROFILES)

        async with self.session_manager.acquire(
            phone,
            module="scraper_standard",
            auto_release=True,
        ) as lease:
            if not lease:
                return 0
            client = lease.client
            try:
                if not client.is_connected():
                    await client.connect()
                entity = await self._bind_and_join(client, group_link)
                group_title = getattr(entity, 'title', 'Scraped Group')

                offset = 0
                limit = 200
                scraped_data = []
                print(f"  ⏳ Downloading parameters mapping list using strategy filter mode: [{mode}]...")

                while True:
                    participants = await client(GetParticipantsRequest(
                        entity, ChannelParticipantsSearch(''), offset, limit, hash=0
                    ))
                    if not participants.users:
                        break

                    for user in participants.users:
                        if getattr(user, 'bot', False) or not user.id:
                            continue

                        activity = self._determine_activity_status(user)

                        if mode == '24h' and activity not in ['Online', 'Recently']:
                            continue
                        if mode == 'weekly' and activity not in ['Online', 'Recently', 'LastWeek']:
                            continue

                        payload = self._convert_user_to_dict(user, activity, group_title)
                        scraped_data.append(payload)

                    offset += len(participants.users)
                    if len(participants.users) < limit:
                        break
                    await asyncio.sleep(0.2)

                upserted = await self.db.save_scraped_members(scraped_data, group_title)
                print(f"  ✅ Operations Complete. Saved/Updated {upserted} records inside DB structure.")
                return upserted

            except FloodWaitError as e:
                print(f"  ⏱️ Rate limiting triggered. Cooldown forced: {e.seconds}s.")
                return 0
            except ChatAdminRequiredError:
                print("  ❌ Administrative security clearance required to parse member lists here.")
                return 0

    async def scrape_hidden_matrix(self, account_doc: Dict, group_link: str) -> int:
        """Scans historical channels history and active live tracking streams to gather hidden participants data logs."""
        phone = str(account_doc.get("phone", ""))

        session_str = account_doc.get("session_string") or account_doc.get("session")
        device = account_doc.get("device_metadata") or random.choice(DEVICE_PROFILES)

        async with self.session_manager.acquire(
            phone,
            module="scraper_hidden",
            auto_release=True,
        ) as lease:
            if not lease:
                return 0
            client = lease.client
            try:
                if not client.is_connected():
                    await client.connect()
                entity = await self._bind_and_join(client, group_link)
                group_title = getattr(entity, 'title', 'Hidden Scraped Group')

                users_map: Dict[int, Any] = {}
                offset_id = 0
                limit = 100
                max_messages = 3000
                messages_crawled = 0

                print("  📜 Initiating comprehensive historical scan layers for hidden entities tracking...")

                while messages_crawled < max_messages:
                    history = await client(GetHistoryRequest(
                        peer=entity, offset_id=offset_id, offset_date=None,
                        add_offset=0, limit=limit, max_id=0, min_id=0, hash=0
                    ))

                    if not history.messages:
                        break

                    for msg in history.messages:
                        if msg.from_id and not isinstance(msg.from_id, PeerChannel):
                            try:
                                sender_id = msg.from_id.user_id
                                if sender_id not in users_map:
                                    user_entity = await client.get_entity(sender_id)
                                    if not getattr(user_entity, 'bot', False):
                                        users_map[sender_id] = user_entity
                            except Exception:
                                pass

                        if getattr(msg, 'action', None):
                            action_users = []
                            if hasattr(msg.action, 'users'):
                                action_users = msg.action.users
                            elif hasattr(msg.action, 'user_id'):
                                action_users = [msg.action.user_id]

                            for u_id in action_users:
                                if u_id not in users_map:
                                    try:
                                        u_ent = await client.get_entity(u_id)
                                        if not getattr(u_ent, 'bot', False):
                                            users_map[u_id] = u_ent
                                    except Exception:
                                        pass

                    offset_id = history.messages[-1].id
                    messages_crawled += len(history.messages)
                    await asyncio.sleep(0.1)

                # Parse active live call arrays
                try:
                    if type(entity).__name__ == 'Channel':
                        full_chat_context = await client(GetFullChannelRequest(entity))
                    else:
                        full_chat_context = await client(GetFullChatRequest(entity.id))

                    if full_chat_context.full_chat.call and hasattr(full_chat_context.full_chat.call, 'participants'):
                        print(f"  🎥 Live Voicechat stream framework matching active instances...")
                        for participant in full_chat_context.full_chat.call.participants:
                            if hasattr(participant, 'peer') and hasattr(participant.peer, 'user_id'):
                                u_id = participant.peer.user_id
                                if u_id not in users_map:
                                    try:
                                        u_ent = await client.get_entity(u_id)
                                        if not getattr(u_ent, 'bot', False):
                                                    users_map[u_id] = u_ent
                                    except Exception:
                                        pass
                except Exception:
                    pass

                # Scan group admins list safely
                try:
                    admins = await client.get_participants(entity, aggressive=True)
                    for admin in admins:
                        if hasattr(admin, 'id') and not getattr(admin, 'bot', False):
                            if admin.id not in users_map:
                                users_map[admin.id] = admin
                except Exception:
                    pass

                hidden_payloads = []
                for usr in users_map.values():
                    act = self._determine_activity_status(usr)
                    hidden_payloads.append(self._convert_user_to_dict(usr, act, group_title))

                upsert_count = await self.db.save_scraped_members(hidden_payloads, group_title)
                print(f"  ✅ Extraction Matrix Complete. Processed {upsert_count} fields inside database logs.")
                return upsert_count

            except Exception as e:
                logger.error(f"Hidden compiler structural failure occurred: {e}")
                return 0
            
    async def scrape_voicechat_matrix(self, account_doc: Dict, group_link: str) -> int:
        """Scans the active live Voice Chat (Group Call) to extract all currently connected members."""
        phone = str(account_doc.get("phone", ""))

        session_str = account_doc.get("session_string") or account_doc.get("session")
        device = account_doc.get("device_metadata") or random.choice(DEVICE_PROFILES)

        async with self.session_manager.acquire(
            phone,
            module="scraper_voicechat",
            auto_release=True,
        ) as lease:
            if not lease:
                return 0
            client = lease.client
            try:
                if not client.is_connected():
                    await client.connect()
                entity = await self._bind_and_join(client, group_link)
                group_title = getattr(entity, 'title', 'VC Scraped Group')

                print("  🎙️ Checking for an active Voice Chat in the target group...")

                # 🔥 FIX: Safely fetch the full chat context using native Telethon APIs to avoid AttributeError
                from telethon.tl.functions.channels import GetFullChannelRequest
                from telethon.tl.functions.messages import GetFullChatRequest

                if type(entity).__name__ == 'Channel':
                    full_chat_context = await client(GetFullChannelRequest(entity))
                else:
                    full_chat_context = await client(GetFullChatRequest(entity.id))

                call_obj = full_chat_context.full_chat.call

                if not call_obj:
                    print("  ❌ No active Voice Chat found in this group.")
                    return 0

                print("  🎧 Active Voice Chat detected! Extracting live participants...")

                users_map: Dict[int, Any] = {}
                offset = ""

                while True:
                    try:
                        # Fetch participants from the active group call
                        from telethon.tl.functions.phone import GetGroupParticipantsRequest
                        result = await client(GetGroupParticipantsRequest(
                            call=call_obj,
                            ids=[],
                            sources=[],
                            offset=offset,
                            limit=100
                        ))

                        for user in result.users:
                            if not getattr(user, 'bot', False) and user.id:
                                users_map[user.id] = user

                        if not result.next_offset:
                            break
                        offset = result.next_offset
                        await asyncio.sleep(0.5)  # Safe delay to prevent rate limits

                    except Exception as api_err:
                        print(f"  ⚠️ Voice chat extraction pagination error: {api_err}")
                        break

                hidden_payloads = []
                for usr in users_map.values():
                    act = self._determine_activity_status(usr)
                    hidden_payloads.append(self._convert_user_to_dict(usr, act, group_title))

                if hidden_payloads:
                    upsert_count = await self.db.save_scraped_members(hidden_payloads, group_title)
                    print(f"  ✅ VoiceChat Matrix Complete. Processed {upsert_count} live participants.")
                    return upsert_count
                else:
                    print("  📭 Voice chat is active, but no visible participants found.")
                    return 0

            except Exception as e:
                logger.error(f"VoiceChat extraction structural failure occurred: {e}")
                return 0