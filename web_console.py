#!/usr/bin/env python3
"""
Ultimate Enterprise Telegram Suite - Pure Backend API Core
Filename: web_console.py
"""

import io
import base64
import os
import re
import logging
import asyncio
from typing import Optional
import datetime
from collections import OrderedDict
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.errors import UserAlreadyParticipantError, AuthKeyDuplicatedError
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.contacts import SearchRequest, GetContactsRequest
from telethon.tl.functions.messages import GetFullChatRequest, ImportChatInviteRequest, CheckChatInviteRequest
from telethon.tl.types import (
    Channel, Chat, ChannelParticipantCreator, ChannelParticipantAdmin,
    MessageMediaPhoto, MessageMediaDocument
)
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageMediaWebPage,
    DocumentAttributeFilename,
    DocumentAttributeAudio,
    DocumentAttributeVideo
)
import io, base64
from telethon.utils import get_peer_id

from config import CONFIG
from exception_classifier import ErrorCategory, classify_exception

# =====================================================================
# 📦 PYDANTIC MODELS
# =====================================================================
class SendMessageRequest(BaseModel):
    phone: str
    chat_id: str
    message: str = ""
    text: str = ""
    reply_to: Optional[int] = None
    edit_id: Optional[int] = None

class MassActionRequest(BaseModel):
    target_channel: str

class JoinActionRequest(BaseModel):
    phone: str
    chat_id: str

class SmartRouteRequest(BaseModel):
    phone: str
    target: str

class ForwardMessageRequest(BaseModel):
    phone: str
    from_chat_id: str
    to_chat_id: str
    msg_id: int

class DeleteMessageRequest(BaseModel):
    phone: str
    chat_id: str
    msg_id: int
    delete_for_everyone: bool
        

# =====================================================================
# 🚀 ROUTER & DB INITIALIZATION
# =====================================================================
logger = logging.getLogger("WebConsoleModule")
console_router = APIRouter()
_db = None
_session_manager = None


def init_console_db(db_instance):
    """Initializes the database reference link for backend APIs."""
    global _db
    _db = db_instance
    logger.info("🌐 Pure Backend Data Transfer API Hub Engine initialized.")
    return console_router


def init_console_session_manager(session_manager_instance):
    """Initializes the SessionManager reference for centralized client creation."""
    global _session_manager
    _session_manager = session_manager_instance

def setup_console_routes(db_instance):
    """Alias placeholder to satisfy pre-existing system loop bindings."""
    return init_console_db(db_instance)

# =====================================================================
# 🔐 PERSISTENT CONNECTION POOL (Collision-Safe)
# =====================================================================
ACTIVE_CLIENT_POOL = OrderedDict()
MAX_WEB_POOL_SIZE = 50          # maximum clients to keep in memory
WEB_CLIENT_IDLE_TIMEOUT = 300    # seconds before eviction (currently not enforced, but we'll use LRU)
CLIENT_LOCKS = {}

async def get_buffered_active_client(phone: str, record: dict) -> TelegramClient:
    """
    Fetches or creates a long-lived TelegramClient session.
    Uses asyncio.Lock() per phone node to prevent concurrent request overlap.
    Implements LRU eviction to limit the pool size.
    """
    clean_phone = phone.replace("+", "").replace(" ", "")
    
    # Enforce CLIENT_LOCKS size limit to prevent memory leaks
    if len(CLIENT_LOCKS) > 500:
        active_phones = set(ACTIVE_CLIENT_POOL.keys())
        stale_locks = [p for p in CLIENT_LOCKS.keys() if p not in active_phones]
        for p in stale_locks:
            CLIENT_LOCKS.pop(p, None)

    if clean_phone not in CLIENT_LOCKS:
        CLIENT_LOCKS[clean_phone] = asyncio.Lock()
        
    async with CLIENT_LOCKS[clean_phone]:
        # Check if client exists and is healthy
        if clean_phone in ACTIVE_CLIENT_POOL:
            client = ACTIVE_CLIENT_POOL[clean_phone]
            try:
                if client.is_connected():
                    if await client.is_user_authorized():
                        # Mark as recently used
                        ACTIVE_CLIENT_POOL.move_to_end(clean_phone)
                        return client
                    else:
                        logger.warning(f"Client for +{clean_phone} is unauthorized. Reinitializing.")
                        await client.disconnect()
                        ACTIVE_CLIENT_POOL.pop(clean_phone, None)
                else:
                    ACTIVE_CLIENT_POOL.pop(clean_phone, None)
            except Exception as e:
                logger.debug(f"Client health check failed for +{clean_phone}: {e}")
                ACTIVE_CLIENT_POOL.pop(clean_phone, None)
                try: await client.disconnect()
                except: pass
        
        # Create new client
        session_token = record.get("session_string") or record.get("session")
        if not session_token:
            raise ValueError("Cryptographic session token missing inside database layers.")
            
        device = record.get("device_metadata") or {
            "device_model": record.get("device_model", "PC 64bit"),
            "system_version": record.get("system_version", "Windows 11"),
            "app_version": record.get("app_version", "4.8.4")
        }
        
        proxy_dict = record.get("proxy")
        
        logger.info(f"🔌 Spawning collision-safe persistent MTProto node link for +{clean_phone}")
        client = _session_manager._create_client(
            session_str=session_token,
            api_id=int(record.get("api_id", CONFIG["API_ID"])),
            api_hash=str(record.get("api_hash", CONFIG["API_HASH"])),
            device=device,
            proxy=proxy_dict if (proxy_dict and isinstance(proxy_dict, dict)) else None,
        )
        await client.connect()
        
        if not await client.is_user_authorized():
            await client.disconnect()
            raise PermissionError("Session authentication trace expired or revoked.")
            
        # Add to pool and mark as recent
        ACTIVE_CLIENT_POOL[clean_phone] = client
        ACTIVE_CLIENT_POOL.move_to_end(clean_phone)
        
        # Enforce pool size limit by evicting oldest (LRU)
        while len(ACTIVE_CLIENT_POOL) > MAX_WEB_POOL_SIZE:
            oldest_phone, oldest_client = ACTIVE_CLIENT_POOL.popitem(last=False)
            try:
                await oldest_client.disconnect()
            except:
                pass
            CLIENT_LOCKS.pop(oldest_phone, None)
            logger.info(f"Evicted idle client for +{oldest_phone} from web pool")
        
        return client

# Helper to safely parse chat IDs (handles negative IDs for groups/channels)
def parse_chat_id(chat_id_str: str):
    return int(chat_id_str) if chat_id_str.lstrip('-').isdigit() else chat_id_str

# =====================================================================
# 📒 CORE ENDPOINTS
# =====================================================================
@console_router.get("/api/console/accounts")
async def api_console_accounts():
    global _db
    if not _db: return []
    accounts = await _db.get_all_suite_sessions()
    catalog = []
    for acc in accounts:
        catalog.append({
            "phone": acc.get("phone"),
            "first_name": acc.get("first_name", acc.get("device_model", "Identity Node")),
            "status": acc.get("status", "pending"),
            "is_restricted": acc.get("is_restricted", False),
            "dc_id": acc.get("dc_id", None)
        })
    return catalog

@console_router.get("/api/console/contacts/{phone}")
async def api_console_contacts(phone: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        result = await client(GetContactsRequest(hash=0))
        contacts_data = []
        for user in result.users:
            contacts_data.append({
                "id": str(user.id),
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "phone": user.phone or "",
                "username": user.username or "",
                "mutual": getattr(user, 'mutual_contact', False)
            })
        contacts_data.sort(key=lambda x: (x['first_name'] or x['username'] or '').lower())
        return {"status": "success", "contacts": contacts_data}
    except Exception as e:
        logger.error(f"Contacts fetch engine fault: {e}")
        return {"status": "error", "reason": str(e)}

@console_router.post("/api/console/send")
async def api_console_send_message(req: SendMessageRequest):
    global _db
    if not _db: raise HTTPException(status_code=500, detail="Database uninitialized.")
    clean_phone = req.phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Target profile credentials missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        target_id = parse_chat_id(req.chat_id)
        content = (req.message or req.text or "").strip()
        if not content:
            return {"status": "error", "reason": "Message text is empty."}
        # 🔥 FIX: Handle edit message and safe reply_to casting
        if req.edit_id and str(req.edit_id).strip():
            await client.edit_message(target_id, int(req.edit_id), content)
        else:
            reply_to_id = int(req.reply_to) if req.reply_to and str(req.reply_to).strip() else None
            await client.send_message(target_id, content, reply_to=reply_to_id)
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Outbound message delivery error: {e}")
        return {"status": "error", "reason": str(e)}

# =====================================================================
# 🤖 MASS AUTOMATION & LIVE BACKGROUND TELEMETRY HUB
# =====================================================================
from collections import deque
automation_logs_stream = deque(maxlen=100)

def append_system_log(message: str):
    # 🔥 FIX: Convert Backend Automation Logs to IST Time
    ist_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    timestamp = ist_time.strftime("%Y-%m-%d %H:%M:%S")
    automation_logs_stream.append(f"[{timestamp}] ⚙️ {message}")

@console_router.get("/api/console/automation-logs")
async def get_live_automation_logs():
    return {"status": "success", "logs": automation_logs_stream}

async def async_mass_join_worker(accounts, target_channel):
    append_system_log(f"INITIATING MASS OPERATIONS: Target resolved to '{target_channel}'")
    success_count = 0
    fail_count = 0
    for idx, acc in enumerate(accounts):
        phone = acc.get("phone")
        try:
            client = await get_buffered_active_client(phone, acc)
            append_system_log(f"Processing Account #{idx+1}/{len(accounts)} (+{phone})...")
            clean_target = target_channel.strip().replace("https://t.me/", "").replace("@", "")
            await client(JoinChannelRequest(clean_target))
            append_system_log(f"✅ Node #{idx+1} (+{phone}) successfully joined group/channel.")
            success_count += 1
        except Exception as err:
            append_system_log(f"❌ Node #{idx+1} (+{phone}) join failure: {str(err)}")
            fail_count += 1
        await asyncio.sleep(5)
    append_system_log(f"🏁 BATCH OPERATIONS FINISHED: Success: {success_count} | Crashed/Failed: {fail_count}")

@console_router.post("/api/console/mass-execute")
async def trigger_mass_operation(req: MassActionRequest):
    global _db
    if not _db: return {"status": "error", "reason": "DB reference dropped."}
    accounts = await _db.get_all_suite_sessions()
    if not accounts:
        return {"status": "error", "reason": "No active identity sessions found inside the database pool container."}
    asyncio.create_task(async_mass_join_worker(accounts, req.target_channel))
    return {"status": "success", "message": "Mass task pipeline injected successfully into background engine."}

# =====================================================================
# 💬 DIALOGS & MESSAGES ENGINE
# =====================================================================
@console_router.get("/api/console/dialogs/{phone}")
async def api_console_dialogs(phone: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database reference pointer uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Target session token missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        me = await client.get_me()
        my_id = me.id
        dialogs = await client.get_dialogs(limit=None)
        dialogs_payload = []
        
        for chat in dialogs:
            title = chat.name or "Private Chat Space"
            last_msg = str(chat.message.message or "").strip() if chat.message else ""
            
            chat_type = "private"
            if chat.is_group:
                chat_type = "group"
            elif chat.is_channel:
                chat_type = "channel"
            elif getattr(chat.entity, 'bot', False):
                chat_type = "bot"
            
            is_saved_messages = False
            is_telegram_service = False
            
            if chat.id == my_id:
                title = "Saved Messages"
                chat_type = "saved"
                is_saved_messages = True
            elif chat.id == 777000:
                title = "Telegram"
                chat_type = "service"
                is_telegram_service = True
                
            if not last_msg and chat.message and chat.message.media:
                last_msg = "[Attachment/Media File]"
                
            dialogs_payload.append({
                "id": str(chat.id),
                "title": title,
                "type": chat_type,
                "last_message": last_msg[:45] + "..." if len(last_msg) > 45 else (last_msg or "No messages"),
                "last_date": int(chat.date.timestamp()) if chat.date else 0,
                "unread_count": getattr(chat, "unread_count", 0) or 0,
                "pinned": bool(getattr(chat, "pinned", False)),
                "muted": bool(getattr(chat, "muted", False)),
                "is_saved": is_saved_messages,
                "is_service": is_telegram_service
            })
        return {"status": "success", "phone": clean_phone, "dialogs": dialogs_payload}
    except Exception as e:
        logger.error(f"Failed to fetch dialogs: {e}")
        return {"status": "error", "reason": str(e)}

@console_router.get("/api/console/messages/{phone}/{chat_id}")
@console_router.get("/api/console/chat-history/{phone}/{chat_id}")
async def api_console_messages(phone: str, chat_id: str):
    global _db
    if not _db:
        return {"status": "error", "reason": "Database reference uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record:
        return {"status": "error", "reason": "Session matrix allocation dropped."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        target_entity = parse_chat_id(chat_id)
        resolved_peer = await client.get_entity(target_entity)
        messages = await client.get_messages(resolved_peer, limit=45)
        messages_payload = []
        
        for msg in reversed(messages):
            if not msg.message and not msg.media:
                continue
            text = str(msg.message or "").strip()
            # 🔥 FIX: Convert Telethon UTC msg.date to IST
            ist_date = msg.date + datetime.timedelta(hours=5, minutes=30)
            time_node = ist_date.strftime("%H:%M")
            media_type = "text"
            media_data = None  # 🔥 Will hold rich media object

            # 🔥 Extract media details
            if msg.media:
                # --- PHOTO ---
                if isinstance(msg.media, MessageMediaPhoto):
                    media_type = "photo"
                    try:
                        photo_buffer = io.BytesIO()
                        await client.download_media(msg.media, file=photo_buffer)
                        if photo_buffer.getvalue():
                            b64_data = base64.b64encode(photo_buffer.getvalue()).decode('utf-8')
                            media_data = {
                                "type": "photo",
                                "url": f"data:image/jpeg;base64,{b64_data}",
                                "caption": text or ""
                            }
                            text = ""  # Avoid duplicate caption
                        else:
                            media_data = {"type": "photo", "url": None}
                    except Exception as e:
                        logger.debug(f"Photo download failed: {e}")
                        media_data = {"type": "photo", "url": None}

                # --- DOCUMENT (file, video, voice, audio) ---
                elif isinstance(msg.media, MessageMediaDocument):
                    doc = msg.media.document
                    attrs = doc.attributes
                    file_name = "Document"
                    is_voice = False
                    is_video = False
                    size = doc.size
                    duration = 0

                    for attr in attrs:
                        if isinstance(attr, DocumentAttributeFilename):
                            file_name = attr.file_name
                        elif isinstance(attr, DocumentAttributeAudio):
                            is_voice = attr.voice
                            duration = attr.duration
                        elif isinstance(attr, DocumentAttributeVideo):
                            is_video = True
                            duration = attr.duration

                    if is_voice:
                        media_type = "audio"
                        media_data = {
                            "type": "audio",
                            "filename": file_name,
                            "size": f"{size // 1024} KB",
                            "duration": duration,
                            "url": None
                        }
                    elif is_video:
                        media_type = "video"
                        media_data = {
                            "type": "video",
                            "filename": file_name,
                            "size": f"{size // 1024} KB",
                            "duration": duration,
                            "url": None
                        }
                    else:
                        media_type = "file"
                        media_data = {
                            "type": "file",
                            "filename": file_name,
                            "size": f"{size // 1024} KB",
                            "url": None
                        }

                # --- WEB PAGE / LINK PREVIEW ---
                elif isinstance(msg.media, MessageMediaWebPage):
                    media_type = "link"
                    media_data = {
                        "type": "link",
                        "url": msg.media.webpage.url if msg.media.webpage else None,
                        "title": msg.media.webpage.title if msg.media.webpage else None
                    }

                # --- OTHER MEDIA TYPES ---
                else:
                    media_type = "document"
                    media_data = {"type": "document", "url": None}

            sender_name = "User"
            if msg.sender:
                sender_name = getattr(msg.sender, 'first_name', '') or getattr(msg.sender, 'title', 'User')

            messages_payload.append({
                "id": msg.id,
                "text": text,
                "time": time_node,
                "date": int(msg.date.timestamp()) if msg.date else 0,
                "is_self": msg.out,
                "outgoing": msg.out,
                "media_type": media_type,
                "media": media_data,          # 🔥 Rich media object
                "sender_name": sender_name.strip()
            })

        return {"status": "success", "messages": messages_payload}

    except Exception as e:
        logger.error(f"Messages trace error: {e}")
        return {"status": "error", "reason": str(e)}

# =====================================================================
# 👤 PROFILE & GLOBAL SEARCH
# =====================================================================
@console_router.get("/api/console/profile/{phone}")
async def api_console_get_profile(phone: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database reference uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        me = await client.get_me()
        if me:
            full_name = f"{me.first_name or ''} {me.last_name or ''}".strip() or f"+{clean_phone}"
            photo_uri = None
            try:
                photo_buffer = io.BytesIO()
                await client.download_profile_photo(me, file=photo_buffer)
                if photo_buffer.getvalue():
                    photo_uri = f"data:image/jpeg;base64,{base64.b64encode(photo_buffer.getvalue()).decode('utf-8')}"
            except: pass
            
            server_dc = getattr(client.session, 'dc_id', 'Unknown')
            is_restricted = getattr(me, 'restricted', False)
            restriction_reason = getattr(me, 'restriction_reason', 'None')
            
            raw_proxy = record.get("proxy") or CONFIG.get("PROXY")
            proxy_string = "Direct Connection"
            if raw_proxy:
                if isinstance(raw_proxy, dict):
                    proxy_string = f"{raw_proxy.get('proxy_type', 'HTTP')}://{raw_proxy.get('addr')}:{raw_proxy.get('port')}"
                elif isinstance(raw_proxy, (list, tuple)) and len(raw_proxy) >= 2:
                    proxy_string = f"SOCKS5://{raw_proxy[0]}:{raw_proxy[1]}"
                    
            if hasattr(_db, "source_accounts") and _db.source_accounts:
                _db.source_accounts.update_one(
                    {"phone": clean_phone},
                    {"$set": {
                        "first_name": full_name,
                        "is_restricted": is_restricted,
                        "dc_id": f"DC {server_dc}"
                    }}
                )
                
            return {
                "status": "success",
                "full_name": full_name,
                "username": me.username or "No Username Set",
                "phone": me.phone or clean_phone,
                "profile_pic": photo_uri,
                "dc_id": f"DC {server_dc}",
                "proxy": proxy_string,
                "restricted": "Restricted (SpamBlock Alert)" if is_restricted else "Good Health (Clear)",
                "restriction_details": str(restriction_reason) if is_restricted else "No active violations found."
            }
        return {"status": "error", "reason": "User block mismatch."}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


# =====================================================================
# ⚕️ HEALTH & ANALYTICS
# =====================================================================
@console_router.get("/api/console/health/{phone}")
async def api_console_health_metrics(phone: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database uninitialized."}
    
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session missing."}

    # Extract dynamic health variables mapped from adder.py & dmsender.py interactions
    status = record.get("status", "unknown")
    err_log = record.get("revocation_reason") or record.get("last_error") or "No active violations found."
    
    health_details = "All systems operational"
    if status in ["failed", "restricted", "banned"]:
        health_details = f"Account restricted: {err_log}"
    
    return {
        "status": "success",
        "health_score": 100 if status == "active" else (0 if status == "revoked" else 50),
        "details": health_details,
        "flood_history": 0,
        "total_added": record.get("account_sequence_index", 0)
    }
    
    
@console_router.get("/api/console/global-search/{phone}")
async def api_console_global_search(phone: str, q: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        result = await client(SearchRequest(q=q, limit=10))
        search_results = []
        
        for user in result.users:
            search_results.append({
                "id": user.username if user.username else str(user.id),
                "title": f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username,
                "username": user.username or "",
                "type": "people",
                "description": "Global User"
            })
            
        for chat in result.chats:
            chat_type = "groups" if getattr(chat, 'megagroup', False) else "channels"
            search_results.append({
                "id": getattr(chat, 'username', '') or str(chat.id),
                "title": chat.title,
                "username": getattr(chat, 'username', '') or "",
                "type": chat_type,
                "description": f"{getattr(chat, 'participants_count', 0)} Members" if hasattr(chat, 'participants_count') else "Global Channel"
            })
        return {"status": "success", "results": search_results}
    except Exception as e:
        logger.error(f"Global search engine fault: {e}")
        return {"status": "error", "reason": str(e)}

# =====================================================================
# 🏢 CHAT INFO & MEDIA EXPLORER
# =====================================================================
@console_router.get("/api/console/chat-info/{phone}/{chat_id}")
async def api_console_chat_info(phone: str, chat_id: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        target_entity = parse_chat_id(chat_id)
        entity = await client.get_entity(target_entity)
        is_user = hasattr(entity, 'first_name')
        
        about_text = "No description"
        invite_link = f"t.me/{entity.username}" if getattr(entity, 'username', None) else "Private Link Space"
        member_count = 0
        photo_b64 = None
        stats = {} # 🔥 FIX: Initialized to prevent UnboundLocalError
        
        try:
            p_buffer = io.BytesIO()
            await client.download_profile_photo(entity, file=p_buffer)
            if p_buffer.getvalue():
                photo_b64 = f"data:image/jpeg;base64,{base64.b64encode(p_buffer.getvalue()).decode('utf-8')}"
        except: pass
        
        if not is_user:
            if isinstance(entity, Channel):
                full_res = await client(GetFullChannelRequest(channel=entity))
                about_text = full_res.full_chat.about or about_text
                member_count = full_res.full_chat.participants_count or 0
                if full_res.full_chat.exported_invite:
                    invite_link = full_res.full_chat.exported_invite.link
            else:
                full_res = await client(GetFullChatRequest(chat_id=entity.id))
                about_text = full_res.full_chat.about or about_text
                member_count = len(full_res.full_chat.users)
                
            stats = {
                "photos": getattr(full_res.full_chat, 'photos_count', 0),
                "videos": getattr(full_res.full_chat, 'videos_count', 0),
                "files": getattr(full_res.full_chat, 'files_count', 0),
                "audios": getattr(full_res.full_chat, 'audios_count', 0),
                "links": getattr(full_res.full_chat, 'links_count', 0),
                "voices": getattr(full_res.full_chat, 'voice_messages_count', 0),
                "gifs": getattr(full_res.full_chat, 'gifs_count', 0)
            }
                
        members_list = []
        if not is_user:
            try:
                # 🔥 FIX: Forced limit to prevent 502 Proxy Timeout
                async for p in client.iter_participants(entity, limit=100):
                    role = "member"
                    if isinstance(p.participant, ChannelParticipantCreator): role = "owner"
                    elif isinstance(p.participant, ChannelParticipantAdmin): role = "admin"
                    
                    status_str = "last seen recently"
                    if p.status and "UserStatusOnline" in type(p.status).__name__:
                        status_str = "online"
                        
                    members_list.append({
                        "id": str(p.id),
                        "name": f"{getattr(p, 'first_name', '')} {getattr(p, 'last_name', '')}".strip() or 'Telegram User',
                        "username": p.username or "",
                        "role": role,
                        "status": status_str
                    })
            except Exception as e:
                logger.debug(f"Iter participants safe-catch: {e}")
                
        return {
            "status": "success",
            "info": {
                "title": getattr(entity, 'title', f"{getattr(entity, 'first_name', '')} {getattr(entity, 'last_name', '')}".strip()),
                "type": "user" if is_user else "group",
                "about": about_text,
                "photo": photo_b64,
                "member_count": member_count,
                "stats": stats,
            },
            "type": "user" if is_user else "group",
            "title": getattr(entity, 'title', f"{getattr(entity, 'first_name', '')} {getattr(entity, 'last_name', '')}".strip()),
            "member_count": member_count,
            "about": about_text,
            "link": invite_link,
            "photo": photo_b64,
            "stats": stats,
            "members": members_list
        }
    except Exception as e:
        logger.error(f"Chat info engine exception: {e}")
        return {"status": "error", "reason": str(e)}

@console_router.get("/api/console/chat-members/{phone}/{chat_id}")
async def api_console_chat_members(phone: str, chat_id: str):
    info = await api_console_chat_info(phone, chat_id)
    if info.get("status") != "success":
        return info
    members = []
    for member in info.get("members", []):
        members.append({
            "id": member.get("id"),
            "first_name": member.get("name", "User"),
            "last_name": "",
            "username": member.get("username", ""),
            "role": member.get("role", "member"),
            "status": member.get("status", "")
        })
    return {"status": "success", "members": members}


@console_router.get("/api/console/ping/{phone}")
async def api_console_ping(phone: str):
    global _db
    if not _db:
        return {"status": "error", "reason": "Database uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record:
        return {"status": "error", "reason": "Session missing."}
    try:
        client = await get_buffered_active_client(clean_phone, record)
        if client.is_connected() and await client.is_user_authorized():
            return {"status": "success", "connected": True}
        return {"status": "error", "reason": "Not connected"}
    except Exception as e:
        return {"status": "error", "reason": str(e)}


@console_router.get("/api/console/analytics/{phone}")
async def api_console_analytics(phone: str):
    profile = await api_console_get_profile(phone)
    health = await api_console_health_metrics(phone)
    global _db
    
    # Fetch all sessions (await the async method)
    all_sessions = await _db.get_all_suite_sessions() if _db else []
    total_sessions = len(all_sessions)
    active_sessions = len([a for a in all_sessions if a.get("status") == "active"])
    
    return {
        "status": "success",
        "total_sessions": total_sessions,
        "active_sessions": active_sessions,
        "profile": profile if profile.get("status") == "success" else {},
        "health": health if health.get("status") == "success" else {}
    }


@console_router.get("/api/console/chat-media/{phone}/{chat_id}")
async def api_console_chat_media(phone: str, chat_id: str, media_type: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session matrix missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        target_entity = parse_chat_id(chat_id)
        resolved_peer = await client.get_entity(target_entity)
        messages = await client.get_messages(resolved_peer, limit=80)
        extracted_items = []
        
        for msg in messages:
            if media_type == "links" and msg.message:
                urls = re.findall(r'(https?://[^\s]+)', msg.message)
                for url in urls:
                    extracted_items.append({
                        "id": msg.id,
                        "url": url,
                        "context": msg.message[:60] + "..." if len(msg.message) > 60 else msg.message
                    })
                continue
                
            if not msg.media: continue
            
            if media_type == "photos" and isinstance(msg.media, MessageMediaPhoto):
                try:
                    p_buffer = io.BytesIO()
                    await client.download_media(msg.media, file=p_buffer)
                    if p_buffer.getvalue():
                        b64_str = base64.b64encode(p_buffer.getvalue()).decode('utf-8')
                        extracted_items.append({
                            "id": msg.id,
                            "src": f"data:image/jpeg;base64,{b64_str}",
                            "caption": msg.message or ""
                        })
                except: pass
            elif media_type == "files" and isinstance(msg.media, MessageMediaDocument):
                attributes = getattr(msg.media.document, 'attributes', [])
                is_voice = any(getattr(a, 'voice', False) for a in attributes)
                is_video = any(getattr(a, 'video', False) for a in attributes)
                if not is_voice and not is_video:
                    file_name = "Document_Object.bin"
                    for attr in attributes:
                        if hasattr(attr, 'file_name'):
                            file_name = attr.file_name
                    extracted_items.append({
                        "id": msg.id,
                        "title": file_name,
                        "size": f"{round(msg.media.document.size / 1024, 1)} KB"
                    })
            elif media_type == "voices" and isinstance(msg.media, MessageMediaDocument):
                attributes = getattr(msg.media.document, 'attributes', [])
                if any(getattr(a, 'voice', False) for a in attributes):
                    ist_date = msg.date + datetime.timedelta(hours=5, minutes=30)
                    extracted_items.append({
                        "id": msg.id,
                        "date": ist_date.strftime("%d %b %H:%M"),  # 🔥 FIX: Converted to IST
                        "duration": "Voice Note Clip"
                    })
        return {"status": "success", "media_type": media_type, "items": extracted_items}
    except Exception as e:
        logger.error(f"Shared media lookup processing dropout: {e}")
        return {"status": "error", "reason": str(e)}

# =====================================================================
# 🛠️ ACTIONS (Join, Route, Forward, Delete)
# =====================================================================
@console_router.post("/api/console/join-chat")
async def api_console_join_chat(req: JoinActionRequest):
    global _db
    if not _db: return {"status": "error"}
    clean_phone = req.phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        target = parse_chat_id(req.chat_id)
        await client(JoinChannelRequest(target))
        return {"status": "success", "message": "Successfully joined the network node!"}
    except Exception as e:
        return {"status": "error", "reason": str(e)}

@console_router.post("/api/console/smart-route")
async def api_console_smart_route(req: SmartRouteRequest):
    global _db
    if not _db: return {"status": "error", "reason": "Database reference pointer uninitialized."}
    clean_phone = req.phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session matrix allocation dropped."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        raw_target = req.target.strip()
        is_private = False
        clean_token = raw_target
        
        if "joinchat/" in raw_target:
            is_private = True
            clean_token = raw_target.split("joinchat/")[-1]
        elif "t.me/+" in raw_target:
            is_private = True
            clean_token = raw_target.split("t.me/+")[-1]
        elif "+" in raw_target and not raw_target.startswith("@"):
            is_private = True
            clean_token = raw_target.replace("+", "")
        else:
            clean_token = raw_target.replace("https://t.me/", "").replace("@", "").strip()
            
        chat_id = None
        title = "Telegram Room"
        
        if is_private:
            clean_hash = clean_token.strip()
            try:
                updates = await client(ImportChatInviteRequest(hash=clean_hash))
                if getattr(updates, "chats", None):
                    entity = updates.chats[0]
                    chat_id = str(get_peer_id(entity))
                    title = getattr(entity, 'title', 'Private Group')
            except UserAlreadyParticipantError:
                invite_info = await client(CheckChatInviteRequest(hash=clean_hash))
                entity = getattr(invite_info, "chat", None)
                if entity:
                    chat_id = str(get_peer_id(entity))
                    title = getattr(entity, 'title', 'Private Group')
        else:
            try:
                entity = await client.get_entity(clean_token)
                chat_id = str(get_peer_id(entity))
                title = getattr(entity, 'title', f"{getattr(entity, 'first_name', '')} {getattr(entity, 'last_name', '')}".strip())
                try:
                    await client(JoinChannelRequest(entity))
                except UserAlreadyParticipantError:
                    pass
            except Exception:
                try:
                    updates = await client(JoinChannelRequest(clean_token))
                    if getattr(updates, "chats", None):
                        entity = updates.chats[0]
                        chat_id = str(get_peer_id(entity))
                        title = getattr(entity, 'title', 'Public Chat')
                except Exception as e:
                    logger.error(f"Smart route fallback failed: {e}")
                    
        if chat_id:
            return {
                "status": "success",
                "chat_id": chat_id,
                "title": title,
                "message": "Successfully routed to target destination node."
            }
        else:
            return {"status": "error", "reason": "Could not resolve structural identity path."}
    except Exception as e:
        logger.error(f"Smart Routing Bridge Error: {e}")
        return {"status": "error", "reason": str(e)}

@console_router.get("/api/console/chat-photo/{phone}/{chat_id}")
async def api_console_chat_photo(phone: str, chat_id: str):
    global _db
    if not _db: return {"status": "error", "reason": "Database uninitialized."}
    clean_phone = phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Session missing."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        target_entity = parse_chat_id(chat_id)
        entity = await client.get_entity(target_entity)
        photo_buffer = io.BytesIO()
        await client.download_profile_photo(entity, file=photo_buffer, download_big=False)
        if photo_buffer.getvalue():
            photo_b64 = base64.b64encode(photo_buffer.getvalue()).decode('utf-8')
            return {"status": "success", "photo": f"data:image/jpeg;base64,{photo_b64}"}
        return {"status": "success", "photo": None}
    except Exception as e:
        logger.debug(f"Silent avatar fetch bypass for {chat_id}: {e}")
        return {"status": "error", "reason": str(e)}

@console_router.post("/api/console/forward")
async def api_console_forward_message(req: ForwardMessageRequest):
    global _db
    if not _db: raise HTTPException(status_code=500, detail="Database uninitialized.")
    clean_phone = req.phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Target profile credentials missing inside database layers."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        from_peer = parse_chat_id(req.from_chat_id)
        to_peer = parse_chat_id(req.to_chat_id)
        await client.forward_messages(to_peer, req.msg_id, from_peer)
        return {"status": "success", "message": "Message routed successfully to target destination node."}
    except Exception as e:
        logger.error(f"Outbound message forward failure exception: {e}")
        return {"status": "error", "reason": str(e)}

@console_router.post("/api/console/delete-message")
async def api_console_delete_message(req: DeleteMessageRequest):
    global _db
    if not _db: raise HTTPException(status_code=500, detail="Database uninitialized.")
    clean_phone = req.phone.replace("+", "").replace(" ", "")
    record = _db.get_session_by_phone(clean_phone)
    if not record: return {"status": "error", "reason": "Target profile credentials missing inside database layers."}
    
    try:
        client = await get_buffered_active_client(clean_phone, record)
        target_peer = parse_chat_id(req.chat_id)
        await client.delete_messages(target_peer, [req.msg_id], revoke=req.delete_for_everyone)
        return {"status": "success", "message": "Message successfully erased from Telegram matrix coordinates."}
    except Exception as e:
        logger.error(f"Message erasing protocol breakdown: {e}")
        return {"status": "error", "reason": str(e)}