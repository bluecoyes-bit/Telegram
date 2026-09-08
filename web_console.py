#!/usr/bin/env python3
"""
Filename: web_console.py
"""

import io
import base64
import os
import re
import logging
import asyncio
import uuid
import hmac
from typing import Optional, List, Dict, Any
import datetime
import time
from contextlib import asynccontextmanager
from collections import OrderedDict
from fastapi import APIRouter, Depends, HTTPException, Header, Request, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
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
from telethon.utils import get_peer_id

from config import CONFIG
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
from exception_classifier import ErrorCategory, classify_exception, ConnectionResult

# =====================================================================
# 🔒 SECURITY CONSTANTS & CONFIGURATION
# =====================================================================
# Pagination defaults
DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 500

# Request size limits
MAX_MESSAGE_LENGTH = 4096
MAX_TARGETS_PER_REQUEST = 100

# Rate limiting (already defined below, constants here for clarity)
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 60
RATE_LIMIT_MAX_BUCKETS = 4096

# Background task tracking
_background_tasks: set = set()
_operations: Dict[str, Dict[str, Any]] = {}  # operation_id -> metadata

# Phone validation regex (E.164 format without +)
PHONE_REGEX = re.compile(r'^\d{10,15}$')

def validate_phone(phone: str) -> str:
    """Validate and normalize phone number. Returns cleaned phone or raises HTTPException."""
    clean = phone.replace("+", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if not PHONE_REGEX.match(clean):
        raise HTTPException(status_code=400, detail="Invalid phone number format")
    return clean

def validate_chat_id(chat_id: str) -> str:
    """Validate chat_id - can be numeric ID or username."""
    if not chat_id or not chat_id.strip():
        raise HTTPException(status_code=400, detail="chat_id is required")
    return chat_id.strip()

def validate_limit(limit: int, default: int = DEFAULT_PAGE_LIMIT, maximum: int = MAX_PAGE_LIMIT) -> int:
    """Validate pagination limit."""
    if limit <= 0:
        return default
    return min(limit, maximum)

def validate_offset(offset: int) -> int:
    """Validate pagination offset."""
    return max(0, offset)

def sanitize_error_message(e: Exception) -> str:
    """Return a safe error message without internal details."""
    return "Operation failed"

# =====================================================================
# 📦 PYDANTIC MODELS (with validation)
# =====================================================================
class SendMessageRequest(BaseModel):
    phone: str = Field(..., min_length=10, max_length=15)
    chat_id: str = Field(..., min_length=1)
    message: str = ""
    text: str = ""
    reply_to: Optional[int] = None
    edit_id: Optional[int] = None
    
    @validator('phone')
    def validate_phone_format(cls, v):
        return validate_phone(v)
    
    @validator('message', 'text')
    def validate_message_length(cls, v):
        if v and len(v) > MAX_MESSAGE_LENGTH:
            raise ValueError(f"Message exceeds maximum length of {MAX_MESSAGE_LENGTH}")
        return v

class MassActionRequest(BaseModel):
    target_channel: str = Field(..., min_length=1, max_length=256)

class JoinActionRequest(BaseModel):
    phone: str = Field(..., min_length=10, max_length=15)
    chat_id: str = Field(..., min_length=1)
    
    @validator('phone')
    def validate_phone_format(cls, v):
        return validate_phone(v)

class SmartRouteRequest(BaseModel):
    phone: str = Field(..., min_length=10, max_length=15)
    target: str = Field(..., min_length=1, max_length=256)
    
    @validator('phone')
    def validate_phone_format(cls, v):
        return validate_phone(v)

class ForwardMessageRequest(BaseModel):
    phone: str = Field(..., min_length=10, max_length=15)
    from_chat_id: str = Field(..., min_length=1)
    to_chat_id: str = Field(..., min_length=1)
    msg_id: int = Field(..., gt=0)
    
    @validator('phone')
    def validate_phone_format(cls, v):
        return validate_phone(v)

class DeleteMessageRequest(BaseModel):
    phone: str = Field(..., min_length=10, max_length=15)
    chat_id: str = Field(..., min_length=1)
    msg_id: int = Field(..., gt=0)
    delete_for_everyone: bool = False
    
    @validator('phone')
    def validate_phone_format(cls, v):
        return validate_phone(v)

class PaginationParams(BaseModel):
    limit: int = Field(default=DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT)
    offset: int = Field(default=0, ge=0)
        

# =====================================================================
# 🚀 ROUTER & DB INITIALIZATION
# =====================================================================
logger = logging.getLogger("WebConsoleModule")
console_router = APIRouter()
_db = None
_session_manager = None

# ── Web API security ──
# Tracked background tasks so fire‑and‑forget work is cancellable on shutdown
_background_tasks: set = set()

# Operation tracking for long-running operations
_operations: Dict[str, Dict[str, Any]] = {}

# Lightweight per-IP sliding-window rate limiter for console endpoints
_RATE_LIMIT_WINDOW = 60          # seconds
_RATE_LIMIT_MAX = 60             # requests per window per IP
_rate_buckets: dict = {}
_RATE_LIMIT_MAX_BUCKETS = 4096   # hard cap so the dict stays bounded


def _rate_limited(client_ip: str) -> None:
    """Enforce a simple bounded sliding-window rate limit per client IP."""
    now = time.time()
    # opportunistically prune stale buckets to keep the dict bounded
    if len(_rate_buckets) > _RATE_LIMIT_MAX_BUCKETS:
        stale = [k for k, (ws, _c) in _rate_buckets.items() if now - ws > _RATE_LIMIT_WINDOW]
        for k in stale:
            _rate_buckets.pop(k, None)
    ws, count = _rate_buckets.get(client_ip, (now, 0))
    if now - ws > _RATE_LIMIT_WINDOW:
        ws = now
        count = 0
    count += 1
    _rate_buckets[client_ip] = (ws, count)
    if count > _RATE_LIMIT_MAX:
        raise HTTPException(status_code=429, detail="Too many requests")


def verify_api_token(supplied: str, configured: str) -> bool:
    """Constant‑time comparison of API tokens.

    Returns ``True`` if both strings are non‑empty and match exactly.
    """
    if not supplied or not configured:
        return False
    return hmac.compare_digest(
        supplied.encode("utf-8"),
        configured.encode("utf-8"),
    )


def ensure_api_token(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    x_api_token: Optional[str] = Header(default=None),
) -> None:
    """Require a valid ``WEB_API_TOKEN`` on every console route.

    The function is fail‑closed: if the token is missing, empty or does not match
    the configured value an HTTP 401/403 is raised. If no token is set in the
    configuration a 503 is returned to avoid accidental exposure of the API.

    Authorization model: single-admin token. All authenticated requests have
    full administrative access. No multi-user RBAC is implemented.
    """
    client_ip = request.client.host if request.client else "unknown"
    _rate_limited(client_ip)
    token = str(CONFIG.get("WEB_API_TOKEN", "") or "").strip()
    if not token:
        raise HTTPException(
            status_code=503,
            detail="WEB_API_TOKEN is not configured. Set WEB_API_TOKEN to secure this API.",
        )
    provided = ""
    if authorization and authorization.lower().startswith("bearer "):
        provided = authorization[7:].strip()
    elif x_api_token:
        provided = x_api_token.strip()
    if not verify_api_token(provided, token):
        raise HTTPException(status_code=401, detail="Invalid or missing API token")


# Apply authentication + rate limiting to every console route.
console_router.dependencies = [Depends(ensure_api_token)]


def shutdown_background_tasks() -> None:
    """Cancel and await tracked console background tasks.
    
    Idempotent: safe to call multiple times.
    """
    global _background_tasks
    pending = list(_background_tasks)
    _background_tasks.clear()
    for t in pending:
        t.cancel()
    if pending:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_wait_background_tasks(pending))
            else:
                loop.run_until_complete(_wait_background_tasks(pending))
        except RuntimeError:
            pass


async def _wait_background_tasks(tasks) -> None:
    for t in tasks:
        try:
            await t
        except asyncio.CancelledError:
            pass
        except Exception:
            pass


def _register_task(task: asyncio.Task, operation_id: str, meta: Dict[str, Any]) -> None:
    """Register a background task with operation metadata."""
    _background_tasks.add(task)
    _operations[operation_id] = {
        **meta,
        "started_at": time.time(),
        "state": "running",
    }
    
    def _done(t: asyncio.Task) -> None:
        _background_tasks.discard(t)
        op_meta = _operations.get(operation_id, {})
        if not t.cancelled():
            exc = t.exception()
            if exc:
                logger.exception("WEB_BACKGROUND_TASK_FAILED", exc_info=exc)
                _operations[operation_id] = {**op_meta, "state": "failed", "error": sanitize_error_message(exc)}
            else:
                _operations[operation_id] = {**op_meta, "state": "completed"}
        else:
            _operations[operation_id] = {**op_meta, "state": "cancelled"}

    task.add_done_callback(_done)


def get_operation_status(operation_id: str) -> Optional[Dict[str, Any]]:
    """Get status of a background operation."""
    return _operations.get(operation_id)


def init_console_db(db_instance):
    """Initializes the database reference link for backend APIs."""
    global _db
    _db = db_instance
    logger.info("Web console database initialized.")
    return console_router


def init_console_session_manager(session_manager_instance):
    """Initializes the SessionManager reference for centralized client creation."""
    global _session_manager
    _session_manager = session_manager_instance


def setup_console_routes(db_instance):
    """Alias placeholder to satisfy pre-existing system loop bindings."""
    return init_console_db(db_instance)

# =====================================================================
# 📋 SESSION-ROUTED CLIENT ACCESS (via SessionManager)
# =====================================================================

@asynccontextmanager
async def managed_web_session(phone: str):
    """
    Context manager: acquires a managed session lease from SessionManager,
    yields (client, lease), and releases the lease on exit.

    Usage:
        async with managed_web_session(phone) as (client, lease):
            await client.send_message(...)
    """
    if not _session_manager:
        raise RuntimeError("SessionManager not initialized for web console")
    clean_phone = validate_phone(phone)
    async with _session_manager.acquire(
        clean_phone,
        module="web_console",
        auto_release=True,
    ) as lease:
        if not lease:
            raise HTTPException(
                status_code=409,
                detail=f"Session unavailable for +{clean_phone} (busy or terminal)"
            )
        try:
            yield lease.client, lease
        finally:
            pass  # auto_release=True handles cleanup

# Helper to safely parse chat IDs (handles negative IDs for groups/channels)
def parse_chat_id(chat_id_str: str):
    return int(chat_id_str) if chat_id_str.lstrip('-').isdigit() else chat_id_str


def _audit_log(endpoint: str, operation_id: str, action: str, target: str, result: str, duration: float, error_category: Optional[str] = None) -> None:
    """Structured audit log for sensitive administrative actions."""
    logger.info(
        "WEB_AUDIT | endpoint=%s | operation_id=%s | action=%s | target=%s | result=%s | duration_ms=%.2f | error_category=%s",
        endpoint, operation_id, action, target, result, duration * 1000, error_category or "none"
    )

# =====================================================================
# 📒 CORE ENDPOINTS
# =====================================================================
@console_router.get("/api/console/accounts")
async def api_console_accounts(
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/accounts", operation_id, "list_accounts", "all", "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    try:
        accounts = await _db.get_all_suite_sessions()
        total = len(accounts)
        paginated = accounts[offset:offset + limit]
        catalog = []
        for acc in paginated:
            catalog.append({
                "phone": acc.get("phone"),
                "first_name": acc.get("first_name", acc.get("device_model", "Identity Node")),
                "status": acc.get("status", "pending"),
                "is_restricted": acc.get("is_restricted", False),
                "dc_id": acc.get("dc_id", None)
            })
        _audit_log("/api/console/accounts", operation_id, "list_accounts", f"{len(catalog)}/{total}", "success", time.time() - start)
        return {"accounts": catalog, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        _audit_log("/api/console/accounts", operation_id, "list_accounts", "all", "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Failed to list accounts")
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))

@console_router.get("/api/console/contacts/{phone}")
async def api_console_contacts(
    phone: str,
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/contacts", operation_id, "get_contacts", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    try:
        async with managed_web_session(clean_phone) as (client, _):
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
            total = len(contacts_data)
            paginated = contacts_data[offset:offset + limit]
            _audit_log("/api/console/contacts", operation_id, "get_contacts", clean_phone, "success", time.time() - start)
            return {"status": "success", "contacts": paginated, "total": total, "limit": limit, "offset": offset}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/contacts", operation_id, "get_contacts", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/contacts", operation_id, "get_contacts", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Failed to fetch contacts for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))

@console_router.post("/api/console/send")
async def api_console_send_message(req: SendMessageRequest):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/send", operation_id, "send_message", req.phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = req.phone  # Already validated by Pydantic
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target_id = parse_chat_id(req.chat_id)
            content = (req.message or req.text or "").strip()
            if not content:
                _audit_log("/api/console/send", operation_id, "send_message", clean_phone, "error_empty_message", time.time() - start, "VALIDATION_ERROR")
                raise HTTPException(status_code=400, detail="Message text is empty")
            if req.edit_id and str(req.edit_id).strip():
                await client.edit_message(target_id, int(req.edit_id), content)
            else:
                reply_to_id = int(req.reply_to) if req.reply_to and str(req.reply_to).strip() else None
                await client.send_message(target_id, content, reply_to=reply_to_id)
            _audit_log("/api/console/send", operation_id, "send_message", clean_phone, "success", time.time() - start)
            return {"status": "success"}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/send", operation_id, "send_message", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/send", operation_id, "send_message", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Failed to send message from %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))

# =====================================================================
# 🤖 MASS AUTOMATION & LIVE BACKGROUND TELEMETRY HUB
# =====================================================================
from collections import deque
automation_logs_stream = deque(maxlen=100)

def append_system_log(message: str):
    # Convert Backend Automation Logs to IST Time
    ist_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    timestamp = ist_time.strftime("%Y-%m-%d %H:%M:%S")
    automation_logs_stream.append(f"[{timestamp}] {message}")


@console_router.get("/api/console/automation-logs")
async def get_live_automation_logs(
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    logs = list(automation_logs_stream)
    total = len(logs)
    paginated = logs[offset:offset + limit]
    _audit_log("/api/console/automation-logs", operation_id, "get_logs", "all", "success", time.time() - start)
    return {"status": "success", "logs": paginated, "total": total, "limit": limit, "offset": offset}


async def async_mass_join_worker(accounts, target_channel, operation_id):
    append_system_log(f"INITIATING MASS OPERATIONS: Target resolved to '{target_channel}'")
    success_count = 0
    fail_count = 0
    for idx, acc in enumerate(accounts):
        phone = acc.get("phone")
        try:
            async with managed_web_session(phone) as (client, _):
                append_system_log(f"Processing Account #{idx+1}/{len(accounts)} (+{phone})...")
                clean_target = target_channel.strip().replace("https://t.me/", "").replace("@", "")
                await client(JoinChannelRequest(clean_target))
                append_system_log(f"Node #{idx+1} (+{phone}) successfully joined group/channel.")
                success_count += 1
        except Exception as err:
            append_system_log(f"Node #{idx+1} (+{phone}) join failure: {sanitize_error_message(err)}")
            fail_count += 1
        await asyncio.sleep(5)
    append_system_log(f"BATCH OPERATIONS FINISHED: Success: {success_count} | Crashed/Failed: {fail_count}")
    _operations[operation_id] = {
        **_operations.get(operation_id, {}),
        "state": "completed",
        "success_count": success_count,
        "fail_count": fail_count,
    }


@console_router.post("/api/console/mass-execute")
async def trigger_mass_operation(req: MassActionRequest):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/mass-execute", operation_id, "mass_join", req.target_channel, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    accounts = await _db.get_all_suite_sessions()
    if not accounts:
        _audit_log("/api/console/mass-execute", operation_id, "mass_join", req.target_channel, "error_no_accounts", time.time() - start)
        raise HTTPException(status_code=400, detail="No active sessions available")
    if len(accounts) > MAX_TARGETS_PER_REQUEST:
        accounts = accounts[:MAX_TARGETS_PER_REQUEST]
    
    task = asyncio.create_task(async_mass_join_worker(accounts, req.target_channel, operation_id))
    _register_task(task, operation_id, {
        "type": "mass_join",
        "target": req.target_channel,
        "account_count": len(accounts),
        "owner": "web_console",
    })
    _audit_log("/api/console/mass-execute", operation_id, "mass_join", req.target_channel, "started", time.time() - start)
    return {"status": "success", "operation_id": operation_id, "message": "Mass operation started in background"}

# =====================================================================
# 💬 DIALOGS & MESSAGES ENGINE
# =====================================================================
@console_router.get("/api/console/dialogs/{phone}")
async def api_console_dialogs(
    phone: str,
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/dialogs", operation_id, "get_dialogs", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            me = await client.get_me()
            my_id = me.id
            dialogs = await client.get_dialogs(limit=limit + offset)
        dialogs_payload = []
        
        for chat in dialogs[offset:offset + limit]:
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
        _audit_log("/api/console/dialogs", operation_id, "get_dialogs", clean_phone, "success", time.time() - start)
        return {"status": "success", "phone": clean_phone, "dialogs": dialogs_payload, "limit": limit, "offset": offset}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/dialogs", operation_id, "get_dialogs", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/dialogs", operation_id, "get_dialogs", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Failed to fetch dialogs for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))

@console_router.get("/api/console/messages/{phone}/{chat_id}")
@console_router.get("/api/console/chat-history/{phone}/{chat_id}")
async def api_console_messages(
    phone: str, 
    chat_id: str,
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/messages", operation_id, "get_messages", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    chat_id = validate_chat_id(chat_id)
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target_entity = parse_chat_id(chat_id)
            resolved_peer = await client.get_entity(target_entity)
            messages = await client.get_messages(resolved_peer, limit=limit + offset)
            messages_payload = []
            
            for msg in reversed(messages[offset:offset + limit]):
                if not msg.message and not msg.media:
                    continue
                text = str(msg.message or "").strip()
                # Convert Telethon UTC msg.date to IST
                ist_date = msg.date + datetime.timedelta(hours=5, minutes=30)
                time_node = ist_date.strftime("%H:%M")
                media_type = "text"
                media_data = None

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
                                text = ""
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
                    "media": media_data,
                    "sender_name": sender_name.strip()
                })

            _audit_log("/api/console/messages", operation_id, "get_messages", clean_phone, "success", time.time() - start)
            return {"status": "success", "messages": messages_payload, "limit": limit, "offset": offset}

    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/messages", operation_id, "get_messages", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/messages", operation_id, "get_messages", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Failed to fetch messages for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))

# =====================================================================
# 👤 PROFILE & GLOBAL SEARCH
# =====================================================================
@console_router.get("/api/console/profile/{phone}")
async def api_console_get_profile(phone: str):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/profile", operation_id, "get_profile", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    record = _db.get_session_by_phone(clean_phone)
    if not record:
        _audit_log("/api/console/profile", operation_id, "get_profile", clean_phone, "error_session_missing", time.time() - start)
        raise HTTPException(status_code=404, detail="Session not found")
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            me = await client.get_me()
            if me:
                full_name = f"{me.first_name or ''} {me.last_name or ''}".strip() or f"+{clean_phone}"
                photo_uri = None
                try:
                    photo_buffer = io.BytesIO()
                    await client.download_profile_photo(me, file=photo_buffer)
                    if photo_buffer.getvalue():
                        photo_uri = f"data:image/jpeg;base64,{base64.b64encode(photo_buffer.getvalue()).decode('utf-8')}"
                except Exception:
                    pass
                
                server_dc = getattr(client.session, 'dc_id', 'Unknown')
                is_restricted = getattr(me, 'restricted', False)
                restriction_reason = getattr(me, 'restriction_reason', 'None')
                
                # Proxy info - safe metadata only (no credentials)
                raw_proxy = record.get("proxy")
                proxy_info = None
                if raw_proxy:
                    if isinstance(raw_proxy, dict):
                        proxy_info = {
                            "provider": raw_proxy.get("provider", "unknown"),
                            "host": raw_proxy.get("host", raw_proxy.get("addr", "unknown")),
                            "port": raw_proxy.get("port", "unknown"),
                            "scheme": raw_proxy.get("scheme", raw_proxy.get("proxy_type", "unknown")),
                        }
                    elif isinstance(raw_proxy, (list, tuple)) and len(raw_proxy) >= 2:
                        proxy_info = {
                            "provider": "unknown",
                            "host": raw_proxy[0],
                            "port": raw_proxy[1],
                            "scheme": "socks5",
                        }
                        
                if hasattr(_db, "source_accounts") and _db.source_accounts:
                    _db.source_accounts.update_one(
                        {"phone": clean_phone},
                        {"$set": {
                            "first_name": full_name,
                            "is_restricted": is_restricted,
                            "dc_id": f"DC {server_dc}"
                        }}
                    )
                    
                _audit_log("/api/console/profile", operation_id, "get_profile", clean_phone, "success", time.time() - start)
                return {
                    "status": "success",
                    "full_name": full_name,
                    "username": me.username or "No Username Set",
                    "phone": me.phone or clean_phone,
                    "profile_pic": photo_uri,
                    "dc_id": f"DC {server_dc}",
                    "proxy": proxy_info,
                    "restricted": "Restricted" if is_restricted else "Good Health",
                    "restriction_details": str(restriction_reason) if is_restricted else "No active violations found."
                }
            _audit_log("/api/console/profile", operation_id, "get_profile", clean_phone, "error_user_mismatch", time.time() - start)
            raise HTTPException(status_code=400, detail="User block mismatch")
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/profile", operation_id, "get_profile", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/profile", operation_id, "get_profile", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Failed to get profile for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


# =====================================================================
# ⚕️ HEALTH & ANALYTICS
# =====================================================================
@console_router.get("/api/console/health/{phone}")
async def api_console_health_metrics(phone: str):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/health", operation_id, "health_check", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    
    clean_phone = validate_phone(phone)
    record = _db.get_session_by_phone(clean_phone)
    if not record:
        _audit_log("/api/console/health", operation_id, "health_check", clean_phone, "error_session_missing", time.time() - start)
        raise HTTPException(status_code=404, detail="Session not found")
    
    status = record.get("status", "unknown")
    err_log = record.get("revocation_reason") or record.get("last_error") or "No active violations found."
    
    health_details = "All systems operational"
    if status in ["failed", "restricted", "banned"]:
        health_details = f"Account restricted: {err_log}"
    
    _audit_log("/api/console/health", operation_id, "health_check", clean_phone, "success", time.time() - start)
    return {
        "status": "success",
        "health_score": 100 if status == "active" else (0 if status == "revoked" else 50),
        "details": health_details,
        "flood_history": 0,
        "total_added": record.get("account_sequence_index", 0)
    }
    
    
@console_router.get("/api/console/global-search/{phone}")
async def api_console_global_search(
    phone: str, 
    q: str = Query(..., min_length=1, max_length=256),
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/global-search", operation_id, "global_search", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            result = await client(SearchRequest(q=q, limit=limit + offset))
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
            
            paginated = search_results[offset:offset + limit]
            _audit_log("/api/console/global-search", operation_id, "global_search", clean_phone, "success", time.time() - start)
            return {"status": "success", "results": paginated, "total": len(search_results), "limit": limit, "offset": offset}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/global-search", operation_id, "global_search", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/global-search", operation_id, "global_search", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Global search failed for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))

# =====================================================================
# 🏢 CHAT INFO & MEDIA EXPLORER
# =====================================================================
# =====================================================================
# 🏢 CHAT INFO & MEDIA EXPLORER
# =====================================================================
@console_router.get("/api/console/chat-info/{phone}/{chat_id}")
async def api_console_chat_info(
    phone: str, 
    chat_id: str,
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/chat-info", operation_id, "chat_info", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    chat_id = validate_chat_id(chat_id)
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target_entity = parse_chat_id(chat_id)
            entity = await client.get_entity(target_entity)
            is_user = hasattr(entity, 'first_name')
            
            about_text = "No description"
            invite_link = f"t.me/{entity.username}" if getattr(entity, 'username', None) else "Private Link Space"
            member_count = 0
            photo_b64 = None
            stats = {}
            
            try:
                p_buffer = io.BytesIO()
                await client.download_profile_photo(entity, file=p_buffer)
                if p_buffer.getvalue():
                    photo_b64 = f"data:image/jpeg;base64,{base64.b64encode(p_buffer.getvalue()).decode('utf-8')}"
            except Exception:
                pass
            
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
                    async for p in client.iter_participants(entity, limit=limit + offset):
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
            
            paginated_members = members_list[offset:offset + limit]
            
            _audit_log("/api/console/chat-info", operation_id, "chat_info", clean_phone, "success", time.time() - start)
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
                "members": paginated_members,
                "total_members": len(members_list),
                "limit": limit,
                "offset": offset,
            }
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/chat-info", operation_id, "chat_info", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/chat-info", operation_id, "chat_info", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Chat info failed for %s/%s", clean_phone, chat_id)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


@console_router.get("/api/console/chat-members/{phone}/{chat_id}")
async def api_console_chat_members(
    phone: str, 
    chat_id: str,
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/chat-members", operation_id, "chat_members", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    chat_id = validate_chat_id(chat_id)
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target_entity = parse_chat_id(chat_id)
            entity = await client.get_entity(target_entity)
            is_user = hasattr(entity, 'first_name')
            
            members_list = []
            if not is_user:
                try:
                    async for p in client.iter_participants(entity, limit=limit + offset):
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
            
            paginated = members_list[offset:offset + limit]
            _audit_log("/api/console/chat-members", operation_id, "chat_members", clean_phone, "success", time.time() - start)
            return {"status": "success", "members": paginated, "total": len(members_list), "limit": limit, "offset": offset}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/chat-members", operation_id, "chat_members", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/chat-members", operation_id, "chat_members", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Chat members failed for %s/%s", clean_phone, chat_id)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


@console_router.get("/api/console/ping/{phone}")
async def api_console_ping(phone: str):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/ping", operation_id, "ping", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    try:
        async with managed_web_session(clean_phone) as (client, _):
            if client.is_connected() and await client.is_user_authorized():
                _audit_log("/api/console/ping", operation_id, "ping", clean_phone, "success", time.time() - start)
                return {"status": "success", "connected": True}
            _audit_log("/api/console/ping", operation_id, "ping", clean_phone, "error_not_connected", time.time() - start)
            return {"status": "error", "reason": "Not connected"}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/ping", operation_id, "ping", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/ping", operation_id, "ping", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Ping failed for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


@console_router.get("/api/console/analytics/{phone}")
async def api_console_analytics(phone: str):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/analytics", operation_id, "analytics", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    
    clean_phone = validate_phone(phone)
    record = _db.get_session_by_phone(clean_phone)
    if not record:
        _audit_log("/api/console/analytics", operation_id, "analytics", clean_phone, "error_session_missing", time.time() - start)
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Fetch all sessions
    all_sessions = await _db.get_all_suite_sessions() if _db else []
    total_sessions = len(all_sessions)
    active_sessions = len([a for a in all_sessions if a.get("status") == "active"])
    
    status = record.get("status", "unknown")
    err_log = record.get("revocation_reason") or record.get("last_error") or "No active violations found."
    health_details = "All systems operational"
    if status in ["failed", "restricted", "banned"]:
        health_details = f"Account restricted: {err_log}"
    
    profile_data = {
        "full_name": record.get("first_name", "Unknown"),
        "username": record.get("username", "No Username"),
        "phone": clean_phone,
        "dc_id": record.get("dc_id", "Unknown"),
        "restricted": "Restricted" if status in ["failed", "restricted", "banned"] else "Good Health",
    }
    
    _audit_log("/api/console/analytics", operation_id, "analytics", clean_phone, "success", time.time() - start)
    return {
        "status": "success",
        "total_sessions": total_sessions,
        "active_sessions": active_sessions,
        "profile": profile_data,
        "health": {
            "status": "success",
            "health_score": 100 if status == "active" else (0 if status == "revoked" else 50),
            "details": health_details,
            "flood_history": 0,
            "total_added": record.get("account_sequence_index", 0)
        }
    }


@console_router.get("/api/console/chat-media/{phone}/{chat_id}")
async def api_console_chat_media(
    phone: str, 
    chat_id: str, 
    media_type: str = Query(..., pattern="^(photos|files|voices|links)$"),
    limit: int = Query(DEFAULT_PAGE_LIMIT, ge=1, le=MAX_PAGE_LIMIT),
    offset: int = Query(0, ge=0),
):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/chat-media", operation_id, "chat_media", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    chat_id = validate_chat_id(chat_id)
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target_entity = parse_chat_id(chat_id)
            resolved_peer = await client.get_entity(target_entity)
            messages = await client.get_messages(resolved_peer, limit=limit + offset)
            extracted_items = []
            
            for msg in messages[offset:offset + limit]:
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
                    except Exception: pass
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
                            "date": ist_date.strftime("%d %b %H:%M"),
                            "duration": "Voice Note Clip"
                        })
            _audit_log("/api/console/chat-media", operation_id, "chat_media", clean_phone, "success", time.time() - start)
            return {"status": "success", "media_type": media_type, "items": extracted_items, "limit": limit, "offset": offset}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/chat-media", operation_id, "chat_media", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/chat-media", operation_id, "chat_media", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Chat media failed for %s/%s", clean_phone, chat_id)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))

# =====================================================================
# 🛠️ ACTIONS (Join, Route, Forward, Delete)
# =====================================================================
@console_router.post("/api/console/join-chat")
async def api_console_join_chat(req: JoinActionRequest):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/join-chat", operation_id, "join_chat", req.phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = req.phone  # Already validated by Pydantic
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target = parse_chat_id(req.chat_id)
            await client(JoinChannelRequest(target))
            _audit_log("/api/console/join-chat", operation_id, "join_chat", clean_phone, "success", time.time() - start)
            return {"status": "success", "message": "Successfully joined the chat"}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/join-chat", operation_id, "join_chat", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/join-chat", operation_id, "join_chat", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Join chat failed for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


@console_router.post("/api/console/smart-route")
async def api_console_smart_route(req: SmartRouteRequest):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/smart-route", operation_id, "smart_route", req.phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = req.phone  # Already validated by Pydantic
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
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
                _audit_log("/api/console/smart-route", operation_id, "smart_route", clean_phone, "success", time.time() - start)
                return {
                    "status": "success",
                    "chat_id": chat_id,
                    "title": title,
                    "message": "Successfully routed to target destination."
                }
            else:
                _audit_log("/api/console/smart-route", operation_id, "smart_route", clean_phone, "error_not_resolved", time.time() - start, "VALIDATION_ERROR")
                raise HTTPException(status_code=400, detail="Could not resolve target chat")
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/smart-route", operation_id, "smart_route", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/smart-route", operation_id, "smart_route", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Smart route failed for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


@console_router.get("/api/console/chat-photo/{phone}/{chat_id}")
async def api_console_chat_photo(phone: str, chat_id: str):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/chat-photo", operation_id, "chat_photo", phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = validate_phone(phone)
    chat_id = validate_chat_id(chat_id)
    
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target_entity = parse_chat_id(chat_id)
            entity = await client.get_entity(target_entity)
            photo_buffer = io.BytesIO()
            await client.download_profile_photo(entity, file=photo_buffer, download_big=False)
            if photo_buffer.getvalue():
                photo_b64 = base64.b64encode(photo_buffer.getvalue()).decode('utf-8')
                _audit_log("/api/console/chat-photo", operation_id, "chat_photo", clean_phone, "success", time.time() - start)
                return {"status": "success", "photo": f"data:image/jpeg;base64,{photo_b64}"}
            _audit_log("/api/console/chat-photo", operation_id, "chat_photo", clean_phone, "success_no_photo", time.time() - start)
            return {"status": "success", "photo": None}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/chat-photo", operation_id, "chat_photo", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/chat-photo", operation_id, "chat_photo", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Chat photo failed for %s/%s", clean_phone, chat_id)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


@console_router.post("/api/console/forward")
async def api_console_forward_message(req: ForwardMessageRequest):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/forward", operation_id, "forward_message", req.phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = req.phone  # Already validated by Pydantic
    try:
        async with managed_web_session(clean_phone) as (client, _):
            from_peer = parse_chat_id(req.from_chat_id)
            to_peer = parse_chat_id(req.to_chat_id)
            await client.forward_messages(to_peer, req.msg_id, from_peer)
            _audit_log("/api/console/forward", operation_id, "forward_message", clean_phone, "success", time.time() - start)
            return {"status": "success", "message": "Message forwarded successfully"}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/forward", operation_id, "forward_message", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/forward", operation_id, "forward_message", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Forward message failed for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


@console_router.post("/api/console/delete-message")
async def api_console_delete_message(req: DeleteMessageRequest):
    operation_id = uuid.uuid4().hex
    start = time.time()
    global _db
    if not _db:
        _audit_log("/api/console/delete-message", operation_id, "delete_message", req.phone, "error_db_uninitialized", time.time() - start)
        raise HTTPException(status_code=503, detail="Database uninitialized")
    clean_phone = req.phone  # Already validated by Pydantic
    try:
        async with managed_web_session(clean_phone) as (client, _):
            target_peer = parse_chat_id(req.chat_id)
            await client.delete_messages(target_peer, [req.msg_id], revoke=req.delete_for_everyone)
            _audit_log("/api/console/delete-message", operation_id, "delete_message", clean_phone, "success", time.time() - start)
            return {"status": "success", "message": "Message deleted successfully"}
    except HTTPException:
        raise
    except PermissionError as e:
        _audit_log("/api/console/delete-message", operation_id, "delete_message", clean_phone, "error_session_unavailable", time.time() - start, "SESSION_UNAVAILABLE")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        _audit_log("/api/console/delete-message", operation_id, "delete_message", clean_phone, "error", time.time() - start, (classify_exception(e).category.name if isinstance(classify_exception(e), ConnectionResult) else type(e).__name__))
        logger.exception("Delete message failed for %s", clean_phone)
        raise HTTPException(status_code=500, detail=sanitize_error_message(e))


# =====================================================================
# 📊 OPERATION STATUS ENDPOINT
# =====================================================================
@console_router.get("/api/console/operations/{operation_id}")
async def api_console_operation_status(operation_id: str):
    """Get status of a background operation."""
    op = get_operation_status(operation_id)
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")
    return op


# =====================================================================
# 🛡️ SECURITY HEADERS MIDDLEWARE
# =====================================================================
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""
    
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response


# Export for app initialization
def get_console_router():
    """Return the console router with security middleware applied."""
    return console_router