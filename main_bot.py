#!/usr/bin/env python3
"""
Ultimate Enterprise Telegram Suite — Master Controller v2.0
Enterprise-Grade Architecture | 100% Feature Parity | Zero Memory Leaks
"""

import os, sys, asyncio, logging, random, time, pathlib, ssl, re, gc, socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any, Callable, Set
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
import uvicorn
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.tl.types import User
from telethon.tl.functions.messages import DeleteHistoryRequest
from telethon.tl.functions.contacts import GetContactsRequest
from telethon.errors import *
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import gc
import httpx

from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
from proxy_manager import ProxyManager
from scraper import MemberScraper
from videochat import CloudVoiceChatEngine
from adder import EnterpriseMemberAdder, AdderState, status_updater_loop
from dmsender import setup_dmsender_handlers
from web_console import console_router, init_console_db, setup_console_routes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MasterSuiteBot")
logging.getLogger("telethon").setLevel(logging.WARNING)

# 🔥 FIX: Silence Telethon's repetitive internal network warnings when testing proxies
logging.getLogger("telethon.network.mtprotosender").setLevel(logging.ERROR)
logging.getLogger("telethon.network.connection.connection").setLevel(logging.ERROR)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ──────────────────────────────────────────────
# TYPED CONFIGURATION
# ──────────────────────────────────────────────
# Ensure defaults for forward-compatibility
CONFIG.setdefault("BOT_TOKEN", os.environ.get("BOT_TOKEN", ""))
CONFIG.setdefault("API_ID", int(os.environ.get("API_ID", 0)))
CONFIG.setdefault("API_HASH", os.environ.get("API_HASH", ""))
CONFIG.setdefault("ADMIN_ID", os.environ.get("ADMIN_ID"))
CONFIG.setdefault("WORKER_NODE_ID", os.environ.get("WORKER_NODE_ID", "worker_01"))

# ──────────────────────────────────────────────
# ENUMS & DATACLASSES
# ──────────────────────────────────────────────
class AccountStatus(str, Enum):
    ACTIVE = "active"
    PENDING = "pending"
    TWOFA_REQUIRED = "2fa_required"
    FAILED = "failed"
    BANNED = "banned"
    RESTRICTED = "restricted"
    REVOKED = "revoked"


class ExplorerFilter(str, Enum):
    ACTIVE = "active"
    REVOKED = "revoked"
    PENDING = "pending"
    TODAY = "today"
    ALL = "all"


@dataclass
class AuthState:
    """Thread-safe state container for active login flows."""
    client: TelegramClient
    phone_code_hash: str
    device: dict
    created_at: float = field(default_factory=time.time)

    def is_expired(self, ttl: int = 300) -> bool:
        return (time.time() - self.created_at) > ttl


@dataclass
class ClientPoolEntry:
    """Wraps a Telethon client with metadata for pool management."""
    client: TelegramClient
    phone: str
    created_at: float
    last_used: float
    device_fingerprint: str

    @property
    def age_seconds(self) -> float:
        return time.time() - self.created_at

    @property
    def idle_seconds(self) -> float:
        return time.time() - self.last_used


# ──────────────────────────────────────────────
# MODIFICATION in GlobalState.initialize()
# ──────────────────────────────────────────────
class GlobalState:
    _instance: Optional['GlobalState'] = None

    def __new__(cls) -> 'GlobalState':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def initialize(self) -> None:
        if self._initialized:
            return
        self._lock = asyncio.Lock()
        self._pool_lock = asyncio.Lock()
        self._auth_lock = asyncio.Lock()
        self._nav_lock = asyncio.Lock()

        # Navigation state
        self.current_page: int = 1
        self.explorer_filter: str = "active"
        self.search_query: Optional[str] = None

        # Auth states (bounded)
        self.auth_states: Dict[str, AuthState] = {}

        # ── 🔥 DYNAMIC POOL SIZE ──
        # Base pool size from config, will auto-scale up to 10% of total accounts
        self._base_pool_max_size: int = CONFIG.get("MAX_POOL_SIZE", 50)
        self._pool_max_size: int = self._base_pool_max_size
        # Client pool (bounded LRU-like)
        self.client_pool: Dict[str, ClientPoolEntry] = {}

        # ── 🔥 STATUS BAR CACHE ──
        self._status_bar_cache: str = ""
        self._status_bar_expires: float = 0.0
        self._status_bar_ttl: int = 30

        # Health check flag
        self.health_check_active: bool = True

        # Background task registry
        self.background_tasks: Set[asyncio.Task] = set()

        self._initialized = True

    # ── 🔥 NEW: Adjust pool size dynamically ──
    async def adjust_pool_size(self, total_accounts: int) -> None:
        """Scale pool to ~10% of total accounts, clamped between base and 200 max."""
        desired = max(self._base_pool_max_size, total_accounts // 10)
        desired = min(desired, CONFIG.get("MAX_POOL_ABSOLUTE", 200))
        async with self._pool_lock:
            self._pool_max_size = desired

    # ── 🔥 NEW: Cached status bar ──

    # ── YEH DO METHODS GlobalState class ke ANDAR DAALO ──
    async def get_status_bar(self, all_sessions: list) -> str:
        """Return cached status bar, recompute only if expired."""
        now = time.time()
        if now < self._status_bar_expires and self._status_bar_cache:
            return self._status_bar_cache
        
        # Cache miss — compute fresh
        total = len(all_sessions)
        active_cnt = sum(1 for x in all_sessions if x.get("status") == AccountStatus.ACTIVE)
        revoked_cnt = sum(1 for x in all_sessions if x.get("status") == AccountStatus.REVOKED)
        pending_cnt = sum(1 for x in all_sessions if x.get("status") in (
            AccountStatus.PENDING, AccountStatus.TWOFA_REQUIRED))
        failed_cnt = sum(1 for x in all_sessions if x.get("status") in (
            AccountStatus.FAILED, AccountStatus.BANNED))
        worker_id = CONFIG.get("WORKER_NODE_ID", "worker_01")
        proxy_count = getattr(proxy_manager, 'working_count', 0)
        
        self._status_bar_cache = (
            "**Workspace Overview**\n"
            f"Total Inventory: `{total}` Accounts\n"
            f"🟢 `{active_cnt}` Active (Good Health)\n"
            f"🟡 `{pending_cnt}` Pending / 2FA\n"
            f"🟠 `{failed_cnt}` Failed / Spam Muted (Recoverable)\n"
            f"🔴 `{revoked_cnt}` Revoked / Dead\n"
            f"Infrastructure: ⚡ Node `{worker_id}` • 🛡️ `{proxy_count}` Proxies Healthy\n"
        )
        self._status_bar_expires = now + self._status_bar_ttl
        return self._status_bar_cache

    async def invalidate_status_bar_cache(self) -> None:
        """Force cache refresh on next call."""
        self._status_bar_expires = 0.0

    # ── Navigation ──
    async def get_nav_state(self) -> dict:
        async with self._nav_lock:
            return {
                "current_page": self.current_page,
                "explorer_filter": self.explorer_filter,
                "search_query": self.search_query,
            }

    async def set_nav_state(self, **kwargs) -> None:
        async with self._nav_lock:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)

    async def set_search_query(self, val: Optional[str]) -> None:
        async with self._nav_lock:
            self.search_query = val

    # ── Auth States ──
    async def get_auth_state(self, phone_key: str) -> Optional[AuthState]:
        async with self._auth_lock:
            state = self.auth_states.get(phone_key)
            if state and state.is_expired():
                # Clean expired
                del self.auth_states[phone_key]
                return None
            return state

    async def set_auth_state(self, phone_key: str, state: AuthState) -> None:
        async with self._auth_lock:
            # Disconnect any old client before overwriting
            old = self.auth_states.get(phone_key)
            if old and old.client is not state.client:
                try:
                    await old.client.disconnect()
                except Exception:
                    pass
            self.auth_states[phone_key] = state

    async def pop_auth_state(self, phone_key: str) -> Optional[AuthState]:
        async with self._auth_lock:
            return self.auth_states.pop(phone_key, None)

    async def cleanup_stale_auth_states(self) -> int:
        async with self._auth_lock:
            stale = [k for k, v in self.auth_states.items() if v.is_expired()]
            for k in stale:
                state = self.auth_states.pop(k)
                try:
                    await state.client.disconnect()
                except Exception:
                    pass
            return len(stale)

    # ── Client Pool ──
    async def pool_get(self, phone: str) -> Optional[ClientPoolEntry]:
        async with self._pool_lock:
            entry = self.client_pool.get(phone)
            if entry:
                entry.last_used = time.time()
            return entry

    async def pool_set(self, phone: str, entry: ClientPoolEntry) -> None:
        async with self._pool_lock:
            # Evict oldest if at capacity
            if len(self.client_pool) >= self._pool_max_size:
                oldest_key = min(self.client_pool, key=lambda k: self.client_pool[k].last_used)
                oldest = self.client_pool.pop(oldest_key)
                try:
                    await oldest.client.disconnect()
                except Exception:
                    pass
                logger.debug(f"Evicted oldest pooled client: {oldest_key}")
            self.client_pool[phone] = entry

    async def pool_remove(self, phone: str) -> Optional[ClientPoolEntry]:
        async with self._pool_lock:
            entry = self.client_pool.pop(phone, None)
            if entry:
                try: await entry.client.disconnect()
                except Exception: pass
            return entry

    async def pool_cleanup_stale(self, max_idle: float = 3600) -> int:
        """Remove clients idle for more than max_idle seconds."""
        async with self._pool_lock:
            now = time.time()
            stale = [k for k, v in self.client_pool.items() if (now - v.last_used) > max_idle]
            for k in stale:
                entry = self.client_pool.pop(k)
                try:
                    await entry.client.disconnect()
                except Exception:
                    pass
            return len(stale)

    async def pool_clear(self) -> int:
        async with self._pool_lock:
            count = len(self.client_pool)
            for entry in self.client_pool.values():
                try:
                    await entry.client.disconnect()
                except Exception:
                    pass
            self.client_pool.clear()
            return count

    async def pool_size(self) -> int:
        async with self._pool_lock:
            return len(self.client_pool)

    # ── Health Check ──
    async def is_health_check_active(self) -> bool:
        async with self._lock:
            return self.health_check_active

    async def set_health_check(self, active: bool) -> None:
        async with self._lock:
            self.health_check_active = active

    # ── Background Tasks ──
    def register_task(self, task: asyncio.Task) -> None:
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)


# Initialize global state
GLOBAL = GlobalState()
GLOBAL.initialize()


# ──────────────────────────────────────────────
# DATABASE & SERVICE INSTANCES
# ──────────────────────────────────────────────
db = SuiteDatabase()
proxy_manager = ProxyManager()
scraper_engine = MemberScraper(db)
voice_engine = CloudVoiceChatEngine(db)
adder_engine = EnterpriseMemberAdder(db, proxy_manager)

bot = TelegramClient(StringSession(), CONFIG["API_ID"], CONFIG["API_HASH"])
dm_engine = setup_dmsender_handlers(bot, db, proxy_manager)

# ──────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────

from telethon.errors import QueryIdInvalidError

async def safe_answer(event, text=None, alert=False):
    """Safely answers callback queries, ignoring expired ID errors."""
    try:
        await event.answer(text, alert=alert)
    except QueryIdInvalidError:
        pass  # Query already expired, ignore silently
    except Exception:
        pass

def get_proxy_count() -> int:
    try:
        return proxy_manager.working_count
    except AttributeError:
        return len(proxy_manager.working_proxies) if hasattr(proxy_manager, 'working_proxies') else 0

def clean_phone_input(phone_str: str) -> str:
    """Sanitize and normalize phone number to international format."""
    if not phone_str:
        return ""
    digits_only = "".join(c for c in str(phone_str) if c.isdigit())
    if not digits_only:
        return ""
    # Indian fallback: if 10 digits and doesn't start with 91
    if not digits_only.startswith("91") and len(digits_only) == 10:
        digits_only = "91" + digits_only
    return f"+{digits_only}"


def normalize_phone(phone: str) -> str:
    """Strip everything non-digit (strip +)."""
    return "".join(c for c in phone if c.isdigit())


def is_admin(sender_id) -> bool:
    admin_id = CONFIG.get("ADMIN_ID")
    if not admin_id:
        logger.error("🚨 ADMIN_ID is not set! Rejecting all commands for security.")
        return False
    return str(sender_id) == str(admin_id).strip()


def safe_session_str(record: dict) -> Optional[str]:
    """Normalize session key: try session_string, then session."""
    return record.get("session_string") or record.get("session")


def get_device_profile(record: dict) -> dict:
    """Extract device profile from record with fallback."""
    meta = record.get("device_metadata") or {}
    return {
        "device_model": meta.get("device_model") or record.get("device_model", "PC 64bit"),
        "system_version": meta.get("system_version") or record.get("system_version", "Windows 11"),
        "app_version": meta.get("app_version") or record.get("app_version", "4.8.4"),
    }


def get_account_label(acc: dict) -> str:
    """Build button label for account explorer."""
    phone_num = str(acc.get("phone", ""))
    status_val = acc.get("status", AccountStatus.PENDING)
    status_icon = {
        AccountStatus.ACTIVE: "🟢",
        AccountStatus.REVOKED: "🔴",
        AccountStatus.PENDING: "🟡",
        AccountStatus.TWOFA_REQUIRED: "🟡",
        AccountStatus.FAILED: "🟠",
        AccountStatus.BANNED: "🔴",
        AccountStatus.RESTRICTED: "🟠",
    }.get(status_val, "⚪")

    first_name = str(acc.get("first_name") or "").strip()
    name_lbl = f"👤 {first_name} | " if first_name and first_name != "None" else ""

    login_time_raw = acc.get("authenticated_at") or acc.get("timestamp") or acc.get("last_updated")
    if isinstance(login_time_raw, (int, float)):
        login_time_raw = datetime.utcfromtimestamp(login_time_raw)
    
    # 🔥 FIX: Convert DB UTC time to IST for Explorer display UI
    if isinstance(login_time_raw, datetime):
        ist_time = login_time_raw + timedelta(hours=5, minutes=30)
        date_str = ist_time.strftime("%d-%m-%Y | %H:%M")
    else:
        date_str = "N/A Date"

    return f"{status_icon} {name_lbl}+{phone_num} • 🗓️ {date_str}"



async def build_premium_status_bar(all_sessions: list) -> str:
    """Cache-enabled SaaS-style operational summary. Async wrapper for GLOBAL cache."""
    return await GLOBAL.get_status_bar(all_sessions)

# ──────────────────────────────────────────────
# CLIENT FACTORY (with device fingerprint preservation)
# ──────────────────────────────────────────────

async def create_authenticated_client(record: dict) -> Optional[TelegramClient]:
    """Create a Telethon client from a DB record, preserving device fingerprint."""
    session_str = safe_session_str(record)
    if not session_str:
        return None
    device = get_device_profile(record)
    api_id = int(record.get("api_id", CONFIG["API_ID"]))
    api_hash = str(record.get("api_hash", CONFIG["API_HASH"]))
    client = TelegramClient(
        StringSession(session_str),
        api_id=api_id,
        api_hash=api_hash,
        device_model=device["device_model"],
        system_version=device["system_version"],
        app_version=device["app_version"],
        timeout=10,
        entity_cache_limit=100,
        sequential_updates=False,
        receive_updates=False,
    )
    return client


async def connect_client(client: TelegramClient, retries: int = 1) -> bool:
    """Connect a client with retries. BLAZING FAST FAIL."""
    for attempt in range(retries):
        try:
            if not client.is_connected():
                await asyncio.wait_for(client.connect(), timeout=10.0)
            return True
        except Exception as e:
            # Catching broad Exception ensures Telethon-specific connection errors 
            # are caught, allowing proper cleanup to prevent "Future exception" warnings.
            try: 
                await client.disconnect()
            except Exception:
                pass
            if attempt == retries - 1:
                logger.warning(f"Failed to connect client after {retries} attempts: {e}")
                return False
            await asyncio.sleep(0.5)


@asynccontextmanager
async def managed_client(record: dict, use_pool: bool = True):
    """
    Context manager for Telethon client lifecycle.
    Uses pool for reuse, ensures cleanup.
    """
    phone = normalize_phone(str(record.get("phone", "")))
    pool = GLOBAL.client_pool if use_pool else None
    entry = await GLOBAL.pool_get(phone) if pool else None
    proxy_dict = record.get("proxy")

    if entry and entry.client:
        client = entry.client
        # Quick liveness check
        try:
            if not client.is_connected():
                if not await connect_client(client):
                    raise ConnectionError("Reconnect failed")
            yield client
            entry.last_used = time.time()
            return
        except Exception:
            # Pooled client is dead, evict and fall through
            await GLOBAL.pool_remove(phone)
            try:
                await client.disconnect()
            except Exception:
                pass

    # Create fresh client
    device = get_device_profile(record)
    api_id = int(record.get("api_id", CONFIG["API_ID"]))
    api_hash = str(record.get("api_hash", CONFIG["API_HASH"]))
    session_str = safe_session_str(record)

    client = TelegramClient(
        StringSession(session_str),
        api_id=api_id,
        api_hash=api_hash,
        device_model=device["device_model"],
        system_version=device["system_version"],
        app_version=device["app_version"],
        timeout=5.0,
        connection_retries=1,
        proxy=proxy_dict,
        entity_cache_limit=100,          # limit entity cache to reduce RAM
        sequential_updates=False,        # avoid processing updates sequentially
        receive_updates=False,           # we don't need live updates in most operations
    )

    try:
        if not await connect_client(client):
            raise ConnectionError("Initial connect failed")
        if not await client.is_user_authorized():
            raise SessionRevokedError(request=None)

        # Add to pool
        if pool:
            await GLOBAL.pool_set(phone, ClientPoolEntry(
                client=client,
                phone=phone,
                created_at=time.time(),
                last_used=time.time(),
                device_fingerprint=f"{device['device_model']}|{device['system_version']}",
            ))

        yield client
    finally:
        # If NOT using pool, disconnect immediately
        if not pool:
            try:
                await client.disconnect()
            except Exception:
                pass


# ──────────────────────────────────────────────
# OTP HANDLER REGISTRY (avoid duplicate listeners)
# ──────────────────────────────────────────────


def ensure_otp_listener(client: TelegramClient, phone_key: str) -> None:
    """Register OTP listener only once per phone key."""
    if getattr(client, "_otp_registered", False):
        return
    client._otp_registered = True

    @client.on(events.NewMessage(from_users=777000))
    async def telegram_service_handler(event) -> None:
        if event.message and event.message.message:
            try:
                db.log_received_otp(phone_key, "777000", event.message.message)
                logger.debug(f"OTP captured for {phone_key}")
            except Exception as e:
                logger.error(f"Failed to log OTP for {phone_key}: {e}")


async def fetch_past_otps(client: TelegramClient, phone_key: str) -> None:
    """Fetch recent OTP messages from Telegram service."""
    try:
        past_messages = await client.get_messages(777000, limit=3)
        for msg in past_messages:
            if msg and msg.message:
                db.log_received_otp(phone_key, "777000", msg.message)
    except Exception as e:
        logger.debug(f"Past OTP fetch failed for {phone_key}: {e}")


# ──────────────────────────────────────────────
# SHARED LOGIN PROCESS (STRICT PROXY ROTATION)
# ──────────────────────────────────────────────

async def shared_login_process(phone: str) -> dict:
    """
    Send login code request strictly through proxy.
    Rotates through multiple healthy proxies if one fails.
    No direct internet fallback to protect IP.
    """
    clean_phone = normalize_phone(phone)
    existing = db.get_session_by_phone(clean_phone)

    device = get_device_profile(existing) if existing and existing.get("device_model") else (
        random.choice(DEVICE_PROFILES) if DEVICE_PROFILES else {}
    )

    string_session = StringSession()
    
    max_attempts = 4  # 🔥 System 4 alag-alag proxies try karega
    last_error = None

    for attempt in range(1, max_attempts + 1):
        proxy_node = None
        raw_proxy = None
        
        # 1. Naye ProxyManager se properly proxy fetch karein
        raw_proxy = proxy_manager.get_proxy()
                
        # 2. Fallback: Agar working proxy na ho toh list se random proxy uthayein
        if not raw_proxy and proxy_manager.proxies:
            raw_proxy = random.choice(proxy_manager.proxies)
            
        if raw_proxy:
            proxy_node = {
                "proxy_type": raw_proxy.get("type", "socks5"),
                "addr": raw_proxy.get("host"),
                "port": raw_proxy.get("port"),
                "username": raw_proxy.get("username") or None,
                "password": raw_proxy.get("password") or None,
                "rdns": True
            }

        client = TelegramClient(
            string_session,
            api_id=CONFIG["API_ID"],
            api_hash=CONFIG["API_HASH"],
            device_model=device.get("device_model", "PC 64bit"),
            system_version=device.get("system_version", "Windows 11"),
            app_version=device.get("app_version", "4.8.4"),
            timeout=10.0,           
            connection_retries=1,   
            request_retries=1,      
            proxy=proxy_node,
        )

        try:
            async def _proxy_req():
                await client.connect()
                return await client.send_code_request(phone)
                
            send_code_result = await asyncio.wait_for(_proxy_req(), timeout=20.0)
            code_hash = send_code_result.phone_code_hash

            db.save_pending_session(clean_phone, string_session.save(), AccountStatus.PENDING, code_hash, device)
            
            proxy_label = f"{raw_proxy.get('host') or raw_proxy.get('addr')}:{raw_proxy.get('port')}" if raw_proxy else "Direct / None"
            
            return {
                "status": "code_sent",
                "phone": phone,
                "db_clean_phone": clean_phone,
                "code_hash": code_hash,
                "device": device,
                "client": client,
                "proxy_used": proxy_label,
            }
        except Exception as e:
            last_error = e
            try:
                await client.disconnect()
            except Exception:
                pass
            
            # 🔥 PUNISH DEAD PROXY: Naye architecture ke method ko call karein
            if raw_proxy:
                proxy_manager.mark_failed(raw_proxy)
                
            logger.warning(f"Proxy attempt {attempt}/{max_attempts} failed ({proxy_node.get('addr') if proxy_node else 'None'}). Rotating to next proxy...")
            continue  

    logger.error(f"Strict Proxy Policy: Exhausted all {max_attempts} attempts. Last error: {str(last_error)}")
    raise Exception(f"Proxy Connection Failed! 🛑 System ne {max_attempts} alag-alag proxies try kiye par sabne connection drop kar diya. Real IP secure rakha gaya hai.")

# ──────────────────────────────────────────────
# 1. HELP PANEL
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/help'))
async def master_help_panel(event) -> None:
    if not is_admin(event.sender_id):
        return
    all_sessions = await db.get_all_suite_sessions()
    status_bar = await build_premium_status_bar(all_sessions)

    text = (
        "🏢 **Telegram Console**\n\n"
        f"{status_bar}\n"
        "**Workspaces**"
    )
    buttons = [
        [Button.inline("Accounts", data="nav_lvl1_accounts"),
         Button.inline("Monitoring", data="nav_lvl1_diag")],
        [Button.inline("Extraction", data="nav_lvl1_data"),
         Button.inline("Campaigns", data="nav_lvl1_campaigns")],
        [Button.inline("Search", data="nav_lvl1_search"),
         Button.inline("Analytics", data="nav_lvl1_stats")],
    ]
    await event.reply(text, buttons=buttons)


# ──────────────────────────────────────────────
# 2. START PANEL (same layout as help)
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/start'))
async def master_start_panel(event) -> None:
    if not is_admin(event.sender_id):
        return
    all_sessions = await db.get_all_suite_sessions()
    status_bar = await build_premium_status_bar(all_sessions)

    text = (
        "🏢 **Telegram Console**\n\n"
        f"{status_bar}\n"
        "Welcome to the administration workspace. Select a core engine module below to begin operations."
    )
    buttons = [
        [Button.inline("Accounts", data="nav_lvl1_accounts"),
         Button.inline("Monitoring", data="nav_lvl1_diag")],
        [Button.inline("Extraction", data="nav_lvl1_data"),
         Button.inline("Campaigns", data="nav_lvl1_campaigns")],
        [Button.inline("Search", data="nav_lvl1_search"),
         Button.inline("Analytics", data="nav_lvl1_stats")],
    ]
    await event.reply(text, buttons=buttons)


# ──────────────────────────────────────────────
# 3. CENTRALIZED UI ROUTER
# ──────────────────────────────────────────────

@bot.on(events.CallbackQuery)
async def centralized_ui_router(event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Access Denied.", alert=True)
        return

    route = event.data.decode('utf-8')
    all_sessions = await db.get_all_suite_sessions()
    status_bar = await build_premium_status_bar(all_sessions)
    back_to_lvl1 = [[Button.inline("Back", data="nav_lvl1_main")]]

    # ── LEVEL 1: MAIN ──
    if route == "nav_lvl1_main":
        text = (
            "🏢 **Telegram Console**\n\n"
            f"{status_bar}\n"
            "**Workspaces**"
        )
        buttons = [
            [Button.inline("Accounts", data="nav_lvl1_accounts"),
             Button.inline("Monitoring", data="nav_lvl1_diag")],
            [Button.inline("Extraction", data="nav_lvl1_data"),
             Button.inline("Campaigns", data="nav_lvl1_campaigns")],
            [Button.inline("Search", data="nav_lvl1_search"),
             Button.inline("Analytics", data="nav_lvl1_stats")],
        ]
        await safe_edit(event, text, buttons)

    # ── LEVEL 1: ACCOUNTS ──
    elif route == "nav_lvl1_accounts":
        text = (
            "**Accounts Administration**\n\n"
            f"{status_bar}\n"
            "Manage your unified account pool and synchronization tasks."
        )
        buttons = [
            [Button.inline("Login New Account", data="action_init_login"),
             Button.inline("Account Explorer", data="nav_lvl2_explorer")],
            [Button.inline("Reload Sessions", data="action_trigger_reload"),
             Button.inline("Clean Revoked", data="action_trigger_clean")],
            [Button.inline("🏥 Health Scan & Recover Muted", data="action_health_scan")],
            [Button.inline("⬅️ Back to Main Console", data="nav_lvl1_main")],
        ]
        await event.edit(text, buttons=buttons)

    # ── ACTION: INIT LOGIN ──
    elif route == "action_init_login":
        await GLOBAL.set_search_query("AWAITING_LOGIN_INPUT")
        await event.edit(
            "📱 **Manual Account Authentication Wizard**\n\n"
            "Kripya niche chat box mein apna full target phone number send karein.\n"
            "👉 **Format Example:** `+919430163152` ya `919430163152`",
            buttons=[[Button.inline("❌ Cancel Operations", data="nav_lvl1_accounts")]],
        )
        await event.answer()

    # ── LEVEL 1: DATA EXTRACTION ──
    elif route == "nav_lvl1_data":
        scraped_rows = await db.count_scraped_data()
        text = (
            "🛰️ **CORE DATA EXTRACTION CONTROL ROOM**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{status_bar}\n"
            "📦 **DATA STORAGE REPOSITORY SNAPSHOT:**\n"
            f"• Synchronized Database Pool: `{scraped_rows}` unique profiles saved\n\n"
            "⚡ **LIVE SCRAPER GRID MODULES:**\n"
            "👉 *Copy parameters to run directly inside the chat window:*\n\n"
            "🆔 **Targeted Specific Account Scraper:**\n"
            "• `/scrape_group_all <group_link> <phone_number>`\n\n"
            "🔹 **Global Aggregate Full Scrape:**\n"
            "• `/scrape_all <group_link>`\n\n"
            "🔹 **Scrape via Group ID (Direct Access):**\n"
            "• `/scrape_from_group_id <group_id>`\n\n"
            "🔹 **Aggressive 24h Active Scan:**\n"
            "• `/scrape_active_24h <group_link>`\n\n"
            "🔹 **7-Day Activity Interval Crawler:**\n"
            "• `/scrape_weekly <group_link>`\n\n"
            "🔹 **Deep Interaction Log Analyzer:**\n"
            "• `/scrape_hidden <group_link>`\n\n"
            "🔹 **Live VoiceChat Call Tracker:**\n"
            "• `/scrape_from_voicechat <group_link>`\n\n"
            "📥 **Direct Contacts Utility Engine:**\n"
            "• `/contact_scraper <phone_number>`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        buttons = [
            [Button.inline("📊 Repo Analytics", data="nav_lvl1_stats"),
             Button.inline("🗑️ Clear Scraped Data", data="action_clear_scraped")],
            [Button.inline("⬅️ Back to Main Console", data="nav_lvl1_main")],
        ]
        await event.edit(text, buttons=buttons)

    # ── LEVEL 1: CAMPAIGNS ──
    elif route == "nav_lvl1_campaigns":
        text = (
            "⚔️ **CAMPAIGNS & LIVE EXECUTION**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{status_bar}\n"
            "Deploy parallel actions to your account pool using these commands:\n\n"
            "🚀 **Mass Member Adder Engine:** `/addmembers <link>`\n"
            "🎙️ **Voice Chat Cluster Deployment:** `/run_voicechat <link> [count]`\n"
            "💬 **Direct Message Blast Campaigns:** `/send_dmsender`\n"
            "🌍 **Global Mass DM (All Groups):** `/send_dmsender_all`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        buttons = [
            [Button.inline("🛑 Halt DM Sender", data="action_halt_dm"),
             Button.inline("🔇 Stop Voice Chat", data="action_halt_voice")],
            [Button.inline("🛑 Halt Member Adder", data="action_halt_adder")],
            [Button.inline("⬅️ Back to Main Console", data="nav_lvl1_main")],
        ]
        await event.edit(text, buttons=buttons)

    # ── LEVEL 1: SEARCH ──
    elif route == "nav_lvl1_search":
        await GLOBAL.set_search_query("AWAITING_INPUT")
        await event.edit(
            "**Global Search**\n\n"
            "Send any phone number (e.g. `919430163152`), username, or Telegram ID in the chat to look up an account profile.",
            buttons=back_to_lvl1,
        )

    # ── LEVEL 1: STATS ──
    elif route == "nav_lvl1_stats":
        scraped_rows = await db.count_scraped_data()
        text = (
            "**Analytics & System Health**\n\n"
            f"**Storage**\n"
            f"Database: `DB 1 (telegram_bot_db)`\n"
            f"Extracted Users: `{scraped_rows}`\n\n"
            f"**Campaign Status**\n"
            f"Voice Engine: " + ("🟢 Running" if voice_engine.is_running else "⚪ Inactive") + "\n"
            f"Member Adder: " + ("🟢 Running" if adder_engine.is_running else "⚪ Inactive") + "\n\n"
            f"**Infrastructure**\n"
            f"Healthy Proxies: `{get_proxy_count()}`"
        )
        await safe_edit(event, text, buttons=back_to_lvl1)

    # ── LEVEL 1: DIAGNOSTICS ──
    elif route == "nav_lvl1_diag":
        worker_id = CONFIG.get("WORKER_NODE_ID", "worker_01")
        pool_size = await GLOBAL.pool_size()
        health_active = await GLOBAL.is_health_check_active()

        text = (
            "🛡️ **INFRASTRUCTURE DIAGNOSTICS & SYSTEM MONITORING**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{status_bar}\n"
            "🖥️ **RUNTIME INFRASTRUCTURE LOGS:**\n"
            f"• Core Worker Node: `{worker_id}`\n"
            f"• Connection Pool Engine: `{pool_size}` active client threads\n"
            f"• Shared Task Queues: `🟢 SYSTEM IDLE / READY`\n\n"
            f"• Auditor State: `{'🟢 ACTIVE' if health_active else '🔴 PAUSED'}`\n"
            "📡 **LIVE TELEMETRY PARAMETERS:**\n"
            "👉 *Niche diye gaye actions ko trigger karke metrics check karein:*\n\n"
            "🔑 **Dynamic OTP Operations:**\n"
            "• `/otp +91XXXXXXXXXX` (Fetch latest server token)\n"
            "• `/otp_wait +91XXXXXXXXXX` (Live polling monitor)\n\n"
            "🔄 **On-Demand Maintenance Pipelines:**\n"
            "• `/clean_banned_accounts` (Purge dead MTProto nodes)\n"
            "• `/reload_accounts` (Sync sessions folder storage)\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        buttons = [
            [Button.inline("📡 Scan Proxies", data="diag_proxy_health"),
             Button.inline("⏳ Runtime Stats", data="diag_runtime_stats")],
            [Button.inline("📨 View Last OTP", data="diag_otp_view"),
             Button.inline("🚨 OTP Live Wait", data="diag_otp_wait")],
            [Button.inline("⏸️ Pause Auditor", data="diag_pause_auditor"),
             Button.inline("▶️ Resume Auditor", data="diag_resume_auditor")],
            [Button.inline("⬅️ Return to Master Console", data="nav_lvl1_main")],
        ]
        await event.edit(text, buttons=buttons)

    # ── LEVEL 2: ACCOUNT EXPLORER ──
    elif route.startswith("nav_lvl2_explorer") or route.startswith("set_exp_"):
        if "set_exp_" in route:
            filter_mode = route.replace("set_exp_", "")
            await GLOBAL.set_nav_state(explorer_filter=filter_mode, current_page=1)

        nav_state = await GLOBAL.get_nav_state()
        current_filter = nav_state["explorer_filter"]
        page = nav_state["current_page"]

        # Filter
        now = datetime.utcnow()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        filter_map = {
            "active": lambda x: x.get("status") == AccountStatus.ACTIVE,
            "revoked": lambda x: x.get("status") == AccountStatus.REVOKED,
            "pending": lambda x: x.get("status") in (AccountStatus.PENDING, AccountStatus.TWOFA_REQUIRED),
            "today": lambda x: (
            # 🔥 FIX: Strictly check original login/creation time, ignore health checks
            (last_up := x.get("authenticated_at") or x.get("timestamp") or x.get("last_updated")) is not None and
            (isinstance(last_up, datetime) and last_up >= today_start or
             isinstance(last_up, (int, float)) and datetime.utcfromtimestamp(last_up) >= today_start)
            ),
        }
        pred = filter_map.get(current_filter, lambda x: True)
        filtered = [x for x in all_sessions if pred(x)]

        header_map = {
            "active": "Active Matrix",
            "revoked": "Revoked Pool",
            "pending": "Pending Interceptions",
            "today": "Today's Logins",
        }
        header_lbl = header_map.get(current_filter, "All Accounts")

        ITEMS_PER_PAGE = 8
        total_items = len(filtered)
        total_pages = max(1, (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

        # Pagination from route
        if route.startswith("nav_lvl2_explorer_page_"):
            page = int(route.replace("nav_lvl2_explorer_page_", ""))
            await GLOBAL.set_nav_state(current_page=page)

        start_idx = (page - 1) * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        page_items = filtered[start_idx:end_idx]

        explorer_text = (
            "🏢 **ENTERPRISE ACCOUNT INVENTORY EXPLORER**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ **Current View Filter:** `[{header_lbl}]`\n"
            f"📦 **Segment Record:** Showing `{start_idx + 1}–{min(end_idx, total_items)}` of `{total_items}` entries\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Select any identity element node from the catalog below to inspect structural metadata logs."
        )

        explorer_buttons = []
        for acc in page_items:
            btn_label = get_account_label(acc)
            phone_num = acc.get("phone", "")
            explorer_buttons.append([Button.inline(btn_label, data=f"view_prof_{phone_num}")])

        prev_data = f"nav_lvl2_explorer_page_{max(1, page - 1)}"
        next_data = f"nav_lvl2_explorer_page_{min(total_pages, page + 1)}"
        explorer_buttons.append([
            Button.inline("⏮️ Previous", data=prev_data),
            Button.inline(f"PAGE {page} OF {total_pages}", data="noop"),
            Button.inline("Next ⏭️", data=next_data),
        ])
        explorer_buttons.append([
            Button.inline("🟢 Active", data="set_exp_active"),
            Button.inline("🔴 Revoked", data="set_exp_revoked"),
            Button.inline("🟡 Pending", data="set_exp_pending"),
        ])
        explorer_buttons.append([
            Button.inline("📅 Today's Session Matrix Logs", data="set_exp_today"),
        ])
        explorer_buttons.append([
            Button.inline("⬅️ Return to Accounts Admin", data="nav_lvl1_accounts"),
        ])

        await safe_edit(event, explorer_text, explorer_buttons)

    # ── LEVEL 3: ACCOUNT PROFILE ──
    elif route.startswith("view_prof_"):
        target_phone = route.replace("view_prof_", "")
        record = db.get_session_by_phone(target_phone)

        if not record:
            await event.answer("Record not found.", alert=True)
            return

        last_check_raw = record.get("last_checked_time") or record.get("last_updated") or datetime.utcnow()
        time_diff = datetime.utcnow() - last_check_raw if isinstance(last_check_raw, datetime) else timedelta(0)
        minutes_ago = int(time_diff.total_seconds() // 60)
        check_lbl = f"{minutes_ago}m ago" if minutes_ago > 0 else "Just now"

        status_val = record.get("status", AccountStatus.PENDING)
        status_labels = {
            AccountStatus.ACTIVE: ("Active", "🟢"),
            AccountStatus.REVOKED: ("Revoked", "🔴"),
            AccountStatus.PENDING: ("Pending", "🟡"),
            AccountStatus.TWOFA_REQUIRED: ("2FA Needed", "🟡"),
            AccountStatus.FAILED: ("Failed", "🟠"),
            AccountStatus.BANNED: ("Banned", "🔴"),
        }
        status_label, status_icon = status_labels.get(status_val, ("Unknown", "⚪"))

        profile_text = (
            f"**Account Profile**\n\n"
            f"**Identity**\n"
            f"Phone: `+{record.get('phone')}`\n"
            f"Status: {status_icon} {status_label}\n\n"
            f"**Device Configuration**\n"
            f"Model: `{record.get('device_model', 'Ubuntu Desktop')}`\n"
            f"OS: `{record.get('system_version', 'Linux Core')}`\n\n"
            f"**Infrastructure**\n"
            f"Node: `{CONFIG.get('WORKER_NODE_ID', 'worker_01')}` (Batch `{record.get('account_sequence_index', 1)}`)\n"
            f"Proxy: `{record.get('device_metadata', {}).get('proxy', 'IN-MUMBAI-01')}`\n\n"
            f"**Activity**\n"
            f"Last Check: `{check_lbl}`\n"
            f"Error Log: `{record.get('revocation_reason', 'None')}`"
        )
        profile_buttons = [
            [Button.inline("Run Audit", data=f"action_audit_{target_phone}")],
            [Button.inline("Remove Account", data=f"action_logout_{target_phone}")],
            [Button.inline("Back", data="nav_lvl2_explorer")],
        ]
        await event.edit(profile_text, buttons=profile_buttons)

    # ── ACTION: RELOAD SESSIONS ──
    elif route == "action_trigger_reload":
        await event.edit("🚀 **Initializing Matrix Storage Connection...**\nPreparing dynamic accounts reload routing...", buttons=None)
        try:
            result = await db.reload_local_accounts(event=event)
            report = (
                "🔄 **Reload Accounts Complete**\n\n"
                f"📊 **Final Storage Audit:**\n"
                f"• Total Processed: `{result.get('staged', 0) + result.get('failed', 0) + result.get('skipped', 0)}`\n"
                f"• Success Active: `{result.get('migrated', 0)}`\n"
                f"• Defective/Banned: `{result.get('failed', 0)}`\n"
                f"• Missing Sessions: `{result.get('skipped', 0)}`"
            )
            errors = result.get("errors", [])
            for idx, err in enumerate(errors, 1):
                clean_phone = str(err.get('phone', '?')).replace('+', '')
                line = f"`{idx}.` `+{clean_phone}` ➜ {err.get('error', 'Unknown')}\n"
                if len(report) + len(line) > 3900:
                    await event.edit(report)
                    event = await event.respond("⏳ **Processing Next Batch of Issues...**")
                    report = "📋 **Issues Detected (Continued):**\n\n"
                report += line
            await event.edit(report)
        except Exception as e:
            logger.error(f"Reload error: {e}", exc_info=True)
            await event.edit(f"❌ **Account Reload Failed!**\nReason: `{str(e)}`")
        await event.answer()

    # ── ACTION: CLEAN REVOKED ──
    elif route == "action_trigger_clean":
        await event.edit("Running account cleanup workflow...", buttons=None)
        try:
            result = await voice_engine.clean_banned_accounts_handler()
            report = (
                "🔄 **Cleaned Accounts Complete**\n\n"
                "📊 **Final Storage Audit:**\n"
                f"• Total Processed: `{result.get('processed', 0)}`\n"
                f"• Success Active: `{result.get('active', 0)}`\n"
                f"• Defective/Banned: `{result.get('failed', 0)}`\n"
                f"• Missing Sessions: `{result.get('skipped', 0)}`"
            )
            errors = result.get("errors", [])
            for idx, err in enumerate(errors, 1):
                raw_phone = str(err.get('phone', '?')).strip()
                formatted_phone = f"+{raw_phone}" if not raw_phone.startswith("+") else raw_phone
                line = f"`{idx}.` `{formatted_phone}` ➜ {err.get('error', 'Unknown')}\n"
                if len(report) + len(line) > 3900:
                    await event.reply(report)
                    report = "📋 **Issues Detected (Continued):**\n\n"
                report += line
            await event.reply(report)
        except Exception as e:
            logger.error(f"Clean error: {e}", exc_info=True)
            await event.reply(f"Execution Error: {e}")
        await event.answer()

    # ── ACTION: HEALTH SCAN ──
    elif route == "action_health_scan":
        await event.edit("⚕️ **Global Health Scan & Auto-Recovery Initiated!**\n\nScanning `failed` and `restricted` accounts...", buttons=None)

        all_accounts = await db.get_all_accounts_raw()
        failed_accounts = [acc for acc in all_accounts if acc.get("status") in (
            AccountStatus.FAILED, AccountStatus.BANNED, AccountStatus.RESTRICTED)]

        if not failed_accounts:
            await event.edit(
                "✅ **System Health Excellent:** Koi bhi account 'failed' ya 'muted' state mein nahi hai.",
                buttons=[[Button.inline("⬅️ Back", data="nav_lvl1_accounts")]],
            )
            return

        recovered_count = 0
        still_restricted = 0
        scan_sem = asyncio.Semaphore(5)

        async def _ui_scan_worker(acc):
            nonlocal recovered_count, still_restricted
            async with scan_sem:
                phone = normalize_phone(str(acc.get("phone", "")))
                session_str = safe_session_str(acc)
                if not session_str:
                    still_restricted += 1
                    return
                try:
                    async with managed_client(acc, use_pool=False) as client:
                        if await client.is_user_authorized():
                            await client.get_me()
                            await client.send_message("SpamBot", "/start")
                            db.update_session_status(phone, AccountStatus.ACTIVE, client.session.save())
                            recovered_count += 1
                            return
                except Exception:
                    still_restricted += 1
                await asyncio.sleep(0.5)

        await asyncio.gather(*[asyncio.create_task(_ui_scan_worker(a)) for a in failed_accounts])

        report = (
            "🏥 **Health Scan & Recovery Complete!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔍 Scanned: `{len(failed_accounts)}` accounts\n"
            f"🟢 **Successfully Recovered:** `{recovered_count}`\n"
            f"🟠 **Still Restricted:** `{still_restricted}`\n"
        )
        await event.edit(report, buttons=[[Button.inline("⬅️ Back to Accounts", data="nav_lvl1_accounts")]])

    # ── ACTION: HALT VOICE ──
    elif route == "action_halt_voice":
        await event.edit("🛑 **Initiating Voice Chat Emergency Shutdown...**\nClearing processes and releasing cluster locks...", buttons=None)
        try:
            # 🔥 FIXED: Added await to actually execute the async method
            await voice_engine.terminate_voice_cluster()
            await event.reply("🎯 **Voice Chat Cluster Offline!**\n• All WebRTC streams violently terminated.\n• Telethon client node sessions disconnected.\n• Master inventory storage database locks fully cleared.")
        except Exception as halt_err:
            logger.error(f"Force stop error: {halt_err}")
            await event.reply(f"❌ **Emergency Halt Failed:** `{str(halt_err)}`")
        await event.answer()

    # ── ACTION: HALT DM ──
    elif route == "action_halt_dm":
        # 🔥 FIXED: Actually halt the DM campaign
        await event.answer("⏳ Halting DM campaign...", alert=True)
        if dm_engine.is_running:
            dm_engine.halt_campaign()
            await event.edit("🛑 **DM Campaign execution halted.** Releasing system buffers...", buttons=back_to_lvl1)
        else:
            await event.edit("ℹ️ No DM campaign is currently running.", buttons=back_to_lvl1)

    # ── ACTION: HALT ADDER ──
    elif route == "action_halt_adder":
        if adder_engine.is_running:
            adder_engine.halt_engine()
            await safe_answer(event, "🛑 Member Adder halted safely.", alert=True)
            await event.edit("🛑 **Member Adder Campaign Halted.** Active processes terminated and locks released.",
                             buttons=[[Button.inline("⬅️ Back", data="nav_lvl1_campaigns")]])
        else:
            await event.answer("ℹ️ Koi Adder process active nahi hai.", alert=True)

    # ── ACTION: CLEAR SCRAPED ──
    elif route == "action_clear_scraped":
        try:
            total = await db.count_scraped_data()
            await db.clear_scraped_data()
            await event.edit(f"🗑️ **Cloud Database Purged Clean!**\nPurged `{total}` profile rows from repository collections.",
                             buttons=back_to_lvl1)
        except Exception as e:
            await event.edit(f"❌ **Purge Failed:** `{str(e)}`", buttons=back_to_lvl1)
        await event.answer()

    # ── DIAGNOSTICS HOOKS ──
    elif route == "diag_otp_view":
        await event.reply("To view the latest OTP, use the command:\n`/otp +91XXXXXXXXXX`")
        await event.answer()

    elif route == "diag_otp_wait":
        await event.reply("To start the OTP listener, use the command:\n`/otp_wait +91XXXXXXXXXX [duration]`")
        await event.answer()

    elif route == "diag_proxy_health":
        await event.answer("Scanning proxy health...", alert=True)
        
        # Start background testing thread
        proxy_manager.start_background_testing()
        
        working = proxy_manager.working_count
        total = proxy_manager.count
        
        await event.edit(
            f"**✅ Proxy Scan Initiated**\n"
            f"Currently tracking `{working}` healthy proxies out of `{total}` total.\n"
            f"Background refresh active every 20 min.",
            buttons=back_to_lvl1,
        )

    elif route == "diag_runtime_stats":
        pool_size = await GLOBAL.pool_size()
        await event.edit(
            f"**Runtime Status**\n\nActive Workers: `4`\nTask Queue: `Idle`\nCached Connections: `{pool_size}`",
            buttons=back_to_lvl1,
        )

    elif route == "diag_pause_auditor":
        await GLOBAL.set_health_check(False)
        logger.warning("🛑 Auditor paused via UI.")
        await event.edit("⏸️ **Auditor Health Checks PAUSED.**", buttons=back_to_lvl1)
        await event.answer()

    elif route == "diag_resume_auditor":
        await GLOBAL.set_health_check(True)
        logger.info("✅ Auditor resumed via UI.")
        await event.edit("▶️ **Auditor Health Checks RESUMED.**", buttons=back_to_lvl1)
        await event.answer()

    # ── NO-OP ──
    elif route == "noop":
        await event.answer()


# ──────────────────────────────────────────────
# 4. SEARCH / LOGIN TEXT INTERCEPTOR
# ──────────────────────────────────────────────

@bot.on(events.NewMessage)
async def catch_global_search_inputs(event) -> None:
    # Ignore commands
    if event.text and event.text.startswith('/'):
        return
    if not is_admin(event.sender_id):
        return

    nav_state = await GLOBAL.get_nav_state()
    current_state = nav_state.get("search_query")

    # ── LOGIN FLOW ──
    if current_state == "AWAITING_LOGIN_INPUT":
        raw_number = event.text.strip()
        await GLOBAL.set_search_query(None)
        # Build a fake pattern_match for login_handler compatibility
        import types
        event.pattern_match = types.SimpleNamespace(group=lambda: raw_number, group1=raw_number)
        await login_handler(event)
        return

    # ── SEARCH ──
    elif current_state == "AWAITING_INPUT":
        raw_query = event.text.strip().replace("+", "").replace("@", "")
        await GLOBAL.set_search_query(None)

        all_sessions = await db.get_all_suite_sessions()
        matched_doc = None
        for doc in all_sessions:
            phone = str(doc.get("phone", ""))
            device = str(doc.get("device_model", "")).lower()
            if raw_query in phone or raw_query.lower() in device:
                matched_doc = doc
                break

        if matched_doc:
            phone_num = matched_doc.get("phone")
            status_val = matched_doc.get("status", "unknown")
            status_icon = {"active": "🟢", "revoked": "🔴"}.get(status_val, "⚪")
            await event.reply(
                f"**Search Result**\n\n"
                f"Phone: `+{phone_num}`\n"
                f"Status: {status_icon} {status_val.capitalize()}\n"
                f"Device: `{matched_doc.get('device_model', 'N/A')}`",
                buttons=[[Button.inline("Open Account Profile", data=f"view_prof_{phone_num}")]],
            )
        else:
            await event.reply("No account found matching your search criteria.")

async def safe_edit(event, text, buttons=None):
    """Edit a message, silently ignoring 'not modified' and 'query expired' errors."""
    try:
        await event.edit(text, buttons=buttons)
    except (MessageNotModifiedError, QueryIdInvalidError) as e:
        # Log at debug level to avoid clutter
        logger.debug(f"Edit skipped: {e}")            


# ──────────────────────────────────────────────
# 5. LOGIN HANDLER
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/login\s+(.+)'))
async def login_handler(event) -> None:
    if not is_admin(event.sender_id):
        return

    raw_phone = event.pattern_match.group(1)
    phone = clean_phone_input(raw_phone)
    db_clean_phone = normalize_phone(phone)

    await event.reply(f"⏳ **Initializing Login Pipeline for:** `{phone}`...\nConnecting to Telegram Core Matrix...")
    logger.info(f"⚙️ Login request for: {phone}")

    try:
        login_result = await shared_login_process(phone)
        client = login_result["client"]
        device = login_result["device"]
        code_hash = login_result["code_hash"]
        proxy_used = login_result.get("proxy_used", "Direct / None")  # 🔥 Extract proxy info

        # Store auth state with TTL
        await GLOBAL.set_auth_state(db_clean_phone, AuthState(
            client=client,
            phone_code_hash=code_hash,
            device=device,
        ))

        await event.reply(
            f"📥 **OTP Code Sent Successfully!**\n"
            f"👤 **Phone:** `{phone}`\n"
            f"📱 **Device Profile:** `{device.get('device_model', 'Unknown')}`\n"
            f"🛡️ **Proxy Used:** `{proxy_used}`\n\n"  # 🔥 Display proxy details
            f"🔑 Ab input verify karein use karke:\n`/verify {db_clean_phone} CODE`"
        )
        logger.info(f"✅ OTP sent for {phone}")


    except asyncio.TimeoutError:
        logger.error(f"Timeout for {phone}")
        await event.reply("❌ **Network Connection Timeout:** Telegram core server ne response nahi diya. Please check your system internet or proxies.")
    except FloodWaitError as fwe:
        logger.error(f"FloodWait {fwe.seconds}s for {phone}")
        await event.reply(f"❌ **FloodWait:** Telegram ne `{fwe.seconds}` seconds ka wait karne ko kaha hai.")
    except Exception as e:
        logger.error(f"Login error for {phone}: {e}", exc_info=True)
        await event.reply(f"❌ **Login Initiation Failed!**\nReason: `{str(e)}`")


# ──────────────────────────────────────────────
# 6. VERIFY HANDLER
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/verify\s+(\+?\d+)\s+(\d+)'))
async def verify_handler(event) -> None:
    if not is_admin(event.sender_id):
        return

    phone_in = event.pattern_match.group(1)
    code = str(event.pattern_match.group(2)).strip()
    clean_phone_with_plus = clean_phone_input(phone_in)
    db_clean_phone = normalize_phone(clean_phone_with_plus)

    await event.reply(f"⚡ **Submitting Verification Token `{code}`** for `{clean_phone_with_plus}`...")

    # Get auth state
    state = await GLOBAL.get_auth_state(db_clean_phone)
    client = state.client if state else None
    phone_code_hash = state.phone_code_hash if state else None
    device = state.device if state else None

    if not client or not phone_code_hash:
        # Fallback: try DB
        record = db.get_session_by_phone(db_clean_phone)
        if not record or not safe_session_str(record):
            await event.reply("❌ **Error:** No active login state found for this phone. Run `/login` first.")
            return
        device = get_device_profile(record)
        client = TelegramClient(
            StringSession(safe_session_str(record)),
            api_id=CONFIG["API_ID"],
            api_hash=CONFIG["API_HASH"],
            device_model=device["device_model"],
            system_version=device["system_version"],
            app_version=device["app_version"],
        )
        await client.connect()
        phone_code_hash = record.get("phone_code_hash")

    try:
        await client.sign_in(phone=clean_phone_with_plus, code=code, phone_code_hash=phone_code_hash)

        session_str = client.session.save()
        db.update_session_status(db_clean_phone, AccountStatus.ACTIVE.value, session_str)
        if hasattr(db, "save_authorized_session"):
            db.save_authorized_session(db_clean_phone, session_str, AccountStatus.ACTIVE, device, two_fa_password=None)

        # OTP setup
        ensure_otp_listener(client, db_clean_phone)
        await fetch_past_otps(client, db_clean_phone)

        await GLOBAL.pop_auth_state(db_clean_phone)
        await event.reply(f"✅ **Login Successful!**\nSession for `{clean_phone_with_plus}` is now live and saved in DB 1 ecosystem.")

    except SessionPasswordNeededError:
        session_str = client.session.save()
        db.save_authorized_session(db_clean_phone, session_str, AccountStatus.ACTIVE, device, two_fa_password=None)
        # Re-store state (client still alive, not disconnected)
        await GLOBAL.set_auth_state(db_clean_phone, AuthState(client=client, phone_code_hash=phone_code_hash, device=device))
        await event.reply(
            f"🔒 **Two-Factor Authentication (2FA) is Active!**\n"
            f"Execute the following command sequence path:\n"
            f"`/verify_2fa {db_clean_phone} PASSWORD`"
        )
    except Exception as e:
        await event.reply(f"❌ **Verification Failed!**\nTraceback: `{str(e)}`")
        await GLOBAL.pop_auth_state(db_clean_phone)
        try:
            await client.disconnect()
        except Exception:
            pass


# ──────────────────────────────────────────────
# 7. VERIFY 2FA HANDLER
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/verify_2fa\s+(\+\d+|\d+)\s+(.+)'))
async def verify_2fa_handler(event) -> None:
    if not is_admin(event.sender_id):
        return
    phone_in = event.pattern_match.group(1)
    password = str(event.pattern_match.group(2)).strip()
    clean_phone_with_plus = clean_phone_input(phone_in)
    db_clean_phone = normalize_phone(clean_phone_with_plus)

    await event.reply(f"🔒 **Submitting 2FA security matrix password** for `{clean_phone_with_plus}`...")

    state = await GLOBAL.get_auth_state(db_clean_phone)
    client = state.client if state else None
    device = state.device if state else None

    if not client:
        record = db.get_session_by_phone(db_clean_phone)
        if not record:
            await event.reply("❌ **Error:** No session data located for this index.")
            return
        device = get_device_profile(record)
        client = TelegramClient(
            StringSession(safe_session_str(record)),
            api_id=CONFIG["API_ID"],
            api_hash=CONFIG["API_HASH"],
            device_model=device["device_model"],
            system_version=device["system_version"],
            app_version=device["app_version"],
        )
        await client.connect()

    try:
        await client.sign_in(password=password)
        final_session_str = client.session.save()

        # 🔥 FIX: Use save_authorized_session only (it handles status correctly)
        db.save_authorized_session(
            db_clean_phone,
            final_session_str,
            AccountStatus.ACTIVE,      # enum – database.py handles .value
            device,
            two_fa_password=password
        )

        # OTP setup
        ensure_otp_listener(client, db_clean_phone)
        await fetch_past_otps(client, db_clean_phone)

        await GLOBAL.pop_auth_state(db_clean_phone)
        await event.reply(f"🎉 **2FA Bypass Complete & Password Saved!**\n`{clean_phone_with_plus}` status elevated to `active` inside DB 1.")

    except Exception as e:
        await event.reply(f"❌ **2FA Submission Rejected:** `{str(e)}`")
    finally:
        active_state = await GLOBAL.get_auth_state(db_clean_phone)
        if not active_state:
            try:
                await client.disconnect()
            except Exception:
                pass


# ──────────────────────────────────────────────
# 8. DETAILS COMMAND
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/details\s+(.+)'))
async def details_handler(event) -> None:
    if not is_admin(event.sender_id):
        return
    phone_in = event.pattern_match.group(1)
    clean_phone_with_plus = clean_phone_input(phone_in)
    db_clean_phone = normalize_phone(clean_phone_with_plus)

    record = db.get_session_by_phone(db_clean_phone)
    if not record:
        await event.reply(f"❌ No records matching phone context: `{clean_phone_with_plus}` found in DB 1 cluster.")
        return

    text = (
        f"📋 **ACCOUNT PROFILE INFORMATION DETAILS**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📱 **Phone Link:** `+{record.get('phone')}`\n"
        f"⚡ **Status Node:** `{record.get('status', 'unknown').upper()}`\n"
        f"🛠️ **Device Model:** `{record.get('device_model', 'N/A')}`\n"
        f"💻 **OS Environment:** `{record.get('system_version', 'N/A')}`\n"
        f"⚙️ **Client Core App Version:** `{record.get('app_version', 'N/A')}`\n"
        f"🔑 **API ID Configuration:** `{CONFIG['API_ID']}`\n"
        f"📦 **String Session Token (Truncated):** `{(safe_session_str(record) or '')[:25]}...`"
    )
    await event.reply(text)


# ──────────────────────────────────────────────
# 9. LIST COMMAND
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/list'))
async def list_handler(event) -> None:
    if not is_admin(event.sender_id):
        return
    all_sessions = await db.get_all_suite_sessions()
    if not all_sessions:
        await event.reply("📂 **DB 1 Layer is empty.** Active or pending node lines zero.")
        return

    active_lines = []
    pending_lines = []
    for item in all_sessions:
        phone = item.get("phone", "Unknown")
        status = item.get("status", AccountStatus.PENDING)
        dev = item.get("device_model", "Unknown Device")
        line = f"• `+{phone}` — _Device: {dev}_"
        if status == AccountStatus.ACTIVE:
            active_lines.append(line)
        else:
            pending_lines.append(f"{line} [**{status.upper()}**]")

    text = "📊 **TELEGRAM ENGINE SECTOR INVENTORY**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "🟢 **ACTIVE SESSIONS CORE:**\n" + ("\n".join(active_lines) if active_lines else "_No active nodes online._")
    text += "\n\n⏳ **PENDING / 2FA INTERCEPTIONS:**\n" + ("\n".join(pending_lines) if pending_lines else "_No current login registrations pending._")
    await event.reply(text)


# ──────────────────────────────────────────────
# 10. OTP COMMAND
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/otp\s+(.+)'))
async def otp_handler(event) -> None:
    if not is_admin(event.sender_id):
        return
    phone_in = event.pattern_match.group(1)
    clean_phone_with_plus = clean_phone_input(phone_in)
    db_clean_phone = normalize_phone(clean_phone_with_plus)

    latest_log = db.get_latest_otp(db_clean_phone)
    if not latest_log:
        await event.reply(f"📭 No verified logs found inside database schema matching query `+{db_clean_phone}`.")
        return

    text = (
        f"📨 **LATEST SERVICE MESSAGE INTERCEPTED**\n"
        f"📱 **Account Target:** `+{db_clean_phone}`\n"
        f"📡 **Source Node:** `{latest_log.get('sender')}`\n"
        f"⏰ **Timestamp Node:** `{latest_log.get('date_received')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💬 **Message Payload:**\n`{latest_log.get('message')}`"
    )
    await event.reply(text)


# ──────────────────────────────────────────────
# 11. OTP WAIT COMMAND
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/otp_wait\s+(\+?\d+)(?:\s+(\d+))?'))
async def otp_wait_handler(event) -> None:
    if not is_admin(event.sender_id):
        return
    phone_in = event.pattern_match.group(1)
    duration_str = event.pattern_match.group(2)
    duration = int(duration_str) if duration_str else 60
    clean_phone_with_plus = clean_phone_input(phone_in)
    db_clean_phone = normalize_phone(clean_phone_with_plus)

    status_msg = await event.reply(
        f"🛰️ **Polling Engine Initiated:** Watching for new incoming 777000 data strings "
        f"for `{clean_phone_with_plus}` (Timeout: `{duration}s`)..."
    )

    start_time = time.time()
    initial_otp = db.get_latest_otp(db_clean_phone)
    initial_ts = initial_otp.get("timestamp", 0) if initial_otp else 0

    while time.time() - start_time < duration:
        await asyncio.sleep(3)
        current_otp = db.get_latest_otp(db_clean_phone)
        if current_otp and current_otp.get("timestamp", 0) > initial_ts:
            text = (
                f"🚨 **NEW INCOMING TIMELINE OTP DETECTED!**\n"
                f"📱 **Phone:** `{clean_phone_with_plus}`\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💬 **Content:**\n`{current_otp.get('message')}`"
            )
            await status_msg.edit(text)
            return

    await status_msg.edit(f"⏰ **Timeout reached (`{duration}s`)!** No newer state notifications caught inside logs for `{clean_phone_with_plus}`.")


# ──────────────────────────────────────────────
# 12. LOGOUT COMMAND
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/logout'))
async def terminate_manual_login(event) -> None:
    args = event.text.split()
    if len(args) < 2:
        await event.reply("❌ **Syntax Error:** Missing parameters. Format: `/logout +91XXXXXXXXXX`")
        return

    phone = args[1].strip().replace(" ", "")
    status_msg = await event.reply(f"⚡ **Initiating termination pipeline context for `{phone}`...**")

    target_sessions = await db.get_active_target_sessions()
    matched_acc = next((acc for acc in target_sessions if str(acc.get("phone")) == phone), None)
    if not matched_acc:
        await status_msg.edit(f"⚠️ **Query Exception:** `{phone}` Target DB clusters me nahi mila.")
        return

    # Evict from pool first
    clean_phone = normalize_phone(phone)
    await GLOBAL.pool_remove(clean_phone)

    async with managed_client(matched_acc, use_pool=False) as client:
        try:
            await client.log_out()
        except Exception:
            pass

    db.remove_account_permanently(phone)
    await status_msg.edit(f"🗑️ **Revocation Complete:** Account session linked to `{phone}` has been closed, unauthorized, and wiped out of MongoDB records completely.")


# ──────────────────────────────────────────────
# 13. RELOAD ACCOUNTS
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/reload(?:_accounts|\s+accounts)?$'))
async def reload_accounts_router(event) -> None:
    status_msg = await event.reply("🔄 **Reloading Local Accounts...** `sessions/` aur `vars.txt` ko database schema ke sath sync kiya ja raha hai.")
    try:
        result = await db.reload_local_accounts(event=status_msg)
        report = (
            "✅ **Reload Accounts Complete**\n"
            f"📥 Staged into source DB: `{result.get('staged', 0)}`\n"
            f"🔐 Verified sessions updated: `{result.get('migrated', 0)}`\n"
            f"⚠️ Failed: `{result.get('failed', 0)}`\n"
            f"⏭️ Skipped: `{result.get('skipped', 0)}`\n"
        )
        errors = result.get("errors", [])
        if errors:
            report += "\n📋 **Issues:**\n"
            for idx, err in enumerate(errors[:10], 1):
                report += f"`{idx}.` `{err.get('phone', '?')}` ➜ {err.get('error', 'Unknown')}\n"
        await status_msg.edit(report)
    except Exception as ex:
        await status_msg.edit(f"❌ **Reload Accounts Failed:** `{str(ex)}`")


# ──────────────────────────────────────────────
# 14. REFRESH ACCOUNTS
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/refresh_accounts'))
async def accounts_refresh_router(event) -> None:
    status_msg = await event.reply("🔄 **Initiating Global Dual-DB Account Migration...** Verification sequences triggered.")
    try:
        success, failed, errors = await voice_engine.process_cross_migration()
        report = (
            "🎯 **Migration Operations Report Complete**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Successfully Verified & Migrated: `{success}` accounts.\n"
            f"❌ Expired/Banned Skips: `{failed}` accounts.\n\n"
        )
        if errors:
            report += "📋 **Detailed Error Log Trace Matrix:**\n"
            for idx, err in enumerate(errors, 1):
                report += f" `{idx}.` 📱 Phone: `{err.get('phone', '?')}` ➔ 🛑 {err.get('error', 'Unknown')}\n"
        await status_msg.edit(report)
    except Exception as ex:
        await status_msg.edit(f"❌ **Core Migration Matrix Failed:** `{str(ex)}`")


# ──────────────────────────────────────────────
# 15. CLEAN BANNED ACCOUNTS
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/clean_banned_accounts'))
async def clean_banned_accounts_router(event) -> None:
    if not is_admin(event.sender_id):
        return
    status_msg = await event.reply(
        "📡 **On-Demand Connectivity Check Triggered!**\n\n"
        "⚙️ Saare database accounts ki live connectivity aur validity check ki ja rahi hai... Isme thoda samay lag sakta hai, kripya pratiksha karein."
    )
    try:
        result = await voice_engine.clean_banned_accounts_handler()
        report = (
            "🔄 **Cleaned Accounts Complete**\n\n"
            "📊 **Final Storage Audit:**\n"
            f"• Total Processed: `{result.get('processed', 0)}`\n"
            f"• Success Active: `{result.get('active', 0)}`\n"
            f"• Defective/Banned: `{result.get('failed', 0)}`\n"
            f"• Missing Sessions: `{result.get('skipped', 0)}`"
        )
        errors = result.get("errors", [])
        if errors:
            report += "\n\n📋 **Issues Detected:**\n"
            for idx, err in enumerate(errors, 1):
                raw_phone = str(err.get('phone', '?')).strip()
                formatted_phone = f"+{raw_phone}" if not raw_phone.startswith("+") else raw_phone
                line = f"`{idx}.` `{formatted_phone}` ➜ {err.get('error', 'Unknown')}\n"
                if len(report) + len(line) > 3900:
                    await status_msg.edit(report)
                    status_msg = await event.respond("⏳ **Processing Next Batch of Issues...**")
                    report = "📋 **Issues Detected (Continued):**\n\n"
                report += line
        await status_msg.edit(report)
    except Exception as ex:
        logger.error(f"Clean banned error: {ex}", exc_info=True)
        await status_msg.edit(f"❌ **Cleanup Execution Failed:** `{str(ex)}`")


# ──────────────────────────────────────────────
# 16. REMOVE ACCOUNT
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/remove_account'))
async def account_purge_router(event) -> None:
    args = event.text.split()
    if len(args) < 2:
        await event.reply("❌ **Syntax Error:** Use: `/remove_account +91XXXXXXXXXX`")
        return
    phone = args[1].strip()
    clean_phone = normalize_phone(phone)
    await GLOBAL.pool_remove(clean_phone)
    if db.remove_account_permanently(phone):
        await event.reply(f"🗑️ **Data Record Dropped:** `{phone}` completely purged from system clusters.")
    else:
        await event.reply(f"⚠️ Record match inside system sets failed.")


# ──────────────────────────────────────────────
# 17. HEALTH SCAN COMMAND
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/health_scan'))
async def global_health_scan_router(event) -> None:
    if not is_admin(event.sender_id):
        return

    status_msg = await event.reply(
        "⚕️ **Global Health Scan & Auto-Recovery Initiated!**\n\n"
        "System is currently scanning all `failed` and `restricted` accounts. "
        "Agar unka temporary Telegram Spam Mute expire ho gaya hoga, toh unhe auto-recover karke wapas `ACTIVE` pool mein add kiya jayega. Please wait..."
    )

    all_accounts = await db.get_all_accounts_raw()
    failed_accounts = [acc for acc in all_accounts if acc.get("status") in (
        AccountStatus.FAILED, AccountStatus.BANNED, AccountStatus.RESTRICTED)]

    if not failed_accounts:
        await status_msg.edit("✅ **System Health Excellent:** Koi bhi account 'failed' ya 'muted' state mein nahi hai. Auto-recovery ki zaroorat nahi.")
        return

    recovered_count = 0
    permanently_dead_count = 0
    still_restricted_count = 0
    scan_semaphore = asyncio.Semaphore(5)

    async def scan_and_recover(acc):
        nonlocal recovered_count, permanently_dead_count, still_restricted_count
        async with scan_semaphore:
            phone = normalize_phone(str(acc.get("phone", "")))
            session_str = safe_session_str(acc)
            if not session_str:
                still_restricted_count += 1
                return

            try:
                async with managed_client(acc, use_pool=False) as client:
                    if not await client.is_user_authorized():
                        raise SessionRevokedError(request=None)
                    me = await client.get_me()
                    try:
                        await client.send_message("SpamBot", "/start")
                        db.update_session_status(phone, AccountStatus.ACTIVE, client.session.save())
                        recovered_count += 1
                    except (ChatWriteForbiddenError, Exception):
                        still_restricted_count += 1
                        db.mark_account_failed(phone, f"Still Restricted")
            except (UserDeactivatedError, UserDeactivatedBanError, SessionRevokedError, AuthKeyUnregisteredError):
                permanently_dead_count += 1
                db.mark_account_revoked(phone, "Permanently Banned / Revoked by Telegram.")
            except Exception:
                still_restricted_count += 1
                db.mark_account_failed(phone, "Unstable connectivity")
            await asyncio.sleep(0.5)

    tasks = [asyncio.create_task(scan_and_recover(acc)) for acc in failed_accounts]
    await asyncio.gather(*tasks)

    updated_all = await db.get_all_accounts_raw()
    total_active = sum(1 for x in updated_all if x.get("status") == AccountStatus.ACTIVE)

    report = (
        "🏥 **Health Scan & Recovery Complete!**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔍 Total 'Failed' Scanned: `{len(failed_accounts)}`\n\n"
        f"🟢 **Successfully Recovered:** `{recovered_count}` (Spam mute lifted!)\n"
        f"🟠 **Still Restricted/Muted:** `{still_restricted_count}` (Need more time)\n"
        f"🔴 **Permanently Dead:** `{permanently_dead_count}` (Marked as Revoked)\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 **New Active Pool Size:** `{total_active}` Accounts ready for use."
    )
    await status_msg.edit(report)


# ──────────────────────────────────────────────
# 18. TOGGLE HEALTH COMMANDS
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/turnof_health'))
async def turn_off_health_cmd(event) -> None:
    if not is_admin(event.sender_id):
        return
    await GLOBAL.set_health_check(False)
    logger.warning("🛑 Admin disabled health auditor.")
    await event.reply("🛑 **System Health Check / Auditor has been TURNED OFF.**\nBackground account validations, get_me() requests, and ban-checks are now completely paused.")


@bot.on(events.NewMessage(pattern=r'/turnon_health'))
async def turn_on_health_cmd(event) -> None:
    if not is_admin(event.sender_id):
        return
    await GLOBAL.set_health_check(True)
    logger.info("✅ Admin enabled health auditor.")
    await event.reply("✅ **System Health Check / Auditor has been TURNED ON.**\nBackground account validations have resumed.")


# ──────────────────────────────────────────────
# 19. GENERIC SCRAPE RUNNER
# ──────────────────────────────────────────────

async def generic_scrape_runner(event, mode: str, title_label: str) -> None:
    raw_text = event.text.strip()
    input_segments = raw_text.split(maxsplit=2)

    if len(input_segments) < 2:
        await event.reply(
            f"❌ **Syntax Error:** Proper target command input required!\n"
            f"👉 **Format:** `/{event.text.split()[0].lstrip('/')} <group_link>`"
        )
        return

    target_link = input_segments[1].strip().replace("<", "").replace(">", "").replace('"', '').replace("'", "")

    selected_worker = None
    if mode == 'specific_phone':
        if len(input_segments) < 3:
            await event.reply("❌ **Syntax Error:** Target phone number missing!\n👉 **Format:** `/scrape_group_all <group_link> <phone_number>`")
            return
        target_phone_input = input_segments[2].strip()
        target_phone = normalize_phone(clean_phone_input(target_phone_input))
        record = db.get_session_by_phone(target_phone)
        if not record or not safe_session_str(record) or record.get("status") != AccountStatus.ACTIVE:
            await event.reply(f"❌ **Operation Dropped:** Provided account `+{target_phone}` is either not in DB, missing session, or not Active.")
            return
        selected_worker = dict(record)
        selected_worker["phone"] = target_phone
        selected_worker["session"] = safe_session_str(record)
        selected_worker["session_string"] = selected_worker["session"]
    else:
        active_sessions = await db.get_active_target_sessions()
        if not active_sessions:
            await event.reply("❌ **Operation Dropped:** Verified processing modules are empty. Run `/reload_accounts` first.")
            return
        selected_worker = random.choice(active_sessions)
    #
    status_msg = await event.reply(
        f"📡 **Launching {title_label} Scan Engine...**\n"
        f"⚡ Connecting via targeted node endpoint `+{selected_worker['phone']}`..."
    )

    try:
        if mode == 'hidden':
            count = await scraper_engine.scrape_hidden_matrix(selected_worker, target_link)
        elif mode == 'voicechat':
            count = await scraper_engine.scrape_voicechat_matrix(selected_worker, target_link)
        else:
            scrape_mode = 'all' if mode == 'specific_phone' else mode
            count = await scraper_engine.scrape_standard_pool(selected_worker, target_link, scrape_mode)

        report = (
            f"🏆 **[{title_label}] Sequence Complete!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **Metrics Summary Output:**\n"
            f"• Scraper Account: `+{selected_worker['phone']}`\n"
            f"• Destination Registry: `scraped_data` repository\n"
            f"• Total Extracted Rows: `{count}` unique profiles saved\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✨ *Dataset is fully synced and ready for target multi-account campaigns.*"
        )
        await status_msg.edit(report)
    except Exception as e:
        logger.error(f"Scrape error: {e}")
        await status_msg.edit(f"❌ **Scraper Infrastructure Exception:** `{str(e)[:150]}`")


# ── Scrape command registrations ──

@bot.on(events.NewMessage(pattern=r'/scrape_from_group_id(\s+|$)'))
async def scrape_group_id_cmd(event):
    # Mode 'all' ke sath ID-based full scraping execute karega
    await generic_scrape_runner(event, 'all', 'ID-Based Aggregate Scrape')

@bot.on(events.NewMessage(pattern=r'/scrape_group_all(\s+|$)'))
async def scrape_group_all_cmd(event):
    await generic_scrape_runner(event, 'specific_phone', 'Targeted Single-Account Full Scrape')


@bot.on(events.NewMessage(pattern=r'/scrape_from_voicechat(\s+|$)'))
async def scrape_vc_cmd(event):
    await generic_scrape_runner(event, 'voicechat', 'Live VoiceChat Call Tracker')


@bot.on(events.NewMessage(pattern=r'/scrape_all(\s+|$)'))
async def scrape_all_cmd(event):
    await generic_scrape_runner(event, 'all', 'Global Aggregate Full Scrape')


@bot.on(events.NewMessage(pattern=r'/scrape_active_24h(\s+|$)'))
async def scrape_24h_cmd(event):
    await generic_scrape_runner(event, '24h', 'Aggressive 24h Active Scan')


@bot.on(events.NewMessage(pattern=r'/scrape_weekly(\s+|$)'))
async def scrape_weekly_cmd(event):
    await generic_scrape_runner(event, 'weekly', '7-Day Activity Interval Crawler')


@bot.on(events.NewMessage(pattern=r'/scrape_hidden(\s+|$)'))
async def scrape_hidden_cmd(event):
    await generic_scrape_runner(event, 'hidden', 'Deep Interaction Log Analyzer')


# ──────────────────────────────────────────────
# 20. DELETE SCRAPED FILES
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/delete_scraped_files'))
async def delete_scraped_files_cmd(event) -> None:
    try:
        total = await db.count_scraped_data()
        if total == 0:
            await event.reply("📂 **Database Notice:** Your cloud memory `scraped_members` collection layer is already completely empty.")
            return
        await db.clear_scraped_data()
        await event.reply(f"🗑️ **Cloud Database Purged Clean!**\n\nSuccessfully dropped and cleared `{total}` user rows from your live MongoDB database server.")
    except Exception as e:
        logger.error(f"Delete scraped error: {e}")
        await event.reply(f"❌ **Database Execution Fault:** Cannot drop active records lines: {e}")


# ──────────────────────────────────────────────
# 21. CONTACT SCRAPER
# ──────────────────────────────────────────────

def clean_db_name(name: str) -> str:
    match = re.search(r'db[\s-]*(\d+)', name, re.IGNORECASE)
    if match:
        return f"DB {match.group(1).zfill(3)}"
    return name.strip()


@bot.on(events.NewMessage(pattern=r'/contact_scraper(?:\s+(.+))?'))
async def direct_contact_csv_scraper(event) -> None:
    if not is_admin(event.sender_id):
        return
    raw_input = event.pattern_match.group(1)
    if not raw_input:
        await event.reply("❌ **Syntax Error:** Proper input parameters required.\n👉 **Format:** `/contact_scraper <phone_number>`")
        return

    phone = clean_phone_input(raw_input.strip())
    db_clean_phone = normalize_phone(phone)

    status_msg = await event.reply(f"📡 **Accessing account session `+{db_clean_phone}`...**")

    record = db.get_session_by_phone(db_clean_phone)
    if not record or not safe_session_str(record):
        await status_msg.edit(f"❌ **Operation Failed:** Account `+{db_clean_phone}` session DB mein nahi mila.")
        return

    try:
        async with managed_client(record, use_pool=True) as client:
            if not await client.is_user_authorized():
                await status_msg.edit(f"🔴 **Session Revoked:** Account `+{db_clean_phone}` access denied.")
                return

            contacts_result = await client(GetContactsRequest(hash=0))
            contacts_list = contacts_result.users

            if not contacts_list:
                await status_msg.edit(f"ℹ️ Account `+{db_clean_phone}` has no saved contacts.")
                return

            await status_msg.edit("📊 **Generating clean TXT structure...**")

            file_path = f"contacts_{db_clean_phone}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                for contact in contacts_list:
                    if contact.deleted:
                        continue
                    raw_name = f"{contact.first_name or ''} {contact.last_name or ''}".strip() or "No Name"
                    clean_name = clean_db_name(raw_name)
                    clean_name = clean_name.replace("\n", " ").replace("\r", " ").replace("'", "\\'")
                    phone_num = str(contact.phone).strip() if contact.phone else ""
                    f.write(f"{{ PhoneNumber: '{phone_num}', UserName: '{clean_name}' }}\n")

            await bot.send_file(
                event.chat_id,
                file_path,
                caption=f"📥 **Contacts Exported (TXT)!**\n\n• **Account:** `+{db_clean_phone}`\n• **Count Saved Extracted:** `{len(contacts_list)}`",
            )
            if os.path.exists(file_path):
                os.remove(file_path)
            await status_msg.delete()

    except Exception as err:
        logger.error(f"Contact scraper error: {err}", exc_info=True)
        await status_msg.edit(f"❌ **Error:** `{str(err)}`")


# ──────────────────────────────────────────────
# 22. MEMBER ADDER
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r"^/addmembers (.+)"))
async def run_member_adder_matrix(event) -> None:
    if not is_admin(event.sender_id):
        return

    if adder_engine.is_running:
        await event.reply("⚠️ Member Adding background engine processing pool is occupied right now.")
        return

    chat_id = event.chat_id
    target_group_link = event.pattern_match.group(1).strip().replace("<", "").replace(">", "").replace('"', '').replace("'", "")

    # Lock all active accounts safely before campaign
    active_accounts = await db.get_active_target_sessions()
    for acc in active_accounts:
        phone = acc.get("phone")
        if phone:
            db.acquire_lock(normalize_phone(str(phone)))

    logger.info(f"⚡ Launching enterprise adder to target: {target_group_link}")

    try:
        # 1. Initialize State Tracker
        adder_state = AdderState(total_target=0, max_workers=10)

        # 2. Send initial status message to get message_id
        status_msg_obj = await bot.send_message(chat_id, "🚀 Initializing Enterprise System...")

        # 3. Fire & Forget background updater task
        asyncio.create_task(
            status_updater_loop(bot, chat_id, status_msg_obj.id, adder_state)
        )

        async def dummy_callback(text):
            pass

        # 4. Start Worker Queue with state tracker
        result_text = await adder_engine.execute_adding_pipeline(
            target_group_link=target_group_link,
            update_callback=dummy_callback,
            adder_state=adder_state
        )

        # 5. Send final summary report
        await bot.send_message(chat_id, result_text)

    except Exception as e:
        logger.error(f"Adder error: {e}")
        await event.reply(f"❌ **Adder System Exception:** `{str(e)[:200]}`")
    finally:
        # Release account locks after completion/failure
        for acc in active_accounts:
            phone = acc.get("phone")
            if phone:
                db.release_lock(normalize_phone(str(phone)))
        logger.info("🔓 Adder locks released.")


# ──────────────────────────────────────────────
# 23. GLOBAL DM SENDER
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/send_dmsender_all'))
async def run_global_dmsender_matrix(event) -> None:
    if not is_admin(event.sender_id):
        return
    if dm_engine.is_running:
        await event.reply("⚠️ **Engine Occupied:** Campaign pehle se background me active hai.")
        return

    all_scraped_data = await asyncio.to_thread(list, db.scraped_members.find({}))
    if not all_scraped_data:
        await event.reply("❌ **Database Empty:** Scraped database me koi users nahi hain. Pehle `/scrape` commands run karein.")
        return

    extracted_targets = []
    for doc in all_scraped_data:
        extracted_targets.append({
            "user_id": doc.get("user_id"),
            "access_hash": doc.get("access_hash"),
            "username": doc.get("username"),
            "phone": doc.get("phone"),
        })

    dm_engine.wizard_state[event.sender_id] = {
        "step": "AWAITING_LIMIT",
        "targets": extracted_targets,
        "text": "",
        "media": None,
        "limit": 0,
    }

    await event.reply(
        f"🌍 **GLOBAL MASS DM INITIATED!**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ **Total `{len(extracted_targets)}` users successfully extracted from ALL groups combined!**\n\n"
        f"Kitne logo ko message bhejna chahte hain? (Number daalein ya `all` likhein):"
    )


# ──────────────────────────────────────────────
# 24. VOICE CHAT
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'/run_voicechat(?:\s+(.+))?'))
async def start_voice_engine_cmd(event) -> None:
    if not is_admin(event.sender_id):
        return
    raw_input = event.pattern_match.group(1)
    if not raw_input:
        await event.reply("❌ **Syntax Error:** Proper input parameters required.\n👉 **Format:** `/run_voicechat <group_link> [count]`")
        return

    input_segments = raw_input.strip().split()
    target = input_segments[0].replace("<", "").replace(">", "").replace('"', '').replace("'", "")

    active_pool = await db.get_active_target_sessions()
    total_available = len(active_pool)
    if total_available == 0:
        await event.reply("❌ **Operation Aborted:** Mapped source range limits are empty. No active sessions online.")
        return

    desired_count = total_available
    if len(input_segments) >= 2:
        try:
            parsed = int(input_segments[1].strip())
            if parsed > 0:
                desired_count = parsed
        except ValueError:
            desired_count = total_available

    desired_count = min(desired_count, total_available)

    await event.reply(
        f"⚡ **Spawning PyTgCalls WebRTC Cluster Matrix...**\n"
        f"🎯 Target Allocation: `{desired_count}` accounts (Total Available: `{total_available}`).\n"
        f"🛰️ Destination: `{target}`"
    )

    response = await voice_engine.launch_voice_cluster(target, audio_file="silent.mp3", desired_count=desired_count)
    await event.reply(response)


# ──────────────────────────────────────────────
# 25. STATUS COMMAND
# ──────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/status'))
async def system_diagnostics_snapshot(event) -> None:
    active_pool = len(await db.get_active_target_sessions())
    scraped_rows = await db.count_scraped_data()

    adder_state = "`🟢 RUNNING`" if adder_engine.is_running else "`🔴 RESTING`"
    dm_state = "`🟢 RUNNING`" if dm_engine.is_running else "`🔴 RESTING`"
    voice_state = "`🟢 ACTIVE`" if voice_engine.is_running else "`🔴 INACTIVE`"

    if adder_engine.is_running:
        adder_state += f" (Workers: {len(getattr(adder_engine, '_workers', []))})"
    if dm_engine.is_running:
        dm_state += f" (Workers: {len(getattr(dm_engine, '_campaign_manager', type('', (), {'_workers': []})())._workers)})"

    text = (
        "📊 **ENTERPRISE SYSTEM SNAPSHOT METRICS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✨ Verified Target Sessions Node: `{active_pool}` active\n"
        f"📂 Scraped Raw Records Pool: `{scraped_rows}` profiles\n\n"
        "**Core Engines Status:**\n"
        f"🚀 Member Adder Engine: {adder_state}\n"
        f"📨 Direct Message Engine: {dm_state}\n"
        f"🎙️ VoiceChat Stream Loop: {voice_state}\n\n"
        f"🛡️ Validated Proxies Pool: `{get_proxy_count()}` functional"
    )
    await event.reply(text)




# ──────────────────────────────────────────────
# 26. CONTINUOUS SESSION AUDITOR
# ──────────────────────────────────────────────

audit_logger = logging.getLogger("SessionAuditor")


async def continuous_session_auditor() -> None:
    """
    Enterprise-Grade Session Integrity Auditor v2.0
    Parallel batch processing + LRU prioritization.
    Handles 10,000+ accounts efficiently.
    """
    await asyncio.sleep(random.randint(30, 90))
    audit_logger.info("🚀 Enterprise Anti-Ban Session Auditor v2.0 (Parallel Batch Mode)")

    # ── 🔥 BATCH CONFIGURATION (tunable) ──
    BATCH_SIZE = CONFIG.get("AUDITOR_BATCH_SIZE", 10)       # Accounts checked in parallel
    BATCH_STAGGER = CONFIG.get("AUDITOR_BATCH_STAGGER", 15) # Seconds between batches
    MACRO_COOLDOWN_MIN = CONFIG.get("AUDITOR_COOLDOWN_MIN", 1800)  # 30 min
    MACRO_COOLDOWN_MAX = CONFIG.get("AUDITOR_COOLDOWN_MAX", 3600)  # 1 hour

    while True:
        try:
            # 🔥 AUTO-PAUSE: Automatically skip auditor cycles if heavy campaigns are running
            if not await GLOBAL.is_health_check_active() or adder_engine.is_running or dm_engine.is_running or voice_engine.is_running:
                await asyncio.sleep(30)
                continue

            active_accounts = await db.get_active_target_sessions()
            if not active_accounts:
                await asyncio.sleep(random.randint(600, 1200))
                continue

            # ── 🔥 DYNAMIC POOL ADJUSTMENT ──
            await GLOBAL.adjust_pool_size(len(active_accounts))

            # ── 🔥 PRIORITIZATION: Least recently checked first ──
            # If no last_checked_time, treat as oldest priority (epoch = 0)
            active_accounts.sort(
                key=lambda x: (
                    x.get("last_checked_time") or
                    x.get("last_updated") or
                    datetime(1970, 1, 1)
                )
            )

            random.shuffle(active_accounts)  # Slight randomness within priority tiers

            accounts_checked = 0
            accounts_failed = 0
            
            # ── 🔥 SEQUENTIAL PROCESSING TO PREVENT EVENT LOOP FREEZE ──
            for i in range(0, len(active_accounts), BATCH_SIZE):
                if not await GLOBAL.is_health_check_active():
                    break

                batch = active_accounts[i:i+BATCH_SIZE]
                results = []
                for acc in batch:
                    # 🔥 Give control back to the event loop so DM/Adder workers can run
                    await asyncio.sleep(3.0) 
                    res = await _audit_single_account(acc)
                    results.append(res)

                for r in results:
                    if isinstance(r, Exception):
                        accounts_failed += 1
                        audit_logger.debug(f"Batch task exception: {r}")
                    elif r is True:
                        accounts_checked += 1

                # Stagger between batches (much shorter than per-account)
                await asyncio.sleep(BATCH_STAGGER)
                # Trigger GC more often to free objects
                if random.random() < 0.5:
                    gc.collect()

            # ── Pool cleanup ──
            evicted = await GLOBAL.pool_cleanup_stale(max_idle=1800)
            if evicted:
                audit_logger.info(f"🧹 Cleaned {evicted} stale pooled connections.")

            stale_auth = await GLOBAL.cleanup_stale_auth_states()
            if stale_auth:
                audit_logger.info(f"🧹 Cleaned {stale_auth} stale auth states.")

            # Force GC periodically
            if random.random() < 0.1:
                gc.collect()
                audit_logger.debug("GC triggered.")

            audit_logger.info(
                f"🏁 Auditor batch complete: {accounts_checked} ok, {accounts_failed} errors. "
                f"Next macro cycle in ~{round(MACRO_COOLDOWN_MIN/60, 1)}-{round(MACRO_COOLDOWN_MAX/60, 1)} min."
            )
            await asyncio.sleep(random.uniform(MACRO_COOLDOWN_MIN, MACRO_COOLDOWN_MAX))

        except Exception as e:
            audit_logger.error(f"Auditor loop error: {e}", exc_info=True)
            await asyncio.sleep(60)


# ──────────────────────────────────────────────
# 26. AUDIT TASK & EXCEPTION HANDLING
# ──────────────────────────────────────────────

async def _audit_single_account(account_doc: dict) -> bool:
    """
    Check one account's session health.
    Returns True if healthy, raises/returns False if revoked.
    """
    phone = account_doc.get("phone")
    clean_phone = normalize_phone(str(phone)) if phone else ""

    if not clean_phone or not safe_session_str(account_doc):
        return False

    # Skip locked accounts (busy in voice/adder/dm)
    if db.is_locked(clean_phone):
        audit_logger.debug(f"🔒 Account +{clean_phone} locked. Evicting from pool.")
        await GLOBAL.pool_remove(clean_phone)
        return False

    reason_failed = None
    is_duplicate = False

    try:
        async with managed_client(account_doc, use_pool=False) as client:
            me = await asyncio.wait_for(client.get_me(), timeout=10.0)
            if not me:
                return True  # Not a failure, just empty response
            return True

    except AuthKeyDuplicatedError as e:
        reason_failed = f"⚠️ CRITICAL CONFLICT: Auth Key Duplication! ({e})"
        is_duplicate = True
    except (AuthKeyUnregisteredError, SessionRevokedError) as e:
        reason_failed = f"Session Revoked: {e}"
    except (UserDeactivatedError, UserDeactivatedBanError) as e:
        reason_failed = f"Account Terminated: {e}"
    except (asyncio.TimeoutError, OSError, ConnectionError, ssl.SSLError):
        audit_logger.debug(f"🌐 Transient network error for +{clean_phone}")
        return True  # Not permanently dead, skip
    except Exception as e:
        err_txt = str(e).lower()
        if any(m in err_txt for m in ["authkey", "sessionrevoked", "expired", "unauthorized",
                                       "revoked", "deactivated", "banned", "locked", "restricted"]):
            reason_failed = f"Structural handshake failure: {e}"
        else:
            audit_logger.debug(f"Transient operational error for +{clean_phone}: {e}")
            return True  # Transient, skip

    if reason_failed:
        audit_logger.critical(f"❌ Session +{clean_phone} is dead: {reason_failed}")
        db.mark_account_revoked(clean_phone, reason_failed)
        await GLOBAL.pool_remove(clean_phone)

        ist_time = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        now_str = ist_time.strftime("%d-%m-%Y | %H:%M:%S")
        icon = "⚠️" if is_duplicate else "❌"
        alert = (
            f"{icon} **Session Status login removed!**\n\n"
            f"• **Phone:** `+{clean_phone}`\n"
            f"• **Detected at:** `{now_str}`\n"
            f"• **Trigger Reason:** `{reason_failed}`\n\n"
            f"⚙️ *System Action: Account isolated from active worker rotation pools.*"
        )
        try:    
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(
                    "https://bluecoys.com/api/telegram-disconnected",
                    params={"phone_number": clean_phone})
                response.raise_for_status()
        except Exception as e:
            audit_logger.error(f"Failed to notify Bluecoys API: {e}")
            
        admin_id = CONFIG.get("ADMIN_ID")
        if admin_id:
            try:
                await bot.send_message(int(str(admin_id).strip()), alert)
            except Exception as send_err:
                audit_logger.error(f"Admin notification failed: {send_err}")
        return False

    return True


# ──────────────────────────────────────────────
# 27. FASTAPI SERVER & BACKGROUND TASKS LIFESPAN
# ──────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Start Telethon Bot directly on Uvicorn's active event loop
    logger.info("⚡ Starting Telethon Bot on active Uvicorn event loop...")
    await bot.start(bot_token=CONFIG["BOT_TOKEN"])
    
    # 2. Register background auditor task
    auditor_task = asyncio.create_task(continuous_session_auditor())
    GLOBAL.register_task(auditor_task)

    # 3. Register auto-recovery loop task
    recovery_task = asyncio.create_task(auto_health_recovery_loop())
    GLOBAL.register_task(recovery_task)
    
    logger.info("🌐 Service, Telegram Bot, Auditor, and Recovery Loops are online!")
    yield
    
    # Cleanup on server stop
    logger.info("🛑 Gracefully shutting down Telethon Bot & Background Tasks...")
    auditor_task.cancel()
    recovery_task.cancel()
    try:
        await asyncio.gather(auditor_task, recovery_task)
    except asyncio.CancelledError:
        pass
    await bot.disconnect()

app = FastAPI(title="Enterprise Telegram Suite API", lifespan=lifespan)
app.include_router(console_router, prefix="/console")

@app.get("/")
async def root_health_check():
    return {"status": "online", "service": "Telegram Bot Suite", "console": "/console"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# ──────────────────────────────────────────────
# 28. AUTO-RECOVERY LOOP
# ──────────────────────────────────────────────

async def auto_health_recovery_loop() -> None:
    """Auto-recovery engine: checks and recovers muted accounts every 12 hours."""
    await asyncio.sleep(3600)  # 1 hour initial delay
    audit_logger.info("🏥 Auto-Recovery Background Engine Started.")

    while True:
        if not await GLOBAL.is_health_check_active() or adder_engine.is_running or dm_engine.is_running or voice_engine.is_running:
            await asyncio.sleep(60)
            continue

        try:
            all_accounts = await db.get_all_accounts_raw()
            failed_accounts = [acc for acc in all_accounts if acc.get("status") in (
                AccountStatus.FAILED, AccountStatus.BANNED, AccountStatus.RESTRICTED)]

            if failed_accounts:
                recovered = 0
                for acc in failed_accounts:
                    phone = normalize_phone(str(acc.get("phone", "")))
                    session_str = safe_session_str(acc)
                    if not session_str:
                        continue
                    try:
                        async with managed_client(acc, use_pool=False) as client:
                            if await client.is_user_authorized():
                                await client.get_me()
                                await client.send_message("SpamBot", "/start")
                                db.update_session_status(phone, AccountStatus.ACTIVE, client.session.save())
                                recovered += 1
                    except Exception as e:
                        audit_logger.error(f"Auto-recovery failed for {phone}: {e}")
            
        except Exception as e:
            audit_logger.error(f"Recovery loop error: {e}")
        
        await asyncio.sleep(43200) # Check every 12 hours

# ──────────────────────────────────────────────
# 29. SERVER LAUNCHER (MUST BE AT THE VERY END)
# ──────────────────────────────────────────────

if __name__ == "__main__":
    # Dynamic Port Binding
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"🌐 Binding Web Service to host 0.0.0.0 on port {port}...")
    
    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=port,
        log_level="info",
        loop="asyncio"
    )
    server = uvicorn.Server(config)
    asyncio.run(server.serve())
