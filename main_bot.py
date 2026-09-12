#!/usr/bin/env python3

import os, sys, asyncio, logging, random, time, pathlib, ssl, re, gc, socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any, Callable, Set
from collections import OrderedDict
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
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import gc
import httpx

from config import CONFIG, DEVICE_PROFILES
from database import SuiteDatabase
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
    register_auditor_hooks,
)
from exception_classifier import (
    ErrorCategory,
    classify_exception,
    classify_connection_error,
)

from collections import OrderedDict


# ── 🔥 TTL-BOUNDED CACHE for session authorization checks ──
class TTLCache(OrderedDict):
    """Simple TTL-bounded OrderedDict replacing unbounded dict."""
    def __init__(self, maxsize: int = 512, ttl: float = 300.0):
        super().__init__()
        self.maxsize = maxsize
        self.ttl = ttl
        self._timestamps: Dict[str, float] = {}

    def __contains__(self, key):
        if key not in self._timestamps:
            return False
        if time.time() - self._timestamps[key] > self.ttl:
            self._timestamps.pop(key, None)
            super().pop(key, None)
            return False
        return super().__contains__(key)

    def __getitem__(self, key):
        if key not in self._timestamps:
            raise KeyError(key)
        if time.time() - self._timestamps[key] > self.ttl:
            self._timestamps.pop(key, None)
            super().pop(key, None)
            raise KeyError(key)
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        if len(self) >= self.maxsize:
            oldest = next(iter(self))
            super().pop(oldest, None)
            self._timestamps.pop(oldest, None)
        super().__setitem__(key, value)
        self._timestamps[key] = time.time()
from scraper import MemberScraper
from videochat import CloudVoiceChatEngine
from adder import EnterpriseMemberAdder, AdderState, status_updater_loop
from dmsender import setup_dmsender_handlers
from web_console import console_router, init_console_db, setup_console_routes, init_console_session_manager, shutdown_background_tasks

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
    AUTH_KEY_DUPLICATED = "auth_key_duplicated"
    PROXY_ERROR = "proxy_error"
    NETWORK_ERROR = "network_error"
    QUARANTINED = "quarantined"


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
        self._auth_lock = asyncio.Lock()
        self._nav_lock = asyncio.Lock()

        # Navigation state
        self.current_page: int = 1
        self.explorer_filter: str = "active"
        self.search_query: Optional[str] = None

        # Auth states (bounded)
        self.auth_states: Dict[str, AuthState] = {}

        # ── 🔥 STATUS BAR CACHE ──
        self._status_bar_cache: str = ""
        self._status_bar_expires: float = 0.0
        self._status_bar_ttl: int = 30

        # Health check flag
        self.health_check_active: bool = True

        # Live monitor task (/live real-time console)
        self._live_task: Optional[asyncio.Task] = None

        # ── 🔥 BACKGROUND ACTIVITY STATE (auditor / recovery) ──
        # Live status published by the background loops and rendered in the
        # Telegram console status bar.
        self.auditor_state: Dict[str, Any] = {
            "phase": "starting", "pass_no": 0, "pool": 0, "due": 0,
            "batch": 0, "batches": 0, "ok": 0, "dead": 0, "busy": 0,
            "no_proxy": 0, "errors": 0, "skipped": 0,
            "current": {}, "next_batch_at": None, "next_pass_at": None,
            "updated_at": 0.0,
        }
        self.recovery_state: Dict[str, Any] = {
            "phase": "waiting", "next_sweep_in": None,
            "last_recovered": None, "updated_at": 0.0,
        }
        # Rolling feed of recent per-account results, shown live in the console.
        self.live_feed: "OrderedDict[float, str]" = OrderedDict()

        # Background task registry
        self.background_tasks: Set[asyncio.Task] = set()

        self._initialized = True

    # ── 🔥 NEW: Cached status bar ──

    # ── YEH DO METHODS GlobalState class ke ANDAR DAALO ──
    async def get_status_bar(self, all_sessions: list, live: bool = False) -> str:
        """Return cached status bar, recompute only if expired.

        live=True always recomputes fresh (used by the /live real-time monitor).
        """
        now = time.time()
        if not live and now < self._status_bar_expires and self._status_bar_cache:
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

        # ── 🔥 LIVE SYSTEM ACTIVITY (auditor / proxy pool / recovery) ──
        now_ts = time.time()
        try:
            free_proxies = proxy_lease_manager.get_available_count()
            total_proxies = len(proxy_lease_manager.proxy_nodes)
            resting_proxies = len(proxy_lease_manager.proxy_cooldown)
            leased_proxies = int(proxy_lease_manager.stats.get("current_active_leases", 0))
            cooling_until = [n.cooldown_until for n in proxy_lease_manager.proxy_nodes.values()
                             if n.is_in_cooldown()]
            next_free_eta = (min(cooling_until) - now_ts) if cooling_until else None
        except Exception:
            free_proxies = resting_proxies = leased_proxies = total_proxies = 0
            next_free_eta = None

        a = self.auditor_state
        a_phase = a.get("phase", "starting")
        if a_phase == "scanning":
            auditor_line = (
                f"🔍 Pass #{a.get('pass_no', 0)} • batch {a.get('batch', 0)}/{a.get('batches', 0)} "
                f"({a.get('due', 0)} due) • ✅ {a.get('ok', 0)} 🪦 {a.get('dead', 0)} "
                f"⏭ {a.get('busy', 0) + a.get('skipped', 0) + a.get('no_proxy', 0)} ⚠️ {a.get('errors', 0)}"
            )
        elif a_phase == "resting":
            npa = a.get("next_pass_at")
            auditor_line = (
                f"💤 Resting • next pass in ~{_fmt_eta(npa - now_ts)}"
                if npa else "💤 Resting"
            )
        elif a_phase == "waiting_proxy":
            auditor_line = "⏳ Waiting for free proxy (pool resting after last use)"
        elif a_phase == "paused":
            auditor_line = "⏸ Paused • operation running"
        else:
            auditor_line = "🚀 Starting..."

        activity_lines = [f"🧠 Auditor: {auditor_line}"]
        if a_phase == "scanning":
            current_phones = sorted((a.get("current") or {}).keys())
            if current_phones:
                shown = ", ".join(f"+{p}" for p in current_phones[:3])
                more = len(current_phones) - 3
                activity_lines.append(
                    f"   ↳ Testing now: {shown}" + (f" (+{more} more)" if more > 0 else ""))
            else:
                activity_lines.append("   ↳ Connecting next account...")
            nba = a.get("next_batch_at")
            if nba and nba > now_ts:
                activity_lines.append(f"   ↳ Next batch in ~{_fmt_eta(nba - now_ts)}")

        r = self.recovery_state
        if r.get("phase") == "sweeping":
            recovery_line = "🩹 Sweeping failed accounts (human-paced)..."
        elif r.get("next_sweep_in") is not None:
            recovery_line = f"💤 Idle • next sweep in ~{_fmt_eta(r.get('next_sweep_in'))}"
        else:
            recovery_line = "💤 Idle"
        if r.get("last_recovered") is not None:
            recovery_line += f" • last: 🟢 {r.get('last_recovered')} recovered"

        proxy_line = (
            f"🛡️ Proxy Pool: `{free_proxies}` free • `{resting_proxies}` resting • "
            f"`{leased_proxies}` leased (of `{total_proxies}`)"
        )
        if free_proxies == 0 and resting_proxies > 0 and next_free_eta is not None:
            proxy_line += f" • next free in ~{_fmt_eta(next_free_eta)}"
        try:
            reserve = proxy_lease_manager.login_reserve_limit()
        except Exception:
            reserve = 0
        if reserve > 0:
            proxy_line += f" • 🔐 {reserve} reserved for login"

        feed_lines = []
        for ts, line in list(self.live_feed.items())[-6:]:
            clock = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
            feed_lines.append(f"`{clock}` {line}")

        self._status_bar_cache = (
            "**Workspace Overview**\n"
            f"Total Inventory: `{total}` Accounts\n"
            f"🟢 `{active_cnt}` Active (Good Health)\n"
            f"🟡 `{pending_cnt}` Pending / 2FA\n"
            f"🟠 `{failed_cnt}` Failed / Spam Muted (Recoverable)\n"
            f"🔴 `{revoked_cnt}` Revoked / Dead\n"
            f"Infrastructure: ⚡ Node `{worker_id}` • 🛡️ `{proxy_count}` Proxies Healthy\n"
            "\n"
            "**⚙️ System Activity**\n"
            + "\n".join(activity_lines) + "\n"
            + proxy_line + "\n"
            + f"🏥 Recovery: {recovery_line}\n"
            + ("\n**📜 Live Feed**\n" + "\n".join(feed_lines) + "\n" if feed_lines else "")
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
            self.auth_states[phone_key] = state

    async def pop_auth_state(self, phone_key: str) -> Optional[AuthState]:
        async with self._auth_lock:
            return self.auth_states.pop(phone_key, None)

    async def cleanup_stale_auth_states(self) -> int:
        async with self._auth_lock:
            stale = [k for k, v in self.auth_states.items() if v.is_expired()]
            for k in stale:
                self.auth_states.pop(k, None)
        for key in stale:
            try:
                await session_manager.release_login(key, f"login:{key}")
            except Exception:
                pass
        return len(stale)

    # ── Health Check ──
    async def is_health_check_active(self) -> bool:
        async with self._lock:
            return self.health_check_active

    async def set_health_check(self, active: bool) -> None:
        async with self._lock:
            self.health_check_active = active

    # ── Background Activity ──
    def update_auditor_state(self, **kwargs) -> None:
        """Publish live auditor progress for the Telegram console status bar."""
        self.auditor_state.update(kwargs)
        self.auditor_state["updated_at"] = time.time()

    def mark_audit_account_started(self, phone: str) -> None:
        """Register an account as currently under test (shown live in console)."""
        self.auditor_state.setdefault("current", {})[str(phone)] = time.time()

    def mark_audit_account_finished(self, phone: str) -> None:
        self.auditor_state.get("current", {}).pop(str(phone), None)

    def push_live_event(self, line: str) -> None:
        """Record a per-account result for the console live feed (keep last 6)."""
        self.live_feed[time.time()] = line
        while len(self.live_feed) > 6:
            self.live_feed.popitem(last=False)

    # ── Live Monitor (/live) ──
    def set_live_task(self, task: Optional[asyncio.Task]) -> None:
        self._live_task = task

    def get_live_task(self) -> Optional[asyncio.Task]:
        return self._live_task

    def update_recovery_state(self, **kwargs) -> None:
        """Publish live recovery-loop progress for the Telegram console status bar."""
        self.recovery_state.update(kwargs)
        self.recovery_state["updated_at"] = time.time()

    # ── Background Tasks ──
    def register_task(self, task: asyncio.Task) -> None:
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)


def _fmt_eta(seconds: Any) -> str:
    """Human-friendly countdown: 1h 23m / 4m 30s / 45s."""
    try:
        s = max(0, int(seconds))
    except (TypeError, ValueError):
        return "?"
    if s >= 3600:
        return f"{s // 3600}h {(s % 3600) // 60}m"
    if s >= 60:
        return f"{s // 60}m {s % 60}s"
    return f"{s}s"


# Initialize global state
GLOBAL = GlobalState()
GLOBAL.initialize()


# ──────────────────────────────────────────────
# DATABASE & SERVICE INSTANCES
# ──────────────────────────────────────────────
db = SuiteDatabase()
proxy_manager = ProxyManager()
proxy_lease_manager = ProxyLeaseManager(proxy_manager)

# 🔥 Centralized managers — the SINGLE source of truth for sessions & accounts
session_manager = SessionManager(
    db=db,
    proxy_manager=proxy_manager,
    proxy_lease_manager=proxy_lease_manager,
    max_active_clients=CONFIG.get("MAX_POOL_ABSOLUTE", 200),
)
account_lease_manager = AccountLeaseManager(db=db)

scraper_engine = MemberScraper(db, proxy_manager, proxy_lease_manager, session_manager, account_lease_manager)
voice_engine = CloudVoiceChatEngine(db, proxy_manager, proxy_lease_manager, session_manager, account_lease_manager)
adder_engine = EnterpriseMemberAdder(db, proxy_manager, proxy_lease_manager, session_manager, account_lease_manager)

# ──────────────────────────────────────────────
# TELETHON BOT PROXY (Solves Event Loop Mismatch in Uvicorn)
# ──────────────────────────────────────────────
class BotProxy:
    def __init__(self):
        self._bot = None
        self._pending_handlers = []

    def on(self, event_builder):
        def decorator(func):
            if self._bot is not None:
                self._bot.add_event_handler(func, event_builder)
            else:
                self._pending_handlers.append((func, event_builder))
            return func
        return decorator

    def add_event_handler(self, callback, event=None):
        if self._bot is not None:
            self._bot.add_event_handler(callback, event)
        else:
            self._pending_handlers.append((callback, event))

    def initialize(self, *args, **kwargs):
        self._bot = TelegramClient(*args, **kwargs)
        for callback, event_builder in self._pending_handlers:
            self._bot.add_event_handler(callback, event_builder)
        self._pending_handlers.clear()
        return self._bot

    def __getattr__(self, name):
        if self._bot is None:
            raise RuntimeError(f"Bot not initialized yet. Cannot access '{name}'. Call initialize() first.")
        return getattr(self._bot, name)

bot = BotProxy()
dm_engine = setup_dmsender_handlers(bot, db, proxy_manager, proxy_lease_manager,
                                    session_manager=session_manager,
                                    account_lease_manager=account_lease_manager)

# ──────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────

from telethon.errors import QueryIdInvalidError

async def safe_answer(event, text=None, alert=False):
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



async def build_premium_status_bar(all_sessions: list, live: bool = False) -> str:
    """Cache-enabled SaaS-style operational summary. Async wrapper for GLOBAL cache."""
    return await GLOBAL.get_status_bar(all_sessions, live=live)


# ──────────────────────────────────────────────
# 1b. LIVE MONITOR (real-time console, /live)
# ──────────────────────────────────────────────

LIVE_REFRESH_SECONDS = 5.0


async def _live_monitor_loop(message) -> None:
    """Continuously re-edit the monitor message with fresh state."""
    try:
        while True:
            try:
                all_sessions = await db.get_all_suite_sessions()
                bar = await build_premium_status_bar(all_sessions, live=True)
                now_str = datetime.now().strftime("%H:%M:%S")
                text = (
                    "🔴 **LIVE MONITOR** — auto-refresh every 5s\n\n"
                    f"{bar}"
                    f"\n_Updated: {now_str}_"
                )
                await message.edit(
                    text,
                    buttons=[[Button.inline("⏹ Stop Live", data="live_stop")]],
                    parse_mode="md",
                )
            except FloodWaitError as e:
                await asyncio.sleep(min(float(e.seconds) + 1.0, 60.0))
                continue
            except Exception as e:
                logger.error(f"Live monitor refresh failed: {e}")
            await asyncio.sleep(LIVE_REFRESH_SECONDS)
    except asyncio.CancelledError:
        try:
            await message.edit("🔴 **LIVE MONITOR** stopped.", buttons=None)
        except Exception:
            pass
        raise


@bot.on(events.NewMessage(pattern='/live$'))
async def live_monitor_start(event) -> None:
    if not is_admin(event.sender_id):
        return
    old = GLOBAL.get_live_task()
    if old and not old.done():
        old.cancel()
    msg = await event.reply("🔴 **LIVE MONITOR** starting...")
    task = asyncio.create_task(_live_monitor_loop(msg))
    GLOBAL.register_task(task)
    GLOBAL.set_live_task(task)


@bot.on(events.NewMessage(pattern='/liveoff$'))
async def live_monitor_stop_cmd(event) -> None:
    if not is_admin(event.sender_id):
        return
    task = GLOBAL.get_live_task()
    if task and not task.done():
        task.cancel()
        await event.reply("⏹ Live monitor stopped.")
    else:
        await event.reply("No live monitor is running.")


@bot.on(events.CallbackQuery(data="live_stop"))
async def live_monitor_stop_button(event) -> None:
    if not is_admin(event.sender_id):
        return
    task = GLOBAL.get_live_task()
    if task and not task.done():
        task.cancel()
    await safe_answer(event, "Live monitor stopped.")


@asynccontextmanager
async def managed_client(record: dict):
    phone = normalize_phone(str(record.get("phone", "")))

    async with session_manager.acquire(
        phone,
        module="managed_client",
        worker_id="managed_client",
        auto_release=True,
    ) as lease:
        if not lease:
            raise ConnectionError(f"No eligible session for {phone}")
        yield lease.client


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
# SHARED LOGIN PROCESS (LEASE-OWNED PROXY ROTATION)
# ──────────────────────────────────────────────

def _login_proxy_label(client: TelegramClient) -> str:
    """Human label for the proxy lease currently used by a login client."""
    try:
        conn = getattr(client, "_connection", None)
        proxy = getattr(conn, "_proxy", None)
        if not proxy:
            return "Lease-managed (resolved)"
        host = proxy.get("addr") or proxy.get("host") or getattr(proxy, "addr", None)
        port = proxy.get("port") or getattr(proxy, "port", None)
        return f"{host}:{port}" if host else "Lease-managed (resolved)"
    except Exception:
        return "Lease-managed (resolved)"


async def shared_login_process(phone: str, login_owner: str) -> dict:
    clean_phone = normalize_phone(phone)
    existing = db.get_session_by_phone(clean_phone)

    device = get_device_profile(existing) if existing and existing.get("device_model") else (
        random.choice(DEVICE_PROFILES) if DEVICE_PROFILES else {}
    )

    max_attempts = 4
    last_error = None

    for attempt in range(1, max_attempts + 1):
        # build_login_client acquires a ProxyLeaseManager lease when proxy=None
        # and registers the client under the login reservation.
        client = await session_manager.build_login_client(
            clean_phone,
            login_owner,
            session_str=StringSession().save(),
            api_id=CONFIG["API_ID"],
            api_hash=CONFIG["API_HASH"],
            device=device,
            proxy=None,
        )

        if client is None:
            last_error = RuntimeError("No proxy lease available / login client build failed")
            await session_manager.release_login(clean_phone, login_owner)
            if not await session_manager.reserve_login(clean_phone, login_owner):
                raise Exception("Login reservation lost — phone is owned by another module. Try again later.")
            continue

        try:
            async def _proxy_req():
                if not client.is_connected():
                    await asyncio.wait_for(client.connect(), timeout=20.0)
                return await asyncio.wait_for(client.send_code_request(phone), timeout=20.0)

            send_code_result = await _proxy_req()
            code_hash = send_code_result.phone_code_hash

            db.save_pending_session(
                clean_phone,
                client.session.save(),
                AccountStatus.PENDING,
                code_hash,
                device,
            )

            return {
                "status": "code_sent",
                "phone": phone,
                "db_clean_phone": clean_phone,
                "code_hash": code_hash,
                "device": device,
                "client": client,
                "proxy_used": _login_proxy_label(client),
            }
        except Exception as e:
            last_error = e
            # Release the failed lease so the next attempt can pull a fresh proxy.
            await session_manager.release_login(clean_phone, login_owner)
            if attempt < max_attempts:
                if not await session_manager.reserve_login(clean_phone, login_owner):
                    raise Exception("Login reservation lost during proxy rotation — phone is owned by another module. Try again later.")
            logger.warning(f"Login proxy attempt {attempt}/{max_attempts} failed. Rotating to next proxy lease...")
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
        health_active = await GLOBAL.is_health_check_active()

        text = (
            "🛡️ **INFRASTRUCTURE DIAGNOSTICS & SYSTEM MONITORING**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{status_bar}\n"
            "🖥️ **RUNTIME INFRASTRUCTURE LOGS:**\n"
            f"• Core Worker Node: `{worker_id}`\n"
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
        now = datetime.now(timezone.utc)
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

        last_check_raw = record.get("last_checked_time") or record.get("last_updated") or datetime.now(timezone.utc)
        time_diff = datetime.now(timezone.utc) - last_check_raw if isinstance(last_check_raw, datetime) else timedelta(0)
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
        await event.edit("⚕️ **Global Health Scan & Auto-Recovery Initiated!**\n\nScanning `failed` accounts (human-paced, 3-6 min per account)...", buttons=None)

        all_accounts = await db.get_all_accounts_raw()
        # FAILED accounts only — banned, restricted and revoked are never touched.
        failed_accounts = [acc for acc in all_accounts if acc.get("status") == AccountStatus.FAILED]

        if not failed_accounts:
            await event.edit(
                "✅ **System Health Excellent:** Koi bhi account 'failed' ya 'muted' state mein nahi hai.",
                buttons=[[Button.inline("⬅️ Back", data="nav_lvl1_accounts")]],
            )
            return

        recovered_count = 0
        still_restricted = 0
        no_proxy_count = 0
        busy_count = 0
        scan_sem = asyncio.Semaphore(max(1, int(CONFIG.get("HEALTH_SCAN_CONCURRENCY", 3))))
        scan_delay = CONFIG.get("RECOVERY_ACCOUNT_DELAY", (180, 360))

        async def _ui_scan_worker(acc):
            nonlocal recovered_count, still_restricted, no_proxy_count, busy_count
            phone = normalize_phone(str(acc.get("phone", "")))
            # FIRST-COME-FIRST-USE LOCK: never touch an account owned by
            # another task (campaign, login, auditor).
            if await account_lease_manager.is_busy(phone) or await session_manager.is_owned(phone):
                busy_count += 1
                logger.debug(f"[UIHealthScan] +{phone} busy on another task — skipped.")
                return
            async with scan_sem:
                # Human pacing between account scans (3-6 min + micro-jitter).
                await asyncio.sleep(random.uniform(float(scan_delay[0]), float(scan_delay[1])))
                if not safe_session_str(acc):
                    still_restricted += 1
                    return
                try:
                    async with managed_client(acc) as client:
                        # Connect explicitly; no free proxy = skip, never mark.
                        if not client.is_connected():
                            try:
                                await asyncio.wait_for(client.connect(), timeout=15.0)
                            except Exception as conn_err:
                                no_proxy_count += 1
                                logger.debug(f"[UIHealthScan] +{phone} connect skipped: {conn_err}")
                                return
                        if await client.is_user_authorized():
                            await client.get_me()
                            await client.send_message("SpamBot", "/start")
                            await db.update_session_status_async(phone, AccountStatus.ACTIVE, client.session.save())
                            recovered_count += 1
                            return
                        still_restricted += 1
                except SessionAlreadyOwnedError:
                    # Safety net: owned at acquire time — never mark it.
                    busy_count += 1
                    logger.debug(f"[UIHealthScan] +{phone} owned at acquire time — skipped.")
                except ConnectionError as e:
                    # No proxy leaseable / connect refused: skip, never mark failed.
                    no_proxy_count += 1
                    logger.debug(f"[UIHealthScan] +{phone} skipped (no proxy/connect): {e}")
                except Exception:
                    still_restricted += 1

        await asyncio.gather(*[asyncio.create_task(_ui_scan_worker(a)) for a in failed_accounts])

        report = (
            "🏥 **Health Scan & Recovery Complete!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔍 Scanned: `{len(failed_accounts)}` accounts\n"
            f"🟢 **Successfully Recovered:** `{recovered_count}`\n"
            f"🟠 **Still Restricted:** `{still_restricted}`\n"
            f"🛡️ **Skipped (No Proxy Available):** `{no_proxy_count}`\n"
            f"🔒 **Skipped (Busy on Another Task):** `{busy_count}`\n"
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
        await event.edit(
            f"**Runtime Status**\n\nActive Workers: `4`\nTask Queue: `Idle`\nCached Connections: `0` (pool removed)",
            buttons=back_to_lvl1,
        )

    elif route == "diag_pause_auditor":
        await GLOBAL.set_health_check(False)
        stop_auditor()  # actually cancel the background auditor + recovery tasks
        logger.warning("🛑 Auditor paused via UI.")
        await event.edit("⏸️ **Auditor Health Checks PAUSED.**", buttons=back_to_lvl1)
        await event.answer()

    elif route == "diag_resume_auditor":
        await GLOBAL.set_health_check(True)
        start_auditor()  # actually restart the background auditor + recovery tasks
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
    login_owner = f"login:{db_clean_phone}"

    await event.reply(f"⏳ **Initializing Login Pipeline for:** `{phone}`...\nConnecting to Telegram Core Matrix...")
    logger.info(f"⚙️ Login request for: {phone}")

    # Reserve the phone for login so no other module can acquire it concurrently
    reserved = await session_manager.reserve_login(db_clean_phone, login_owner)
    if not reserved:
        await event.reply("❌ **Login Blocked:** Phone is already owned by another module (login/auditor/DM/adder/scraper/videochat/web). Try again later.")
        return

    try:
        # shared_login_process routes through build_login_client(), so the
        # client is already registered on the reservation by SessionManager.
        login_result = await shared_login_process(phone, login_owner)
        client = login_result["client"]
        device = login_result["device"]
        code_hash = login_result["code_hash"]
        proxy_used = login_result.get("proxy_used", "Lease-managed (resolved)")

        # Move the reservation to OTP_WAITING (same client reused across OTP/2FA)
        await session_manager.set_login_stage(
            db_clean_phone, login_owner, SessionLifecycleState.OTP_WAITING
        )

        # Store auth state with TTL (NON-OWNING reference; SessionManager owns
        # the client, its proxy lease and its counts.)
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
        await session_manager.release_login(db_clean_phone, login_owner)
        logger.error(f"Timeout for {phone}")
        await event.reply("❌ **Network Connection Timeout:** Telegram core server ne response nahi diya. Please check your system internet or proxies.")
    except FloodWaitError as fwe:
        await session_manager.release_login(db_clean_phone, login_owner)
        logger.error(f"FloodWait {fwe.seconds}s for {phone}")
        await event.reply(f"❌ **FloodWait:** Telegram ne `{fwe.seconds}` seconds ka wait karne ko kaha hai.")
    except Exception as e:
        await session_manager.release_login(db_clean_phone, login_owner)
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
    login_owner = f"login:{db_clean_phone}"

    await event.reply(f"⚡ **Submitting Verification Token `{code}`** for `{clean_phone_with_plus}`...")

    # Get auth state
    state = await GLOBAL.get_auth_state(db_clean_phone)
    client = state.client if state else None
    phone_code_hash = state.phone_code_hash if state else None
    device = state.device if state else None

    # If no live login client in memory, fall back to the DB pending session.
    # This is still a login-flow client, so it must go through reserve_login.
    if not client or not phone_code_hash:
        # Fallback: try DB
        if not await session_manager.reserve_login(db_clean_phone, login_owner):
            await event.reply("❌ **Error:** Phone is owned by another module. Run `/login` fresh.")
            return
        record = db.get_session_by_phone(db_clean_phone)
        if not record or not safe_session_str(record):
            await session_manager.release_login(db_clean_phone, login_owner)
            await event.reply("❌ **Error:** No active login state found for this phone. Run `/login` first.")
            return
        device = get_device_profile(record)
        client = await session_manager.build_login_client(
            db_clean_phone,
            login_owner,
            session_str=safe_session_str(record),
            api_id=CONFIG["API_ID"],
            api_hash=CONFIG["API_HASH"],
            device=device,
            proxy=record.get("proxy"),
        )
        if client is None:
            await session_manager.release_login(db_clean_phone, login_owner)
            await event.reply("❌ **Error:** Phone is no longer reserved for login. Run `/login` fresh.")
            return
        await asyncio.wait_for(client.connect(), timeout=20.0)
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
        await session_manager.release_login(db_clean_phone, login_owner)
        await event.reply(f"✅ **Login Successful!**\nSession for `{clean_phone_with_plus}` is now live and saved in DB 1 ecosystem.")

    except SessionPasswordNeededError:
        session_str = client.session.save()
        db.save_authorized_session(db_clean_phone, session_str, AccountStatus.ACTIVE, device, two_fa_password=None)
        # Stay reserved in TWOFA_WAITING — same client is reused by /verify_2fa
        await session_manager.set_login_stage(db_clean_phone, login_owner, SessionLifecycleState.TWOFA_WAITING)
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
        await session_manager.release_login(db_clean_phone, login_owner)


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
    login_owner = f"login:{db_clean_phone}"

    await event.reply(f"🔒 **Submitting 2FA security matrix password** for `{clean_phone_with_plus}`...")

    state = await GLOBAL.get_auth_state(db_clean_phone)
    client = state.client if state else None
    device = state.device if state else None

    if not client:
        # No live login client — must reserve before creating one.
        if not await session_manager.reserve_login(db_clean_phone, login_owner):
            await event.reply("❌ **Error:** Phone is owned by another module. Run `/login` fresh.")
            return
        record = db.get_session_by_phone(db_clean_phone)
        if not record:
            await session_manager.release_login(db_clean_phone, login_owner)
            await event.reply("❌ **Error:** No session data located for this index.")
            return
        device = get_device_profile(record)
        client = await session_manager.build_login_client(
            db_clean_phone,
            login_owner,
            session_str=safe_session_str(record),
            api_id=CONFIG["API_ID"],
            api_hash=CONFIG["API_HASH"],
            device=device,
            proxy=record.get("proxy"),
        )
        if client is None:
            await session_manager.release_login(db_clean_phone, login_owner)
            await event.reply("❌ **Error:** Phone is no longer reserved for login. Run `/login` fresh.")
            return
        await asyncio.wait_for(client.connect(), timeout=20.0)

    try:
        await client.sign_in(password=password)
        final_session_str = client.session.save()

        # 2FA password is intentionally NOT persisted — no plaintext secret
        # written to logs or the database.
        db.save_authorized_session(
            db_clean_phone,
            final_session_str,
            AccountStatus.ACTIVE,      # enum – database.py handles .value
            device,
            two_fa_password=None
        )

        # OTP setup
        ensure_otp_listener(client, db_clean_phone)
        await fetch_past_otps(client, db_clean_phone)

        await GLOBAL.pop_auth_state(db_clean_phone)
        await session_manager.release_login(db_clean_phone, login_owner)
        await event.reply(f"🎉 **2FA Bypass Complete!**\n`{clean_phone_with_plus}` status elevated to `active` inside DB 1.")

    except Exception as e:
        await event.reply(f"❌ **2FA Submission Rejected:** `{str(e)}`")
        # Clean up the login reservation, in-memory login client and proxy lease
        # through the public SessionManager API.
        await GLOBAL.pop_auth_state(db_clean_phone)
        await session_manager.release_login(db_clean_phone, login_owner)


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

    clean_phone = normalize_phone(phone)

    async with managed_client(matched_acc) as client:
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
    )

    all_accounts = await db.get_all_accounts_raw()
    # Scan FAILED accounts only — banned, restricted and revoked are never touched.
    failed_accounts = [acc for acc in all_accounts if acc.get("status") == AccountStatus.FAILED]

    if not failed_accounts:
        await status_msg.edit("✅ **System Health Excellent:** Koi bhi account 'failed' ya 'muted' state mein nahi hai. Auto-recovery ki zaroorat nahi.")
        return

    recovered_count = 0
    permanently_dead_count = 0
    still_restricted_count = 0
    no_proxy_count = 0
    busy_count = 0
    scan_semaphore = asyncio.Semaphore(max(1, int(CONFIG.get("HEALTH_SCAN_CONCURRENCY", 3))))
    scan_delay = CONFIG.get("RECOVERY_ACCOUNT_DELAY", (180, 360))

    async def scan_and_recover(acc):
        nonlocal recovered_count, permanently_dead_count, still_restricted_count, no_proxy_count, busy_count
        phone = normalize_phone(str(acc.get("phone", "")))
        # FIRST-COME-FIRST-USE LOCK: an account busy on any other task
        # (campaign, login, auditor) is never touched by the health scan.
        if await account_lease_manager.is_busy(phone) or await session_manager.is_owned(phone):
            busy_count += 1
            logger.debug(f"[HealthScan] +{phone} busy on another task — skipped.")
            return
        async with scan_semaphore:
            # Human pacing between account scans (3-6 min + micro-jitter).
            await asyncio.sleep(random.uniform(float(scan_delay[0]), float(scan_delay[1])))
            session_str = safe_session_str(acc)
            if not session_str:
                still_restricted_count += 1
                return

            try:
                async with managed_client(acc) as client:
                    if not await client.is_user_authorized():
                        raise SessionRevokedError(request=None)
                    me = await client.get_me()
                    try:
                        await client.send_message("SpamBot", "/start")
                        await db.update_session_status_async(phone, AccountStatus.ACTIVE, client.session.save())
                        recovered_count += 1
                    except (ChatWriteForbiddenError, Exception):
                        still_restricted_count += 1
                        await db.mark_account_failed_async(phone, f"Still Restricted")
            except SessionAlreadyOwnedError:
                # Safety net: account became owned between the pre-check and
                # the acquire. Never mark it — another task owns it.
                busy_count += 1
                logger.debug(f"[HealthScan] +{phone} owned at acquire time — skipped.")
            except (UserDeactivatedError, UserDeactivatedBanError, SessionRevokedError, AuthKeyUnregisteredError):
                permanently_dead_count += 1
                await db.mark_account_revoked_async(phone, "Permanently Banned / Revoked by Telegram.")
            except ConnectionError as e:
                # No proxy leaseable / connect refused: skip, never mark failed.
                no_proxy_count += 1
                logger.debug(f"[HealthScan] +{phone} skipped (no proxy/connect): {e}")
            except Exception:
                still_restricted_count += 1
                await db.mark_account_failed_async(phone, "Unstable connectivity")

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
        f"🛡️ **Skipped (No Proxy Available):** `{no_proxy_count}`\n"
        f"🔒 **Skipped (Busy on Another Task):** `{busy_count}`\n"
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
    stop_auditor()  # actually cancel the background auditor + recovery tasks
    logger.warning("🛑 Admin disabled health auditor.")
    await event.reply("🛑 **System Health Check / Auditor has been TURNED OFF.**\nBackground account validations, get_me() requests, and checks are now completely paused.")


@bot.on(events.NewMessage(pattern=r'/turnon_health'))
async def turn_on_health_cmd(event) -> None:
    if not is_admin(event.sender_id):
        return
    await GLOBAL.set_health_check(True)
    start_auditor()  # actually restart the background auditor + recovery tasks
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
        async with managed_client(record) as client:
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

@bot.on(events.NewMessage(pattern=r"^/addmembers\s+(\S+)(?:\s+(\d+))?"))
async def run_member_adder_matrix(event) -> None:
    if not is_admin(event.sender_id):
        return

    if adder_engine.is_running:
        await event.reply("⚠️ Member Adding background engine processing pool is occupied right now.")
        return

    chat_id = event.chat_id
    target_group_link = event.pattern_match.group(1).strip().replace("<", "").replace(">", "").replace('"', '').replace("'", "")
    requested_workers = None
    try:
        requested_workers = int(event.pattern_match.group(2)) if event.pattern_match.group(2) else None
    except (ValueError, TypeError):
        requested_workers = None

    # Session ownership is handled by SessionManager/AccountLeaseManager
    logger.info(
        f"⚡ Launching enterprise adder to target: {target_group_link} "
        f"(requested_workers={requested_workers})"
    )

    try:
        # 1. Initialize State Tracker
        adder_state = AdderState(total_target=0, max_workers=requested_workers or 10)

        # 2. Send initial status message to get message_id
        status_msg_obj = await bot.send_message(chat_id, "🚀 Initializing Enterprise System...")

        # 3. Background updater task (tracked so it is observable and cancelled
        #    with the rest of the suite at shutdown)
        updater_task = asyncio.create_task(
            status_updater_loop(bot, chat_id, status_msg_obj.id, adder_state)
        )
        GLOBAL.register_task(updater_task)

        async def dummy_callback(text):
            pass

        # 4. Start Worker Queue with state tracker
        result_text = await adder_engine.execute_adding_pipeline(
            target_group_link=target_group_link,
            update_callback=dummy_callback,
            adder_state=adder_state,
            requested_workers=requested_workers,
        )

        # 5. Send final summary report
        await bot.send_message(chat_id, result_text)

    except Exception as e:
        logger.error(f"Adder error: {e}")
        await event.reply(f"❌ **Adder System Exception:** `{str(e)[:200]}`")


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


def should_start_auditor() -> bool:
    """Startup gate for the background auditor. Honors AUDITOR_ENABLED."""
    return bool(CONFIG.get("AUDITOR_ENABLED", True))


def should_start_recovery() -> bool:
    """Startup gate for the auto-recovery loop. Honors ENABLE_AUTO_RECOVERY."""
    return bool(CONFIG.get("ENABLE_AUTO_RECOVERY", True))


# ── 🔥 AUDITOR & RECOVERY CONTROLLER: full stop/resume around operations ──
_auditor_task: Optional[asyncio.Task] = None
_recovery_task: Optional[asyncio.Task] = None


def stop_auditor() -> None:
    """Completely stop the background auditor AND auto-recovery (cancel tasks).
    Called when dmsender / adder / videochat operations start, or when the
    admin explicitly pauses health checks, so the operation owns the proxy
    pool with zero background churn."""
    global _auditor_task, _recovery_task
    if _auditor_task is not None and not _auditor_task.done():
        _auditor_task.cancel()
        logger.info("🛑 Auditor STOPPED — background health checks cancelled.")
    _auditor_task = None
    if _recovery_task is not None and not _recovery_task.done():
        _recovery_task.cancel()
        logger.info("🛑 Recovery STOPPED — background recovery cancelled.")
    _recovery_task = None
    GLOBAL.update_auditor_state(phase="paused")


def start_auditor() -> None:
    """Resume the background auditor and auto-recovery if enabled, not running,
    and no operation currently owns the proxy pool."""
    global _auditor_task, _recovery_task
    # Never resume while an active operation still owns the proxy pool. The
    # operation's own finally block will call start_auditor again when it ends.
    if adder_engine.is_running or dm_engine.is_running or voice_engine.is_running:
        logger.info("⏸ Auditor resume deferred — an operation still owns the proxy pool.")
        return
    if should_start_auditor():
        if _auditor_task is None or _auditor_task.done():
            _auditor_task = asyncio.create_task(
                _run_with_bounded_restarts("auditor", continuous_session_auditor)
            )
            GLOBAL.register_task(_auditor_task)
            logger.info("✅ Auditor RESUMED — background health checks restarted.")
    if should_start_recovery():
        if _recovery_task is None or _recovery_task.done():
            _recovery_task = asyncio.create_task(
                _run_with_bounded_restarts("recovery", auto_health_recovery_loop)
            )
            GLOBAL.register_task(_recovery_task)
            logger.info("✅ Recovery RESUMED — background recovery restarted.")


def _normalize_check_time(account_doc: dict) -> float:
    """Epoch used for LRU ordering. Missing/invalid values sort to the front."""
    val = account_doc.get("last_checked_time") or account_doc.get("last_updated")
    if val is None:
        return 0.0
    if isinstance(val, datetime):
        # Mongo returns naive UTC datetimes; treat naive values as UTC.
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val.timestamp()
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except ValueError:
            pass
    return 0.0


def _auditor_network_capacity() -> int:
    try:
        # Reserve-aware: the auditor must not consume the login buffer either.
        return int(proxy_lease_manager.usable_available_count())
    except Exception:
        try:
            return int(proxy_lease_manager.get_available_count())
        except Exception:
            return 0


def _auditor_pool_size() -> list:
    """Total tracked proxy nodes (for diagnostics in skip logs)."""
    try:
        return list(proxy_lease_manager.proxy_nodes.keys())
    except Exception:
        return []


def _auditor_effective_capacity(
    eligible_accounts: int,
    available_network_capacity: int,
    configured_audit_concurrency: int,
) -> int:
    if eligible_accounts <= 0:
        return 0
    cap = min(
        eligible_accounts,
        max(0, available_network_capacity),
        max(1, configured_audit_concurrency),
    )
    return max(0, cap)


async def _auditor_run_pass(accounts: list, capacity: int) -> dict:
    outcomes = {"ok": 0, "dead": 0, "failed": 0, "busy": 0, "skipped": 0, "no_proxy": 0}
    if not accounts:
        return outcomes
    if capacity <= 0:
        # No network capacity: no audit clients are created; every account is
        # reported as skipped.
        outcomes["skipped"] = len(accounts)
        return outcomes

    sem = asyncio.Semaphore(max(1, capacity))

    async def _worker(acc):
        async with sem:
            phone = normalize_phone(str(acc.get("phone", "")))
            GLOBAL.mark_audit_account_started(phone)
            started = time.monotonic()
            try:
                # Small micro-jitter only: the real human pacing (3-5 min) comes
                # from the proxy cooldown window enforced by the lease manager.
                await asyncio.sleep(random.uniform(2.0, 6.0))
                try:
                    ok = await _audit_single_account(acc)
                    dur = time.monotonic() - started
                    line = (f"✅ +{phone} connected & authorized • {dur:.1f}s" if ok
                            else f"🪦 +{phone} DEAD • {dur:.1f}s")
                except _AuditNoProxy as e:
                    ok = "skip"
                    line = f"⏭ +{phone} skipped (proxy: {str(e)[:80]}) • {time.monotonic() - started:.1f}s"
                except SessionAlreadyOwnedError:
                    ok = "busy"
                    line = f"🔒 +{phone} busy (owned by another worker)"
                except Exception as e:
                    ok = "error"
                    line = f"⚠️ +{phone} error: {str(e)[:80]} • {time.monotonic() - started:.1f}s"
                audit_logger.info(line)
                GLOBAL.push_live_event(line)
                if ok is True:
                    return ("ok",)
                if ok is False:
                    return ("dead",)
                if ok == "busy":
                    return ("busy",)
                if ok == "error":
                    return ("failed",)
                return ("no_proxy",)
            finally:
                GLOBAL.mark_audit_account_finished(phone)

    for (outcome,) in await asyncio.gather(*(_worker(a) for a in accounts)):
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
    return outcomes


async def continuous_session_auditor() -> None:
    await asyncio.sleep(random.randint(30, 90))

    # ── 🔥 BATCH CONFIGURATION (tunable) ──
    BATCH_SIZE = CONFIG.get("AUDITOR_BATCH_SIZE", 10)       # Accounts processed per batch
    BATCH_STAGGER = CONFIG.get("AUDITOR_BATCH_STAGGER", 60) # Seconds between batches
    PASS_GAP_MIN = CONFIG.get("AUDITOR_COOLDOWN_MIN", 300)  # Rest between full passes
    PASS_GAP_MAX = CONFIG.get("AUDITOR_COOLDOWN_MAX", 600)
    RECHECK_SECONDS = max(60, int(CONFIG.get("AUDITOR_RECHECK_MINUTES", 720)) * 60)
    CONCURRENCY = max(1, int(CONFIG.get("AUDITOR_CONCURRENCY", 3)))
    pass_no = 0

    audit_logger.info(
        "🚀 Session Auditor online (batch mode) — checking ACTIVE + FAILED accounts, "
        f"recheck interval={RECHECK_SECONDS // 3600}h, human-paced via the proxy cooldown window."
    )

    while True:
        try:
            # 🔥 AUTO-PAUSE: Automatically skip auditor cycles if heavy campaigns are running
            if not await GLOBAL.is_health_check_active() or adder_engine.is_running or dm_engine.is_running or voice_engine.is_running:
                GLOBAL.update_auditor_state(phase="paused")
                await asyncio.sleep(30)
                continue

            all_accounts = await db.get_all_accounts_raw()
            # Auditor pool: ACTIVE accounts (session still alive?) plus FAILED
            # accounts. BANNED, RESTRICTED and REVOKED accounts are never
            # touched by the scanner.
            audit_pool = [
                acc for acc in all_accounts
                if str(acc.get("status", "")).lower() in (
                    AccountStatus.ACTIVE, AccountStatus.FAILED,
                ) and safe_session_str(acc)
            ]
            if not audit_pool:
                await asyncio.sleep(random.randint(300, 600))
                continue
            audit_pool.sort(key=_normalize_check_time)

            capacity = _auditor_effective_capacity(
                len(audit_pool),
                _auditor_network_capacity(),
                CONCURRENCY,
            )
            if capacity <= 0:
                audit_logger.warning(
                    f"Auditor pass skipped: 0 of {len(_auditor_pool_size())} proxies free "
                    "(resting in 3-5 min cooldown after last use)."
                )
                GLOBAL.update_auditor_state(phase="waiting_proxy")
                await asyncio.sleep(random.randint(60, 120))
                continue

            now = time.time()
            due = [acc for acc in audit_pool
                   if (now - _normalize_check_time(acc)) >= RECHECK_SECONDS]
            if not due:
                audit_logger.info(
                    f"Auditor: all {len(audit_pool)} accounts checked within the last "
                    f"{RECHECK_SECONDS // 3600}h; nothing due. Sleeping."
                )
                GLOBAL.update_auditor_state(
                    phase="resting", due=0, batch=0, batches=0,
                    next_batch_at=None, next_pass_at=time.time() + 300)
                await asyncio.sleep(random.randint(300, 600))
                continue

            pass_no += 1
            total_batches = (len(due) + BATCH_SIZE - 1) // BATCH_SIZE
            audit_logger.info(
                f"🔍 Audit pass starting: {len(due)} due of {len(audit_pool)} "
                f"(active+failed pool), concurrency={capacity}, recheck={RECHECK_SECONDS // 3600}h."
            )
            GLOBAL.update_auditor_state(
                phase="scanning", pass_no=pass_no, pool=len(audit_pool), due=len(due),
                batch=0, batches=total_batches, current={},
                ok=0, dead=0, busy=0, no_proxy=0, errors=0, skipped=0,
                next_batch_at=None,
            )

            counts = {"ok": 0, "dead": 0, "failed": 0, "busy": 0, "skipped": 0, "no_proxy": 0}

            # ── 🔥 BOUNDED CONCURRENCY WITHIN BATCHES ──
            for i in range(0, len(due), BATCH_SIZE):
                if not await GLOBAL.is_health_check_active():
                    break
                batch = due[i:i + BATCH_SIZE]
                batch_counts = await _auditor_run_pass(batch, capacity)
                for k, v in batch_counts.items():
                    counts[k] = counts.get(k, 0) + v
                GLOBAL.update_auditor_state(
                    batch=i // BATCH_SIZE + 1,
                    ok=counts["ok"], dead=counts["dead"], busy=counts["busy"],
                    no_proxy=counts["no_proxy"], errors=counts["failed"],
                    skipped=counts["skipped"],
                    next_batch_at=time.time() + BATCH_STAGGER,
                )
                # Stagger between batches
                await asyncio.sleep(BATCH_STAGGER)
                # Trigger GC more often to free objects
                if random.random() < 0.5:
                    gc.collect()

            stale_auth = await GLOBAL.cleanup_stale_auth_states()
            if stale_auth:
                audit_logger.info(f"🧹 Cleaned {stale_auth} stale auth states.")

            audit_logger.info(
                f"🏁 Audit pass done: {counts.get('ok', 0)} ok, {counts.get('dead', 0)} dead, "
                f"{counts.get('busy', 0)} busy, {counts.get('no_proxy', 0)} no-proxy, "
                f"{counts.get('failed', 0)} errors, {counts.get('skipped', 0)} skipped. "
                f"Next pass in {PASS_GAP_MIN // 60}-{PASS_GAP_MAX // 60} min."
            )
            gap = random.uniform(PASS_GAP_MIN, PASS_GAP_MAX)
            GLOBAL.update_auditor_state(
                phase="resting", next_pass_at=time.time() + gap,
                next_batch_at=None, current={})
            await asyncio.sleep(gap)

        except Exception as e:
            audit_logger.error(f"Auditor loop error: {e}", exc_info=True)
            await asyncio.sleep(60)


# ──────────────────────────────────────────────
# 26. AUDIT TASK & EXCEPTION HANDLING
# ──────────────────────────────────────────────

# ── 🔥 SESSION AUTHORIZATION CACHE (lightweight optimization) ──
_last_auth_check: TTLCache = TTLCache(maxsize=512, ttl=3600)
AUTH_CHECK_CACHE_SECONDS = 3600  # Skip duplicate checks within 1 hour


async def check_session_authorization(client, phone_display: str = "") -> tuple:
    """
    Lightweight session health check using is_user_authorized().
    
    Returns:
        (True, "authorized")     - Account is healthy/authorized
        (False, "unauthorized")  - Session not authorized
        (False, "revoked")       - Auth key unregistered/revoked
        (False, "timeout")       - Authorization check timed out
        (False, "connection_error") - Network/connection failure
        (False, "unknown")       - Other unexpected error
    """
    try:
        if not client.is_connected():
            return False, "disconnected"

        authorized = await asyncio.wait_for(
            client.is_user_authorized(),
            timeout=10.0
        )

        if authorized:
            if phone_display:
                audit_logger.info(f"[SessionCheck] +{phone_display} authorized")
            return True, "authorized"

        if phone_display:
            audit_logger.info(f"[SessionCheck] +{phone_display} unauthorized")
        return False, "unauthorized"

    except (AuthKeyUnregisteredError, SessionRevokedError):
        if phone_display:
            audit_logger.info(f"[SessionCheck] +{phone_display} revoked")
        return False, "revoked"

    except asyncio.TimeoutError:
        if phone_display:
            audit_logger.info(f"[SessionCheck] +{phone_display} timeout")
        return False, "timeout"

    except (ConnectionError, OSError, ssl.SSLError):
        if phone_display:
            audit_logger.info(f"[SessionCheck] +{phone_display} connection_error")
        return False, "connection_error"

    except Exception as e:
        audit_logger.warning(
            f"[SessionCheck] +{phone_display} Authorization check failed: {e}"
        )
        return False, "unknown"


# Raised when no proxy could be leased for an audit attempt. Treated as a
# skip (never as an account failure): the proxy pool is just cooling down.
class _AuditNoProxy(Exception):
    pass


async def _promote_authorized_failed(account_doc: dict, client, clean_phone: str) -> None:
    """A FAILED account whose session verifies as authorized is promoted back
    to ACTIVE (with a fresh session string) so the console reflects reality."""
    if str(account_doc.get("status", "")).lower() != AccountStatus.FAILED:
        return
    try:
        await db.update_session_status_async(
            clean_phone, AccountStatus.ACTIVE, client.session.save())
        audit_logger.info(f"🟢 +{clean_phone} authorized → promoted FAILED → ACTIVE")
        GLOBAL.push_live_event(f"🟢 +{clean_phone} FAILED → ACTIVE (verified)")
    except Exception as e:
        audit_logger.error(f"Promotion failed for +{clean_phone}: {e}")


async def _audit_single_account(account_doc: dict) -> bool:
    phone = account_doc.get("phone")
    clean_phone = normalize_phone(str(phone)) if phone else ""

    if not clean_phone or not safe_session_str(account_doc):
        return False

    # Skip accounts that are currently leased/busy (voice/adder/dm).
    # Ownership is tracked by AccountLeaseManager/SessionManager, not DB locks.
    if await account_lease_manager.is_busy(clean_phone):
        audit_logger.debug(f"🔒 Account +{clean_phone} busy (leased). Skipping audit.")
        return False

    # Skip accounts in a login/OTP/2FA reservation or actively leased via
    # SessionManager — the auditor must never overlap an owned session.
    if await session_manager.is_owned(clean_phone):
        audit_logger.debug(f"🔒 Account +{clean_phone} owned (SessionManager). Skipping audit.")
        return False

    # ── 🔥 LIGHT CACHING: Skip if recently checked successfully ──
    now = time.time()
    last_check = _last_auth_check.get(clean_phone, 0.0)
    if (now - last_check) < AUTH_CHECK_CACHE_SECONDS:
        audit_logger.debug(f"[SessionCheck] +{clean_phone} skipped (cached)")
        return True

    reason_failed = None
    is_duplicate = False

    try:
        async with managed_client(account_doc) as client:
            # Ensure connection first
            if not client.is_connected():
                try:
                    await asyncio.wait_for(client.connect(), timeout=15.0)
                except Exception as conn_err:
                    # Most common cause: no proxy leaseable right now (pool
                    # resting). Skip without marking anything.
                    audit_logger.debug(f"[SessionCheck] +{clean_phone} connect failed: {conn_err}")
                    raise _AuditNoProxy(f"connect failed: {conn_err}")

            # Step 1: Lightweight authorization check
            authorized, reason = await check_session_authorization(client, clean_phone)

            if authorized:
                # Account is healthy/authorized - cache timestamp
                _last_auth_check[clean_phone] = time.time()
                await _promote_authorized_failed(account_doc, client, clean_phone)
                await db.mark_account_checked_async(clean_phone)
                return True

            # Step 2: Handle specific failure reasons
            if reason == "revoked":
                reason_failed = "Session revoked/unregistered"
            elif reason == "unauthorized":
                reason_failed = "Session unauthorized"
            elif reason == "timeout":
                reason_failed = "Authorization check timeout"
            elif reason == "connection_error":
                reason_failed = "Connection error"
            else:
                # IMPORTANT: For ambiguous/unknown failures, perform ONE deep fallback verification
                try:
                    me = await asyncio.wait_for(client.get_me(), timeout=10.0)
                    if me:
                        # Deep check passed - account is actually healthy
                        _last_auth_check[clean_phone] = time.time()
                        await _promote_authorized_failed(account_doc, client, clean_phone)
                        await db.mark_account_checked_async(clean_phone)
                        return True
                    else:
                        reason_failed = "Deep identity verification failed"
                except (AuthKeyUnregisteredError, SessionRevokedError):
                    reason_failed = "Session revoked/unregistered"
                except Exception as e:
                    reason_failed = f"Deep verification failed: {str(e)[:120]}"

    except AuthKeyDuplicatedError as e:
        audit_logger.critical(
            f"🔥 AuthKeyDuplicatedError for +{clean_phone}. "
            f"Session key collision — quarantining account."
        )
        reason_failed = "AuthKeyDuplicatedError: Session key already in use"
        is_duplicate = True
        # Quarantine via SessionManager and DB
        await session_manager.mark_quarantined(
            clean_phone,
            reason="AuthKeyDuplicatedError",
            category=ErrorCategory.AUTH_KEY_DUPLICATED,
        )
        db.set_account_state(clean_phone, AccountStatus.AUTH_KEY_DUPLICATED)

    except SessionAlreadyOwnedError:
        # The account became owned (worker or login reservation) between the
        # pre-checks and the acquire. SessionAlreadyOwnedError is a BUSY/skip
        # condition — it must NEVER escalate into an account failure.
        audit_logger.debug(f"🔒 Account +{clean_phone} owned at acquire time. Skipping audit (busy).")
        return True

    except (UserDeactivatedError, UserDeactivatedBanError) as e:
        reason_failed = f"Account Terminated: {e}"
    except _AuditNoProxy:
        raise  # No proxy available: skip, never mark the account.
    except ConnectionError as e:
        # managed_client raises "No eligible session" when no proxy could be
        # leased (pool resting) — skip without advancing the check timestamp.
        if "No eligible session" in str(e):
            raise _AuditNoProxy(str(e))
        audit_logger.debug(f"🌐 Transient network error for +{clean_phone}")
        await db.mark_account_checked_async(clean_phone)
        return True
    except (asyncio.TimeoutError, OSError, ssl.SSLError):
        audit_logger.debug(f"🌐 Transient network error for +{clean_phone}")
        await db.mark_account_checked_async(clean_phone)
        return True  # Not permanently dead, skip
    except Exception as e:
        err_txt = str(e).lower()
        if any(m in err_txt for m in ["authkey", "sessionrevoked", "expired", "unauthorized",
                                       "revoked", "deactivated", "banned", "locked", "restricted"]):
            reason_failed = f"Structural handshake failure: {e}"
        else:
            audit_logger.debug(f"Transient operational error for +{clean_phone}: {e}")
            await db.mark_account_checked_async(clean_phone)
            return True  # Transient, skip

    if reason_failed:
        audit_logger.critical(f"❌ Session +{clean_phone} is dead: {reason_failed}")
        if not is_duplicate:
            db.mark_account_revoked(clean_phone, reason_failed)
        await db.mark_account_checked_async(clean_phone)

        ist_time = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        now_str = ist_time.strftime("%d-%m-%Y | %H:%M:%S")
        icon = "⚠️" if is_duplicate else "❌"
        alert = (
            f"{icon} **Session Status login removed!\n\n"
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

_shutdown_done = False


async def _cancel_tracked_background_tasks() -> None:
    """Cancel GLOBAL.background_tasks (status updaters, web updaters) and await."""
    tasks = [t for t in list(GLOBAL.background_tasks) if not t.done()]
    for t in tasks:
        t.cancel()
    if tasks:
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass


async def _run_with_bounded_restarts(
    name: str,
    coro_factory,
    *,
    max_restarts: int = 3,
    base_delay: float = 5.0,
) -> None:
    attempts = 0
    while True:
        try:
            await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempts += 1
            audit_logger.error(
                f"{name} background task exited with error: {exc}",
                exc_info=True,
            )
            if attempts > max_restarts:
                audit_logger.critical(
                    f"{name} background task hit the restart limit "
                    f"({max_restarts}); stopping."
                )
                return
            delay = min(base_delay * (2 ** (attempts - 1)), 300.0)
            audit_logger.warning(
                f"{name} background task restarting in {delay}s "
                f"(attempt {attempts}/{max_restarts})."
            )
            await asyncio.sleep(delay)


async def lifespan(app: FastAPI):
    global _shutdown_done
    _shutdown_done = False

    # 1. Initialize and start Telethon Bot on Uvicorn's active event loop
    logger.info("Starting Telethon Bot on active Uvicorn event loop...")
    real_bot = bot.initialize(StringSession(), CONFIG["API_ID"], CONFIG["API_HASH"])
    await real_bot.start(bot_token=CONFIG["BOT_TOKEN"])

    # 2. Start Proxy Lease Manager and Account Lease Manager
    logger.info("Starting ProxyLeaseManager (Auto-Reaper active)...")
    await proxy_lease_manager.start()
    proxy_manager.start_background_testing()

    logger.info("Starting AccountLeaseManager (Lease Expiration Reaper active)...")
    await account_lease_manager.start()

    # 3. Register background auditor task via the controller (gated by
    #    AUDITOR_ENABLED). Engines fully stop/resume the auditor around
    #    operations through the registered hooks.
    register_auditor_hooks(stop_auditor, start_auditor)
    auditor_task = None
    if should_start_auditor():
        start_auditor()
        auditor_task = _auditor_task
        logger.info("AUDITOR: enabled - background audit task started.")
    else:
        logger.info("AUDITOR: disabled (AUDITOR_ENABLED=false) - no audit task, no auditor workers/session acquisition.")

    # 4. Register auto-recovery loop task (gated by ENABLE_AUTO_RECOVERY)
    global _recovery_task
    if should_start_recovery():
        _recovery_task = asyncio.create_task(
            _run_with_bounded_restarts("recovery", auto_health_recovery_loop)
        )
        GLOBAL.register_task(_recovery_task)
        logger.info("AUTO_RECOVERY: enabled - background recovery loop started.")
    else:
        _recovery_task = None
        logger.info("AUTO_RECOVERY: disabled (ENABLE_AUTO_RECOVERY=false) - no recovery loop started.")

    logger.info("Service, Telegram Bot, Auditor, Recovery Loops, and ProxyLeaseManager are online!")
    yield

    # Cleanup on server stop - ordered and idempotent (shutdown twice is safe)
    if _shutdown_done:
        logger.info("Shutdown already completed - skipping duplicate shutdown.")
        return
    _shutdown_done = True
    logger.info("Gracefully shutting down Telethon Bot, Background Tasks, and ProxyLeaseManager...")

    # 1) STOP NEW WORK - gracefully halt DM/adder/videochat workers
    try:
        if adder_engine.is_running:
            adder_engine.halt_engine()
    except Exception as exc:
        logger.error(f"Failed to stop adder engine during shutdown: {exc}")
    try:
        if dm_engine.is_running:
            dm_engine.halt_campaign()
    except Exception as exc:
        logger.error(f"Failed to stop DM engine during shutdown: {exc}")
    try:
        if voice_engine.is_running:
            await voice_engine.terminate_voice_cluster()
    except Exception as exc:
        logger.error(f"Failed to stop voice engine during shutdown: {exc}")

    # 2) STOP AUDITOR + RECOVERY, THEN WAIT FOR THE TASKS
    #    (_auditor_task may differ from the startup reference if engines
    #    stopped/resumed it during the session.)
    pending = []
    for task in (auditor_task, _auditor_task, _recovery_task):
        if task is not None:
            task.cancel()
            pending.append(task)
    if pending:
        try:
            await asyncio.gather(*pending)
        except asyncio.CancelledError:
            pass

    # 3) CANCEL tracked background tasks (status updaters, web console tasks)
    await _cancel_tracked_background_tasks()
    shutdown_background_tasks()

    # 4) DISCONNECT ALL SESSION-MANAGED CLIENTS (releases clients + proxy leases)
    await session_manager.disconnect_all()

    # 5) STOP PROXY TESTING + LEASE MANAGERS (after clients released leases)
    try:
        proxy_manager.stop_background_testing()
    except Exception as exc:
        logger.error(f"Failed to stop proxy background testing: {exc}")
    await proxy_lease_manager.stop()
    await account_lease_manager.stop()

    # 6) CLOSE DATABASE
    db.close()

    # 7) DISCONNECT BOT
    await real_bot.disconnect()
    logger.info("Shutdown complete.")


app = FastAPI(title="Enterprise Telegram Suite API", lifespan=lifespan)
app.include_router(console_router, prefix="/console")

# Initialize web_console with db and session_manager references
init_console_db(db)
init_console_session_manager(session_manager)

@app.get("/")
async def root_health_check():
    return {"status": "online", "service": "Telegram Bot Suite", "console": "/console"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# ──────────────────────────────────────────────
# 28. AUTO-RECOVERY LOOP
# ──────────────────────────────────────────────

async def _recover_failed_accounts(failed_accounts: list) -> int:
    recovered = 0
    semaphore = asyncio.Semaphore(max(1, int(CONFIG.get("HEALTH_SCAN_CONCURRENCY", 3))))
    acc_delay = CONFIG.get("RECOVERY_ACCOUNT_DELAY", (180, 360))

    async def _recover_one(acc) -> bool:
        phone = normalize_phone(str(acc.get("phone", "")))
        session_str = safe_session_str(acc)
        if not session_str:
            return False

        # 1) Never attempt terminal accounts (revoked/banned/deactivated/
        #    invalid/auth_key_duplicated/permanently_failed/quarantined).
        db_status = str(acc.get("status", "")).lower()
        if db_status in TERMINAL_DB_STATUSES:
            audit_logger.debug(
                f"RECOVERY_SKIP | +{phone} | terminal status={db_status}"
            )
            return False
        # 2) Never operate on an account currently owned by another worker
        #    (DM/adder/scraper/videochat).
        if await account_lease_manager.is_busy(phone):
            audit_logger.debug(
                f"RECOVERY_SKIP | +{phone} | account is busy/owned by a worker"
            )
            return False
        # 3) Never operate on an account in a login/OTP/2FA reservation or
        #    otherwise actively leased through SessionManager.
        if await session_manager.is_owned(phone):
            audit_logger.debug(
                f"RECOVERY_SKIP | +{phone} | login/active reservation in progress"
            )
            return False
        try:
            async with semaphore:
                # Human pacing between account recovery attempts.
                await asyncio.sleep(random.uniform(float(acc_delay[0]), float(acc_delay[1])))
                async with managed_client(acc) as client:
                    if await client.is_user_authorized():
                        await client.get_me()
                        await client.send_message("SpamBot", "/start")
                        await db.update_session_status_async(
                            phone, AccountStatus.ACTIVE, client.session.save())
                        audit_logger.info(f"🟢 Recovered +{phone} (spam mute lifted).")
                        return True
                    else:
                        # Retry/terminal classification via managed_client;
                        # managed_client also refuses to bypass an owned session.
                        audit_logger.debug(
                            f"RECOVERY_SKIP | +{phone} | client not authorized"
                        )
        except SessionAlreadyOwnedError:
            # Acquire race: account became owned by a worker or login.
            # Skip - never escalate into an account failure.
            audit_logger.debug(
                f"RECOVERY_SKIP | +{phone} | owned at acquire time (busy)"
            )
        except Exception as e:
            audit_logger.error(f"Auto-recovery failed for {phone}: {e}")
        return False

    results = await asyncio.gather(*(_recover_one(acc) for acc in failed_accounts))
    recovered = sum(1 for ok in results if ok)
    return recovered


async def auto_health_recovery_loop() -> None:
    """Auto-recovery engine: re-checks and recovers muted accounts with
    human-paced delays (3-6 min per account + micro-jitter)."""
    await asyncio.sleep(max(60, int(CONFIG.get("RECOVERY_INITIAL_DELAY", 600))))
    audit_logger.info("🏥 Auto-Recovery Background Engine Started.")

    sweep_interval = max(3600, int(CONFIG.get("RECOVERY_INTERVAL", 21600)))
    GLOBAL.update_recovery_state(phase="idle", next_sweep_in=sweep_interval)

    while True:
        if not await GLOBAL.is_health_check_active() or adder_engine.is_running or dm_engine.is_running or voice_engine.is_running:
            await asyncio.sleep(60)
            continue

        try:
            all_accounts = await db.get_all_accounts_raw()
            # Recovery sweep touches FAILED accounts only — banned, restricted
            # and revoked accounts are never operated on.
            failed_accounts = [acc for acc in all_accounts if acc.get("status") == AccountStatus.FAILED]

            if failed_accounts:
                audit_logger.info(
                    f"🏥 Recovery sweep starting: {len(failed_accounts)} failed/muted accounts, "
                    f"human-paced {CONFIG.get('RECOVERY_ACCOUNT_DELAY', (180, 360))[0]}-"
                    f"{CONFIG.get('RECOVERY_ACCOUNT_DELAY', (180, 360))[1]}s per account."
                )
                GLOBAL.update_recovery_state(phase="sweeping", total=len(failed_accounts))
                recovered = await _recover_failed_accounts(failed_accounts)
                GLOBAL.update_recovery_state(
                    phase="idle", last_recovered=recovered, next_sweep_in=sweep_interval)
                if recovered:
                    audit_logger.info(f"Auto-recovery pass recovered {recovered} accounts.")

        except Exception as e:
            GLOBAL.update_recovery_state(phase="idle", next_sweep_in=sweep_interval)
            audit_logger.error(f"Recovery loop error: {e}")

        await asyncio.sleep(sweep_interval)



class _AuthBotAdapter:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.sessions: Dict[str, TelegramClient] = {}
        self.pending_codes: Dict[str, dict] = {}

    def create_user_client(self, phone: str):
        clean_phone = normalize_phone(phone)
        existing = db.get_session_by_phone(clean_phone) or {}
        device = get_device_profile(existing) if existing else (
            random.choice(DEVICE_PROFILES) if DEVICE_PROFILES else {}
        )
        return session_manager.create_client(
            session_str=StringSession().save(),
            api_id=CONFIG["API_ID"],
            api_hash=CONFIG["API_HASH"],
            device=device,
            proxy=None,
        )

    def save_account_metadata(self, phone: str):
        clean_phone = normalize_phone(phone)
        client = self.sessions.get(clean_phone)
        if not client:
            return
        existing = db.get_session_by_phone(clean_phone) or {}
        db.save_authorized_session(
            clean_phone,
            client.session.save(),
            AccountStatus.ACTIVE,
            get_device_profile(existing),
            two_fa_password=None,
        )

    def save_twofa_password(self, phone: str, password: str):
        clean_phone = normalize_phone(phone)
        existing = db.get_session_by_phone(clean_phone) or {}
        db.save_authorized_session(
            clean_phone,
            existing.get("session_string") or existing.get("session") or StringSession().save(),
            AccountStatus.ACTIVE,
            get_device_profile(existing),
            two_fa_password=password,
        )


auth_bot = _AuthBotAdapter()


class LoginReq(BaseModel):
    phone: str

class VerifyReq(BaseModel):
    phone: str
    code: str

class Verify2FAReq(BaseModel):
    phone: str
    password: str

class BulkLoginReq(BaseModel):
    phones: list[str]


@app.post("/login")
async def api_login(req: LoginReq):
    phone = req.phone
    phone_key = normalize_phone(phone)
    login_owner = f"login:{phone_key}"
    client = None

    try:
        login_result = await shared_login_process(phone, login_owner)
        client = login_result["client"]
        code_hash = login_result["code_hash"]

        async with auth_bot._lock:
            auth_bot.pending_codes[phone_key] = {
                "client": client,
                "phone_code_hash": code_hash,
                "timeout": 120,
            }

        return {
            "status": "code_sent",
            "phone": phone,
            "message": "OTP successfully sent to device. Use /verify to confirm code."
        }

    except FloodWaitError as e:
        if client:
            try:
                await client.disconnect()
            except:
                pass
        raise HTTPException(429, f"Rate limited. Wait {e.seconds}s")
    except asyncio.TimeoutError:
        if client:
            try:
                await client.disconnect()
            except:
                pass
        raise HTTPException(408, "Request timeout: Could not connect to Telegram or send code")
    except HTTPException:
        if client:
            try:
                await client.disconnect()
            except:
                pass
        raise
    except Exception as e:
        if client:
            try:
                await client.disconnect()
            except:
                pass
        raise HTTPException(400, str(e))


@app.post("/verify")
async def api_verify(req: VerifyReq):
    phone, code = req.phone, req.code
    phone_key = normalize_phone(phone)

    async with auth_bot._lock:
        if phone_key not in auth_bot.pending_codes:
            raise HTTPException(404, "No pending login for this number. Call /login first.")
        pending = auth_bot.pending_codes[phone_key]
        client = pending["client"]

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=pending["phone_code_hash"])
        async with auth_bot._lock:
            auth_bot.sessions[phone_key] = client
            del auth_bot.pending_codes[phone_key]
        auth_bot.save_account_metadata(phone_key)

        me = await client.get_me()
        return {"status": "ok", "phone": phone, "name": f"{me.first_name} {me.last_name or ''}".strip(), "username": me.username, "id": me.id}

    except SessionPasswordNeededError:
        return {"status": "2fa_required", "phone": phone}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/verify_2fa")
async def api_verify_2fa(req: Verify2FAReq):
    phone = req.phone
    phone_key = normalize_phone(phone)

    async with auth_bot._lock:
        if phone_key not in auth_bot.pending_codes:
            raise HTTPException(404, "No pending login for this number. Call /login first.")
        client = auth_bot.pending_codes[phone_key]["client"]
    try:
        await client.sign_in(password=req.password)
        async with auth_bot._lock:
            auth_bot.sessions[phone_key] = client
            del auth_bot.pending_codes[phone_key]
        auth_bot.save_account_metadata(phone_key)
        auth_bot.save_twofa_password(phone_key, req.password)

        me = await client.get_me()
        return {"status": "ok", "phone": phone, "name": f"{me.first_name} {me.last_name or ''}".strip(), "username": me.username, "id": me.id}

    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/sessions")
async def api_sessions():
    async with auth_bot._lock:
        return {
            "active": list(auth_bot.sessions.keys()),
            "pending": list(auth_bot.pending_codes.keys()),
        }


@app.get(
    "/otp/{phone}",
    summary="Fetch OTP messages",
    description=(
        "Returns recent messages from Telegram's OTP sender (777000) for the given phone number. "
        "Use `since_seconds` to restrict to messages received in the last N seconds (default 300 = last 5 min). "
        "Use `limit` to control how many messages to return (default 5)."
    ),
)
async def get_otp(
    phone: str,
    limit: int = 5,
    since_seconds: int = 300,
):
    phone_key = normalize_phone(phone)
    async with auth_bot._lock:
        if phone_key not in auth_bot.sessions:
            raise HTTPException(404, "No active session for this number. Login first via /login.")
        client = auth_bot.sessions[phone_key]
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=since_seconds)
        messages = await client.get_messages(777000, limit=limit)
        results = []
        for msg in messages:
            if msg.date < cutoff:
                continue
            ist = msg.date + timedelta(hours=5, minutes=30)
            results.append({
                "id": msg.id,
                "text": msg.message,
                "received_at_ist": ist.strftime("%d-%m-%Y %H:%M:%S"),
                "received_at_utc": msg.date.strftime("%d-%m-%Y %H:%M:%S"),
            })
        return {"phone": phone, "count": len(results), "messages": results}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/session/{phone}")
async def api_check(phone: str):
    phone_key = normalize_phone(phone)
    async with auth_bot._lock:
        if phone_key in auth_bot.sessions:
            try:
                me = await auth_bot.sessions[phone_key].get_me()
                return {"status": "active", "name": f"{me.first_name} {me.last_name or ''}".strip(), "username": me.username}
            except Exception:
                return {"status": "expired"}
        if phone_key in auth_bot.pending_codes:
            return {"status": "pending_otp"}
    raise HTTPException(404, "No session found")


@app.delete("/session/{phone}")
async def api_logout(phone: str):
    phone_key = normalize_phone(phone)
    async with auth_bot._lock:
        if phone_key in auth_bot.sessions:
            try:
                await auth_bot.sessions[phone_key].log_out()
            except Exception:
                pass
            try:
                await auth_bot.sessions[phone_key].disconnect()
            except Exception:
                pass
            del auth_bot.sessions[phone_key]
            return {"status": "logged_out"}
        if phone_key in auth_bot.pending_codes:
            try:
                await auth_bot.pending_codes[phone_key]["client"].disconnect()
            except Exception:
                pass
            del auth_bot.pending_codes[phone_key]
            return {"status": "cancelled"}
    raise HTTPException(404, "No session found")


@app.post("/bulk_login")
async def api_bulk_login(req: BulkLoginReq):
    results = {"sent": [], "already": [], "failed": {}}
    for phone in req.phones:
        try:
            phone_key = normalize_phone(phone)
            async with auth_bot._lock:
                if phone_key in auth_bot.sessions:
                    results["already"].append(phone)
                    continue

            client = auth_bot.create_user_client(phone_key)
            await client.connect()
            if await client.is_user_authorized():
                async with auth_bot._lock:
                    auth_bot.sessions[phone_key] = client
                results["already"].append(phone)
                continue
            sent = await client.send_code_request(phone)
            async with auth_bot._lock:
                auth_bot.pending_codes[phone_key] = {"client": client, "phone_code_hash": sent.phone_code_hash, "timeout": sent.timeout}
            results["sent"].append(phone)
            await asyncio.sleep(3)
        except Exception as e:
            results["failed"][phone] = str(e)
    return results


# === File Browser ===
BASE_DIR = Path(__file__).resolve().parent


def _dir_listing(directory: Path, url_path: str) -> HTMLResponse:
    entries = sorted(directory.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    rows = ""
    if url_path.strip("/"):
        parent = "/" + "/".join(url_path.strip("/").split("/")[:-1])
        rows += f'<tr><td><a href="/files{parent}">.. (up)</a></td><td></td></tr>'
    for entry in entries:
        entry_url = f"/files/{url_path.strip('/')}/{entry.name}".replace("//", "/")
        size = f"{entry.stat().st_size:,} B" if entry.is_file() else "—"
        icon = "📄" if entry.is_file() else "📁"
        rows += f'<tr><td><a href="{entry_url}">{icon} {entry.name}</a></td><td>{size}</td></tr>'
    html = f"""<!DOCTYPE html>
<html><head><title>/{url_path}</title>
<style>body{{font-family:monospace;padding:20px}}table{{border-collapse:collapse;width:100%}}
td{{padding:6px 12px;border-bottom:1px solid #eee}}a{{text-decoration:none;color:#0066cc}}a:hover{{text-decoration:underline}}</style>
</head><body>
<h2>/{url_path}</h2><hr>
<table><tr><th align=left>Name</th><th align=left>Size</th></tr>{rows}</table>
</body></html>"""
    return HTMLResponse(html)


@app.get("/files", response_class=HTMLResponse)
@app.get("/files/{file_path:path}")
async def browse(file_path: str = ""):
    target = (BASE_DIR / file_path).resolve()
    base_resolved = BASE_DIR.resolve()

    # Strict path validation to prevent directory traversal and symlink attacks
    try:
        target.relative_to(base_resolved)
    except ValueError:
        raise HTTPException(403, "Access denied")

    if not target.exists():
        raise HTTPException(404, "Not found")
    if target.is_dir():
        return _dir_listing(target, file_path)
    return FileResponse(target, filename=target.name)


# === Health check endpoint (replaces separate HTTP health server) ===
@app.get("/health")
async def health():
    return {"status": "ok"}        

# ──────────────────────────────────────────────
# 29. SERVER LAUNCHER (MUST BE AT THE VERY END)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🌐 Binding Web Service to host 0.0.0.0 on port {port}...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
        workers=1
    )
