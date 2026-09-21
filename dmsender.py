#!/usr/bin/env python3
"""Enterprise DM sender engine."""

import os
import time
import asyncio
import logging
import random
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple, Set

from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio, InputPeerUser
from telethon.errors import (
    FloodWaitError, PeerFloodError, SessionRevokedError, AuthKeyDuplicatedError,
)
try:
    from telethon.errors import UserBannedInChannelError
except ImportError:
    UserBannedInChannelError = None

from config import CONFIG
from database import is_spam_park_active, is_module_rest_active
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
from exception_classifier import ErrorCategory, classify_exception

logger = logging.getLogger("DMSenderEngine")

# #region agent log
def _agent_dbg(hypothesis_id: str, location: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
    try:
        import json as _json
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug-b0b96b.log"), "a", encoding="utf-8") as _f:
            _f.write(_json.dumps({
                "sessionId": "b0b96b",
                "hypothesisId": hypothesis_id,
                "location": location,
                "message": message,
                "data": data or {},
                "timestamp": int(time.time() * 1000),
            }) + "\n")
    except Exception:
        pass
# #endregion

# Priority Override: ADMIN_ID mapped to environment variable as per system rules
ADMIN_ID = os.environ.get("ADMIN_ID")

# DB statuses that never enter SessionManager acquisition.
TERMINAL_STATUSES = frozenset({
    "revoked",
    "banned",
    "deactivated",
    "invalid",
    "auth_key_duplicated",
    "permanently_failed",
    "quarantined",
})


class _WizardStateStore(dict):

    def __init__(self, max_items: int = 1000, ttl_seconds: int = 1800):
        super().__init__()
        self.max_items = max_items
        self.ttl_seconds = ttl_seconds

    def _now(self) -> float:
        return time.monotonic()

    def _cleanup(self, now: float) -> None:
        expired = [
            key for key, (ts, _) in list(self.items())
            if now - ts > self.ttl_seconds
        ]
        for key in expired:
            dict.__delitem__(self, key)

    def __setitem__(self, key, value) -> None:
        now = self._now()
        self._cleanup(now)
        dict.__setitem__(self, key, (now, value))
        while len(self) > self.max_items:
            dict.__delitem__(self, next(iter(self)))

    def __getitem__(self, key):
        entry = dict.__getitem__(self, key)
        ts, value = entry
        if self._now() - ts > self.ttl_seconds:
            dict.__delitem__(self, key)
            raise KeyError(key)
        return value

    def __contains__(self, key) -> bool:
        try:
            self[key]
            return True
        except KeyError:
            return False

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key, default=None):
        try:
            value = self[key]
        except KeyError:
            return default
        dict.__delitem__(self, key)
        return value

    def set(self, key, value) -> None:
        self[key] = value

    def remove(self, key) -> None:
        dict.pop(self, key, None)


def _interval_pair(key: str, default: Tuple[float, float]) -> Tuple[float, float]:
    raw = CONFIG.get(key, default)
    if isinstance(raw, (tuple, list)) and len(raw) >= 2:
        low, high = float(raw[0]), float(raw[1])
        if high < low:
            low, high = high, low
        return low, high
    return default


_PEER_FALLBACK_ERRORS = frozenset({
    "PeerIdInvalidError",
    "UserIdInvalidError",
    "UsernameInvalidError",
    "UsernameNotOccupiedError",
})


def _normalized_username(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text or text.lower() in ("none", "null"):
        return None
    return text if text.startswith("@") else f"@{text}"


def iter_dm_entities(target: Any):
    """Yield DM peers that work on a *different* account than the scraper.

    Only @username is cross-account. A scraped access_hash is bound to the
    scraper session; sending it from another account is PEER_ID_INVALID and
    was the 971-fail blast. Hash-only / hidden-username users are skipped.
    """
    if isinstance(target, dict):
        username = _normalized_username(target.get("username"))
        if username:
            yield username
        return
    target_str = str(target).strip()
    if not target_str:
        return
    if target_str.isdigit():
        yield int(target_str)
        return
    yield target_str if target_str.startswith("@") else f"@{target_str}"


def resolve_dm_entity(target: Any):
    """Build a Telethon peer without a doomed raw-id or foreign-hash fallback."""
    for entity in iter_dm_entities(target):
        return entity
    raise ValueError("unresolvable_peer: need access_hash or username")


def partition_dm_targets(targets: List[Any]) -> Tuple[list, int]:
    """Split DM-ready (@username) targets from hidden-username skips."""
    ready: list = []
    skipped = 0
    for target in targets or []:
        if any(True for _ in iter_dm_entities(target)):
            ready.append(target)
        else:
            skipped += 1
    return ready, skipped


if UserBannedInChannelError is not None:
    DM_RESTRICTION_ERRORS = (PeerFloodError, UserBannedInChannelError)
else:
    DM_RESTRICTION_ERRORS = (PeerFloodError,)


def _batch_letter(index: int) -> str:
    n = max(0, int(index)) + 1
    out = ""
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out or "A"


def plan_dm_accounts(
    member_count: int,
    available_accounts: int,
    members_per_account: int = 30,
) -> int:
    """Use ceil(targets/30) accounts — never more than are available."""
    if member_count <= 0 or available_accounts <= 0:
        return 0
    per = max(1, int(members_per_account))
    needed = (int(member_count) + per - 1) // per
    return min(int(available_accounts), max(1, needed))


def compute_dm_worker_capacity(
    num_accounts: int,
    available_proxies: int,
    session_capacity: int,
    configured_limit: int,
) -> int:
    if num_accounts <= 0:
        return 0
    return max(
        1,
        min(num_accounts, available_proxies, session_capacity, configured_limit),
    )


class EnterpriseDMSender:
    def __init__(self, db, proxy_lease_manager=None, session_manager=None, account_lease_manager=None):
        self.db = db
        self.proxy_lease_manager = proxy_lease_manager  # 🔥 NEW: Lease manager integration
        self.session_manager = session_manager
        self.account_lease_manager = account_lease_manager
        self.is_running = False
        self.active_task = None
        self._campaign_halted = False
        # Bounded wizard state (PATCH 5): TTL + max-size, still dict-compatible.
        self.wizard_state: _WizardStateStore = _WizardStateStore(
            max_items=1000,
            ttl_seconds=1800,
        )
        self.stats = {
            "total_sent": 0,
            "failed": 0,
            "skipped": 0,
            "accounts_used": 0,
            "accounts_down": 0,
            "total_targets": 0,
            "fail_reasons": {},
        }

        # P0-CLOSEOUT: DM lifecycle instrumentation
        self.lifecycle_events: list = []        # structured event log (capped)
        self.max_lifecycle_events = 500
        self.worker_states: Dict[int, str] = {}  # worker_id -> current waiting/state label
        self._dm_stall_timeout = 60.0           # worker must never wait silently longer than this
        self._active_workers: List = []
        self._campaign_metrics: Optional[Dict[str, Any]] = None

        # PATCH 5: tunable bounds (tests may shrink these)
        self._queue_poll_seconds = 0.5          # worker queue-wake granularity
        self._resource_wait_bounds: Tuple[float, float] = (0.5, 1.5)
        self._busy_exclusion_seconds = 2.0      # don't re-select a busy account faster than this
        self._reporter_interval_seconds = 8.0   # live dashboard refresh interval
        self._human_delay_override: Optional[Tuple[float, float]] = None
        self._account_launch_delay_override: Optional[Tuple[float, float]] = None
        self._lease_retry_delay: Tuple[float, float] = (15.0, 30.0)
        self.resting: List[str] = []
        self.batch_reports: list = []
        self._batch_index = 0
        self._flood_delay_cap = int(CONFIG.get("DM_MAX_RETRY_DELAY", 60))
        self.max_target_attempts = 5            # Telegram-visible retry budget per target
        self._attempt_counts: Dict[str, int] = {}
        self._used_phones = set()

    def _emit(self, event: str, worker: Optional[int] = None,
              phone: Optional[str] = None, detail: Optional[str] = None) -> None:
        """Append a structured DM lifecycle event (capped)."""
        self.lifecycle_events.append({
            "ts": datetime.utcnow().isoformat() + "Z",
            "event": event,
            "worker": worker,
            "phone": phone,
            "detail": detail,
        })
        if len(self.lifecycle_events) > self.max_lifecycle_events:
            self.lifecycle_events = self.lifecycle_events[-self.max_lifecycle_events:]
        logger.debug(f"DM_EVENT | {event} | worker={worker} | phone={phone} | {detail}")

    def get_lifecycle_events(self) -> list:
        """Return a copy of the structured event log for tests/status."""
        return list(self.lifecycle_events)

    def _set_worker_state(self, worker_id: int, state: str,
                          phone: Optional[str] = None,
                          detail: Optional[str] = None) -> None:
        self.worker_states[worker_id] = state
        self._emit(state, worker=worker_id, phone=phone, detail=detail)

    # ──────────────────────────────────────────────
    # Resource capacity helpers (read-only snapshots)
    # ──────────────────────────────────────────────

    async def _get_available_proxies(self) -> int:
        """Read-only proxy capacity snapshot (login-reserve aware)."""
        if self.proxy_lease_manager is None:
            return 0
        usable = getattr(self.proxy_lease_manager, "get_usable_count_async", None)
        if usable is not None:
            return int(await usable())
        async_cap = getattr(self.proxy_lease_manager, "get_available_count_async", None)
        if async_cap is not None:
            return int(await async_cap())
        if hasattr(self.proxy_lease_manager, "get_available_count"):
            return int(self.proxy_lease_manager.get_available_count())
        return 0

    async def _proxies_exhausted(self) -> bool:
        if self.proxy_lease_manager is None:
            return False
        try:
            count = await self._get_available_proxies()
        except Exception:
            count = 0
        return count <= 0

    async def _get_session_capacity(self, configured_limit: int) -> int:
        if self.session_manager is None:
            return configured_limit
        try:
            stats = await self.session_manager.get_stats()
            max_clients = int(
                getattr(self.session_manager, "_max_active_clients", configured_limit)
            )
            return max(1, max_clients - int(stats.get("active_clients", 0)))
        except Exception:
            return configured_limit

    async def _bounded_resource_wait(self) -> bool:
        """Wait without busy-looping. Returns whether the campaign still runs."""
        low, high = self._resource_wait_bounds
        await asyncio.sleep(random.uniform(low, high))
        return self.is_running

    def _human_delay(self) -> float:
        if self._human_delay_override is not None:
            low, high = self._human_delay_override
            return random.uniform(low, high)
        low, high = _interval_pair("DM_HUMAN_INTERVAL", (25.0, 45.0))
        return random.uniform(low, high)

    # ──────────────────────────────────────────────
    # Account eligibility
    # ──────────────────────────────────────────────

    def _filter_eligible_accounts(self, account_docs: list) -> list:
        eligible = []
        for doc in account_docs or []:
            status = str(doc.get("status", "") or "").lower()
            if status in TERMINAL_STATUSES:
                continue
            if is_spam_park_active(doc):
                continue
            if is_module_rest_active(doc, "dmsender"):
                continue
            phone = str(doc.get("phone", "") or "").strip()
            if not phone:
                continue
            eligible.append(doc)
        return eligible

    @staticmethod
    def _target_key(target: Any) -> str:
        if isinstance(target, dict):
            uid = str(target.get("user_id") or "")
            uname = str(target.get("username") or "")
            return f"{uid}:{uname}"
        return str(target)

    async def _select_eligible_account(
        self,
        candidate_accounts: list,
        busy_accounts: Dict[str, float],
        rr: Dict[str, int],
        rr_lock: asyncio.Lock,
    ):
        if not candidate_accounts:
            return None
        now = time.monotonic()
        for key in [k for k, exp in busy_accounts.items() if exp <= now]:
            busy_accounts.pop(key, None)
        async with rr_lock:
            for _ in range(len(candidate_accounts)):
                idx = rr["idx"] % len(candidate_accounts)
                rr["idx"] += 1
                doc = candidate_accounts[idx]
                phone = str(doc.get("phone", "") or "").strip().replace("+", "")
                if not phone:
                    continue
                if busy_accounts.get(phone, 0) > now:
                    continue
                return doc
        return None

    def _all_accounts_long_parked(
        self,
        candidate_accounts: list,
        busy_accounts: Dict[str, float],
        min_remaining: float = 60.0,
    ) -> bool:
        """True when every remaining account is parked longer than min_remaining.

        Distinguishes brief contention (2s busy exclusion) from flood/restriction
        parks (minutes–hours) so the campaign can finish instead of spinning.
        """
        if not candidate_accounts:
            return True
        now = time.monotonic()
        remaining = []
        for doc in candidate_accounts:
            phone = str(doc.get("phone", "") or "").strip().replace("+", "")
            if not phone:
                continue
            remaining.append(busy_accounts.get(phone, 0) - now)
        return bool(remaining) and min(remaining) > min_remaining

    def _mark_account_temporarily_failed(self, phone: str, reason: str) -> None:
        marker = getattr(self.db, "mark_account_failed", None)
        if not callable(marker):
            return
        try:
            marker(phone, reason[:120])
        except Exception:
            logger.debug("DM_MARK_FAILED_SKIP | phone=%s", phone)

    async def _park_module_rest(self, phone: str, reason: str, hours: Optional[float] = None) -> None:
        """24h rest for dmsender only. Status stays active; adder can still use it."""
        if hours is None:
            hours = float(CONFIG.get("SPAM_RECHECK_HOURS", 24.0) or 24.0)
        park_async = getattr(self.db, "park_module_rest_async", None)
        try:
            if callable(park_async):
                await park_async(phone, "dmsender", reason[:120], hours)
                return
            park_sync = getattr(self.db, "park_module_rest", None)
            if callable(park_sync):
                await asyncio.to_thread(park_sync, phone, "dmsender", reason[:120], hours)
                return
            park_legacy = getattr(self.db, "park_spam_limited_async", None)
            if callable(park_legacy):
                await park_legacy(phone, reason[:120], hours)
        except Exception:
            logger.debug("DM_PARK_MODULE_SKIP | phone=%s", phone)

    def _drop_candidate(self, phone: str, candidate_accounts: list) -> None:
        for doc in list(candidate_accounts or []):
            candidate_phone = str(doc.get("phone", "") or "").strip().replace("+", "")
            if candidate_phone == phone:
                try:
                    candidate_accounts.remove(doc)
                except ValueError:
                    pass

    # ──────────────────────────────────────────────
    # Target helpers
    # ──────────────────────────────────────────────

    def _requeue_target(self, target: Any, target_queue: asyncio.Queue, counter: dict) -> None:
        target_queue.put_nowait(target)
        counter["queued"] += 1
        if self._campaign_metrics is not None:
            self._campaign_metrics["queued"] = counter["queued"]

    def _maybe_requeue(self, worker_id: int, phone: str, target: Any,
                       consume_attempt: bool = True) -> bool:
        if not consume_attempt:
            return True
        key = self._target_key(target)
        self._attempt_counts[key] = self._attempt_counts.get(key, 0) + 1
        if self._attempt_counts[key] >= self.max_target_attempts:
            self.stats["failed"] += 1
            if self._campaign_metrics is not None:
                self._campaign_metrics["failed"] += 1
            self._set_worker_state(
                worker_id,
                "TARGET_FAILED",
                phone=phone,
                detail=f"max_attempts={self.max_target_attempts}",
            )
            self._emit("send_failure", worker=worker_id, phone=phone,
                       detail=f"target exhausted retry budget")
            return False
        return True

    def _record_result(self, worker_id: int, phone: str, result: str) -> None:
        if result == "sent":
            self.stats["total_sent"] += 1
            if self._campaign_metrics is not None:
                self._campaign_metrics["completed"] += 1
            self._used_phones.add(phone)
            self._set_worker_state(worker_id, "SEND_SUCCESS", phone=phone)
        else:
            self.stats["failed"] += 1
            self._bump_fail_reason(result)
            if self._campaign_metrics is not None:
                self._campaign_metrics["failed"] += 1
            self._set_worker_state(worker_id, "TARGET_FAILED", phone=phone, detail=result)

    async def _handle_terminal_account(
        self,
        worker_id: int,
        phone: str,
        reason: str,
        category: ErrorCategory,
        candidate_accounts: list,
    ) -> None:
        self.stats["accounts_down"] += 1
        self._emit("TERMINAL_ACCOUNT", worker=worker_id, phone=phone, detail=reason[:80])
        if self.session_manager is not None:
            try:
                await self.session_manager.mark_quarantined(
                    phone,
                    reason=reason[:100],
                    category=category,
                )
            except Exception:
                pass
        for doc in list(candidate_accounts or []):
            candidate_phone = str(doc.get("phone", "") or "").strip().replace("+", "")
            if candidate_phone == phone:
                try:
                    candidate_accounts.remove(doc)
                except ValueError:
                    pass

    async def _handle_operation_error(
        self,
        worker_id: int,
        phone: Optional[str],
        target: Any,
        exc: BaseException,
        candidate_accounts: list,
        target_queue: asyncio.Queue,
        counter: dict,
        busy_accounts: Optional[Dict[str, float]] = None,
    ) -> None:
        result = classify_exception(exc)
        category = result.category
        logger.warning(
            "DM_WORKER_ERROR | worker=%s | phone=%s | target=%s | exc=%s | category=%s",
            worker_id,
            phone or "",
            self._target_key(target),
            type(exc).__name__,
            category.value,
        )
        # #region agent log
        _agent_dbg("H4", "dmsender.py:_handle_operation_error", "classified send error", {
            "exc_type": type(exc).__name__,
            "category": category.value,
            "retryable": bool(result.retryable),
            "quarantinable": bool(result.is_quarantinable),
        })
        # #endregion
        if result.is_quarantinable:
            self._set_worker_state(worker_id, "TERMINAL_ACCOUNT",
                                   phone=phone, detail=category.value)
            await self._handle_terminal_account(
                worker_id, phone or "", result.reason, category, candidate_accounts,
            )
            if self._maybe_requeue(worker_id, phone or "", target):
                self._requeue_target(target, target_queue, counter)
            return
        park_seconds = {
            ErrorCategory.PEER_FLOOD: 12 * 3600,
            ErrorCategory.ACCOUNT_RESTRICTED: 6 * 3600,
            ErrorCategory.ACCOUNT_FLOOD: 15 * 60,
        }.get(category)
        if park_seconds and phone:
            if busy_accounts is not None:
                busy_accounts[phone] = time.monotonic() + park_seconds
            if park_seconds >= 3600:
                if category == ErrorCategory.PEER_FLOOD:
                    await self._park_module_rest(phone, result.reason)
                self._drop_candidate(phone, candidate_accounts)
                # #region agent log
                _agent_dbg("H4", "dmsender.py:_handle_operation_error", "long park dropped candidate", {
                    "category": category.value,
                    "park_seconds": park_seconds,
                    "spam_park": category == ErrorCategory.PEER_FLOOD,
                })
                # #endregion
            self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                   phone=phone, detail=category.value)
            if self._maybe_requeue(worker_id, phone, target, consume_attempt=False):
                self._requeue_target(target, target_queue, counter)
            return
        if not result.retryable:
            self._set_worker_state(worker_id, "TARGET_FAILED",
                                   phone=phone, detail=category.value)
            self.stats["failed"] += 1
            self._bump_fail_reason(type(exc).__name__)
            if self._campaign_metrics is not None:
                self._campaign_metrics["failed"] += 1
            logger.warning(
                "DM_TARGET_FAIL | account=%s | err=%s | category=%s | %s",
                phone or "", type(exc).__name__, category.value, result.reason[:80],
            )
            self._emit("send_failure", worker=worker_id, phone=phone,
                       detail=f"{category.value}: {result.reason[:60]}")
            return
        self._set_worker_state(worker_id, "RETRYING", phone=phone, detail=category.value)
        await self._bounded_resource_wait()
        if self._maybe_requeue(worker_id, phone or "", target):
            self._requeue_target(target, target_queue, counter)

    # ──────────────────────────────────────────────
    # Status generators
    # ──────────────────────────────────────────────

    async def _generate_live_status(self) -> str:
        elapsed = max(1, int(time.time() - getattr(self, '_campaign_start_time', time.time())))
        elapsed_min = elapsed / 60
        rate = round(self.stats['total_sent'] / elapsed_min, 1) if elapsed_min > 0.1 else 0

        remaining, progress_pct = self._campaign_remaining_and_pct()
        if rate > 0:
            eta_min = round(remaining / rate)
            eta_str = f"{eta_min // 60}h {(eta_min % 60)}m" if eta_min > 60 else f"{eta_min}m"
        else:
            eta_str = "Calculating..."

        active_workers = sum(1 for t in getattr(self, '_active_workers', []) if not t.done())
        processing = sum(1 for s in self.worker_states.values() if s == "PROCESSING")
        waiting = sum(1 for s in self.worker_states.values()
                      if s in ("WAITING_FOR_PROXY", "WAITING_FOR_ACCOUNT",
                               "SESSION_BUSY", "RETRYING", "ACQUIRING_SESSION"))

        proxy_stats = ""
        if self.proxy_lease_manager:
            try:
                stats = await self.proxy_lease_manager.get_stats()
                proxy_stats = (
                    f"🛡️ **PROXY POOL**\n"
                    f"   🟢 Available: `{stats['available_proxies']}`\n"
                    f"   🔒 Leased: `{stats['current_active_leases']}`\n"
                    f"   🧊 In Cooldown: `{stats['proxies_in_cooldown']}`\n"
                )
            except Exception:
                proxy_stats = "🛡️ **PROXY POOL** (unavailable)\n"

        metrics = getattr(self, '_campaign_metrics', None) or {}
        snapshot = ""
        if metrics:
            snapshot = (
                f"🎛️ **CAMPAIGN RESOURCES**\n"
                f"   👥 Eligible Accounts: `{metrics.get('eligible_accounts', 0)}`\n"
                f"   🎯 Needed / unused: `{metrics.get('accounts_needed', 0)}` / `{metrics.get('unused_spares', 0)}`\n"
                f"   🧮 Effective Workers: `{metrics.get('effective_worker_capacity', 0)}`\n"
                f"   ⏳ Queued: `{metrics.get('queued', 0)}` | Inflight: `{metrics.get('inflight', 0)}`\n"
            )

        state_line = ""
        if self.worker_states:
            grp = {}
            for s in self.worker_states.values():
                grp[s] = grp.get(s, 0) + 1
            state_line = " | ".join(f"{k}={v}" for k, v in grp.items())

        return (
            f"📊 **LIVE DM CAMPAIGN DASHBOARD**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📨 Sent: `{self.stats['total_sent']}` | Failed: `{self.stats['failed']}`\n"
            f"{self._skip_line()}"
            f"{self._fail_reason_line()}"
            f"🎯 Targets: `{self.stats['total_targets']}`\n"
            f"   Progress: `{progress_pct}%`\n"
            f"⚡ Rate: `{rate} msgs/min` | Runtime: `{elapsed // 60}m {elapsed % 60}s` | ETA: `{eta_str}`\n"
            f"👷 Active Workers: `{active_workers}` | Waiting: `{waiting}` | Processing: `{processing}`\n"
            f"👥 Accounts: used=`{self.stats['accounts_used']}` down=`{self.stats['accounts_down']}` rest=`{len(self.resting)}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{snapshot}"
            f"{proxy_stats}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔄 Worker States: `{state_line or 'idle'}`\n"
            f"🔄 *Auto-updates every 8s • `/dm_status` for manual check*"
        )

    async def _generate_detailed_status(self) -> str:
        elapsed = max(1, int(time.time() - getattr(self, '_campaign_start_time', time.time())))
        elapsed_min = elapsed / 60
        rate = round(self.stats['total_sent'] / elapsed_min, 1) if elapsed_min > 0.1 else 0

        remaining, progress_pct = self._campaign_remaining_and_pct()
        if rate > 0:
            eta_min = round(remaining / rate)
            eta_str = f"{eta_min // 60}h {(eta_min % 60)}m" if eta_min > 60 else f"{eta_min}m"
        else:
            eta_str = "Calculating..."

        proxy_stats = ""
        if self.proxy_lease_manager:
            try:
                stats = await self.proxy_lease_manager.get_stats()
                available = stats.get('available_proxies', 0)
                try:
                    if hasattr(self.proxy_lease_manager, "get_available_count_async"):
                        available = await self.proxy_lease_manager.get_available_count_async()
                except Exception:
                    pass
                proxy_stats = (
                    f"🛡️ **PROXY POOL**\n"
                    f"   🟢 Available: `{available}`\n"
                    f"   🔒 Leased: `{stats['current_active_leases']}`\n"
                    f"   🧊 In Cooldown: `{stats['proxies_in_cooldown']}`\n"
                    f"   📊 Total Acquires: `{stats['total_acquires']}`\n"
                )
            except Exception:
                proxy_stats = "🛡️ **PROXY POOL** (unavailable)\n"

        active_workers = sum(1 for t in getattr(self, '_active_workers', []) if not t.done())

        if self.stats['accounts_down'] > self.stats['accounts_used'] * 0.5:
            health_icon = "🔴"
            health_text = "CRITICAL - Many accounts down"
        elif self.stats['accounts_down'] > self.stats['accounts_used'] * 0.2:
            health_icon = "🟠"
            health_text = "WARNING - Accounts dropping"
        elif rate > 0:
            health_icon = "🟢"
            health_text = "HEALTHY - Sending normally"
        else:
            health_icon = "🟡"
            health_text = "WAITING - No proxies available"

        return (
            f"📊 **LIVE DM CAMPAIGN DASHBOARD**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{health_icon} **System Health:** {health_text}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📨 **MESSAGING METRICS**\n"
            f"   ✅ Sent: `{self.stats['total_sent']}`\n"
            f"   ❌ Failed: `{self.stats['failed']}`\n"
            f"{self._skip_line()}"
            f"{self._fail_reason_line()}"
            f"   🎯 Total Targets: `{self.stats['total_targets']}`\n"
            f"   📈 Progress: `{progress_pct}%`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ **PERFORMANCE**\n"
            f"   🚀 Rate: `{rate} msgs/min`\n"
            f"   ⏱️ Runtime: `{elapsed // 60}m {elapsed % 60}s`\n"
            f"   🕒 ETA: `{eta_str}`\n"
            f"   👷 Active Workers: `{active_workers}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 **ACCOUNT POOL**\n"
            f"   🟢 Total Accounts: `{self.stats['accounts_used']}`\n"
            f"   💀 Banned/Dropped: `{self.stats['accounts_down']}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{proxy_stats}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔄 *Auto-updates every 8s • `/dm_status` for manual check*"
        )

    # ──────────────────────────────────────────────
    # Campaign control
    # ──────────────────────────────────────────────

    def reset_stats(self):
        self.stats = {
            "total_sent": 0,
            "failed": 0,
            "skipped": 0,
            "accounts_used": 0,
            "accounts_down": 0,
            "total_targets": 0,
            "fail_reasons": {},
        }

    def _bump_fail_reason(self, reason: str) -> None:
        key = str(reason or "unknown")[:48]
        reasons = self.stats.setdefault("fail_reasons", {})
        reasons[key] = int(reasons.get(key, 0) or 0) + 1

    def _fail_reason_line(self) -> str:
        reasons = self.stats.get("fail_reasons") or {}
        if not reasons:
            return ""
        top = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:4]
        return "   ⚠️ Fails: `" + ", ".join(f"{k}={v}" for k, v in top) + "`\n"

    def _skip_line(self) -> str:
        n = int(self.stats.get("skipped") or 0)
        if n <= 0:
            return ""
        return f"⏭️ Skipped (no @username): `{n}`\n"

    def _campaign_remaining_and_pct(self) -> Tuple[int, float]:
        sent = int(self.stats.get("total_sent") or 0)
        failed = int(self.stats.get("failed") or 0)
        skipped = int(self.stats.get("skipped") or 0)
        total = int(self.stats.get("total_targets") or 0)
        remaining = max(0, total - sent - failed - skipped)
        pct = round((sent + failed + skipped) / max(1, total) * 100, 1)
        return remaining, pct

    def halt_campaign(self):
        """Stops the DM campaign immediately (resource cleanup is left to the
        SessionManager context managers, which unwind during cancellation)."""
        self.is_running = False
        self._campaign_halted = True
        if self.active_task:
            self.active_task.cancel()

    async def execute_dm_campaign(self, target_list: list, message_text: str, media_path: str, limit: int, ui_callback):

        self.is_running = True
        self._campaign_halted = False
        self.reset_stats()
        self._campaign_start_time = time.time()
        self._attempt_counts = {}
        self._used_phones = set()
        self._emit("campaign_start", detail=f"targets={len(target_list)}")
        # Fully stop the background auditor/recovery while the campaign owns the pool.
        try:
            notify_auditor_stop()
        except Exception:
            pass
        try:
            final_text = str(message_text).strip() if message_text else ""
            if final_text.lower() == "skip" or not final_text:
                final_text = None

            if not final_text and (not media_path or not os.path.exists(str(media_path))):
                await ui_callback("❌ **Campaign Aborted:** Both Text and Media payload cannot be empty. Setup aborted.")
                return

            all_accounts = await self.db.get_active_target_sessions()
            if not all_accounts:
                await ui_callback("❌ **Campaign Aborted:** Koi active verified session nahi mila.")
                return

            # PATCH 5: terminal accounts never reach worker acquisition.
            candidate_accounts = self._filter_eligible_accounts(all_accounts)
            if not candidate_accounts:
                await ui_callback(
                    "❌ **Campaign Aborted:** Koi eligible active session nahi mila "
                    "(all accounts terminal/filtered)."
                )
                return

            return await self._dynamic_rolling_worker(
                target_list, final_text, media_path, limit, ui_callback, candidate_accounts
            )
        finally:
            # Always reset, even on unexpected exceptions — otherwise the
            # engine reports "Occupied" forever.
            self.is_running = False
            # Background auditor/recovery can resume now that the campaign is done.
            try:
                notify_auditor_resume()
            except Exception:
                pass

    # ──────────────────────────────────────────────
    # Core engine
    # ──────────────────────────────────────────────

    async def _dynamic_rolling_worker(
        self, target_list: list, final_text: Optional[str], media_path: str,
        limit: int, ui_callback, candidate_accounts: list
    ):
        ready, skipped_n = partition_dm_targets(list(target_list or []))
        targets = list(ready[:limit] if limit > 0 else ready)
        candidate_accounts = self._filter_eligible_accounts(candidate_accounts)
        self.stats["total_targets"] = len(targets) + skipped_n
        self.stats["skipped"] = skipped_n
        self.resting = []
        self.batch_reports = []
        self._batch_index = 0
        if skipped_n:
            logger.warning(
                "DM_SKIP_NO_USERNAME | skipped=%s | queued=%s | scraper hashes cannot DM from other accounts",
                skipped_n, len(targets),
            )
        self._emit("target_queue_created",
                   detail=f"targets={len(targets)}, skipped={skipped_n}, accounts={len(candidate_accounts)}")
        if not targets:
            await ui_callback(
                "❌ **Campaign Aborted:** Koi DM-ready target nahi mila. "
                "Hidden username / sirf scraper hash se doosre account DM nahi kar sakte. "
                "Jin users ke paas `@username` ho unhe scrape karke dubara try karein."
            )
            return

        BATCH_SIZE = max(1, int(CONFIG.get("DM_BATCH_SIZE", 10)))
        MAX_CONCURRENT_BATCHES = max(1, min(2, int(CONFIG.get("DM_MAX_CONCURRENT_BATCHES", 2))))
        MEMBERS_PER_ACCOUNT = max(1, int(CONFIG.get("DM_MEMBERS_PER_ACCOUNT", 30)))
        LIVE_CAP = BATCH_SIZE * MAX_CONCURRENT_BATCHES
        SHORT_FLOOD_WAIT = max(1, int(CONFIG.get("DM_SHORT_FLOOD_WAIT", 30)))
        if self._account_launch_delay_override is not None:
            ACCOUNT_LAUNCH_DELAY = self._account_launch_delay_override
        else:
            ACCOUNT_LAUNCH_DELAY = _interval_pair("DM_ACCOUNT_LAUNCH_DELAY", (8.0, 15.0))

        n_targets = len(targets)
        accounts_needed = plan_dm_accounts(
            n_targets, len(candidate_accounts), MEMBERS_PER_ACCOUNT,
        )
        try:
            _configured_limit = int(CONFIG.get("DM_MAX_WORKERS", 20))
        except (TypeError, ValueError):
            _configured_limit = 20
        _configured_limit = min(_configured_limit, LIVE_CAP)
        try:
            _available_proxies = await self._get_available_proxies()
        except Exception:
            _available_proxies = 0
        try:
            _session_capacity = await self._get_session_capacity(_configured_limit)
        except Exception:
            _session_capacity = _configured_limit

        if accounts_needed <= 0:
            MAX_LIVE_ACCOUNTS = 0
        elif _available_proxies > 0:
            MAX_LIVE_ACCOUNTS = max(
                1,
                min(accounts_needed, _configured_limit, _available_proxies, _session_capacity),
            )
        else:
            MAX_LIVE_ACCOUNTS = max(1, min(accounts_needed, _configured_limit, _session_capacity))

        work_accounts = list(candidate_accounts[:accounts_needed])
        spare_accounts: deque = deque(candidate_accounts[accounts_needed:])
        remaining_q: deque = deque(work_accounts)
        batches_required = (
            (accounts_needed + BATCH_SIZE - 1) // BATCH_SIZE if accounts_needed else 0
        )
        self.stats["accounts_used"] = accounts_needed
        self._campaign_metrics = {
            "eligible_accounts": len(candidate_accounts),
            "accounts_needed": accounts_needed,
            "unused_spares": len(spare_accounts),
            "batches_required": batches_required,
            "available_proxies": _available_proxies,
            "available_session_capacity": _session_capacity,
            "effective_worker_capacity": MAX_LIVE_ACCOUNTS,
            "target_count": n_targets,
            "queued": n_targets,
            "inflight": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "cancelled": 0,
            "unprocessed": 0,
        }
        self._emit(
            "worker_started",
            detail=(
                f"effective_capacity={MAX_LIVE_ACCOUNTS} "
                f"(needed={accounts_needed}/{len(candidate_accounts)}, "
                f"spares={len(spare_accounts)}, batches={batches_required}, "
                f"proxies={_available_proxies})"
            ),
        )
        await ui_callback(
            f"🚀 **DM Engine Started (2-batch pipeline)**\n"
            f"Targets: `{n_targets}` · Skipped no-username: `{skipped_n}` · "
            f"Accounts needed: `{accounts_needed}` / "
            f"`{len(candidate_accounts)}` · Unused: `{len(spare_accounts)}`\n"
            f"Batch size `{BATCH_SIZE}` · max parallel `{MAX_CONCURRENT_BATCHES}` · "
            f"live cap `{MAX_LIVE_ACCOUNTS}`"
        )

        target_queue: asyncio.Queue = asyncio.Queue()
        for t in targets:
            target_queue.put_nowait(t)
        counter = {"queued": n_targets, "inflight": 0}
        queue_lock = asyncio.Lock()
        blocked_phones: Set[str] = set()
        self._active_workers = []
        self._batch_tasks: List[asyncio.Task] = []
        accounts_in_flight = 0
        in_flight_lock = asyncio.Lock()

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
                kept_sp = deque(d for d in spare_accounts if _phone_key(d) != key)
                spare_accounts.clear()
                spare_accounts.extend(kept_sp)

        async def _rest_account_24h(phone: str, err_name: str) -> None:
            await _block_for_run(phone)
            hours = float(CONFIG.get("SPAM_RECHECK_HOURS", 24.0) or 24.0)
            await self._park_module_rest(phone, err_name, hours)
            until_str = (datetime.now() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M")
            display = f"+{phone} unavailable (24h DM rest) until {until_str} · {err_name}"
            self.resting.append(display)
            logger.warning("DM_REST_24H | %s", display)
            try:
                await ui_callback(
                    f"⏸️ Account +{phone} unavailable for DM · 24h rest · "
                    f"{err_name} · until {until_str}"
                )
            except Exception:
                pass

        def _note_target_fail(reason: str = "unknown") -> None:
            self.stats["failed"] += 1
            self._bump_fail_reason(reason)
            if self._campaign_metrics is not None:
                self._campaign_metrics["failed"] += 1

        def _requeue(target: Any) -> None:
            target_queue.put_nowait(target)
            counter["queued"] += 1
            if self._campaign_metrics is not None:
                self._campaign_metrics["queued"] = counter["queued"]

        async def _pending_accounts() -> bool:
            async with queue_lock:
                return any(not _is_blocked(d) for d in remaining_q) or any(
                    not _is_blocked(d) for d in spare_accounts
                )

        async def _pop_wave(max_n: int) -> List[dict]:
            async with queue_lock:
                wave: List[dict] = []
                while len(wave) < max_n:
                    doc = None
                    if remaining_q:
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
                return None

        async def run_one_account(account_doc: dict) -> Tuple[int, bool]:
            sent_here = 0
            needs_replace = False
            if not self.is_running:
                return 0, False
            if _is_blocked(account_doc):
                return 0, True
            phone = str(account_doc.get("phone", "") or "").strip()
            clean_phone = phone.replace("+", "")
            if not clean_phone:
                return 0, True

            while self.is_running:
                if _is_blocked(clean_phone):
                    return sent_here, True
                try:
                    async with self.session_manager.acquire(
                        clean_phone,
                        module="dmsender",
                        worker_id=f"dm:{clean_phone}",
                        auto_release=True,
                        timeout=10.0,
                    ) as lease:
                        if lease is None:
                            if await self._proxies_exhausted():
                                self._set_worker_state(
                                    0, "WAITING_FOR_PROXY",
                                    phone=clean_phone, detail="lease=None",
                                )
                            else:
                                self._set_worker_state(
                                    0, "WAITING_FOR_ACCOUNT",
                                    phone=clean_phone, detail="lease=None",
                                )
                            self._handle_contended_lease(clean_phone)
                            await self._bounded_resource_wait()
                            if not self.is_running:
                                return sent_here, False
                            continue

                        client = lease.client
                        self._set_worker_state(0, "CONNECTING", phone=clean_phone)
                        if not client.is_connected():
                            await client.connect()
                        if not await client.is_user_authorized():
                            raise SessionRevokedError(request=None)

                        pending_retry = None
                        while self.is_running:
                            is_restriction_retry = False
                            if pending_retry is not None:
                                target = pending_retry
                                pending_retry = None
                                is_restriction_retry = True
                            else:
                                try:
                                    target = target_queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    return sent_here, False
                                counter["queued"] = max(0, counter["queued"] - 1)
                                counter["inflight"] += 1
                                if self._campaign_metrics is not None:
                                    self._campaign_metrics["queued"] = counter["queued"]
                                    self._campaign_metrics["inflight"] = counter["inflight"]

                            try:
                                self._set_worker_state(0, "PROCESSING", phone=clean_phone)
                                result = await self._process_target(
                                    client, target, final_text, media_path,
                                )
                                self._record_result(0, clean_phone, result)
                                sent_here += 1
                                await asyncio.sleep(self._human_delay())
                            except DM_RESTRICTION_ERRORS as fl_err:
                                err_name = type(fl_err).__name__
                                if not is_restriction_retry:
                                    logger.info(
                                        "DM_RESTRICT_RETRY | account=%s | err=%s | retrying this send once",
                                        clean_phone, err_name,
                                    )
                                    pending_retry = target
                                    continue
                                _requeue(target)
                                needs_replace = True
                                logger.warning(
                                    "DM_RESTRICT_STOP | account=%s | err=%s | target requeued, 24h DM rest",
                                    clean_phone, err_name,
                                )
                                await _rest_account_24h(clean_phone, err_name)
                                return sent_here, True
                            except FloodWaitError as exc:
                                seconds = int(getattr(exc, "seconds", 30) or 30)
                                _requeue(target)
                                if 0 < seconds <= SHORT_FLOOD_WAIT:
                                    self._set_worker_state(
                                        0, "WAITING_FOR_ACCOUNT",
                                        phone=clean_phone, detail=f"flood {seconds}s",
                                    )
                                    await asyncio.sleep(max(0, seconds))
                                    continue
                                needs_replace = True
                                await _block_for_run(clean_phone)
                                logger.warning(
                                    "DM_FLOOD_STOP | account=%s | FloodWait(%ss) | parked this run",
                                    clean_phone, seconds,
                                )
                                return sent_here, True
                            except AuthKeyDuplicatedError:
                                await self._handle_terminal_account(
                                    0, clean_phone,
                                    "AuthKeyDuplicatedError in dm_worker",
                                    ErrorCategory.AUTH_KEY_DUPLICATED,
                                    remaining_q,
                                )
                                _requeue(target)
                                await _block_for_run(clean_phone)
                                return sent_here, True
                            except asyncio.CancelledError:
                                _requeue(target)
                                raise
                            except Exception as exc:
                                result = classify_exception(exc)
                                if result.is_quarantinable:
                                    await self._handle_terminal_account(
                                        0, clean_phone, result.reason,
                                        result.category, remaining_q,
                                    )
                                    _requeue(target)
                                    await _block_for_run(clean_phone)
                                    return sent_here, True
                                if not result.retryable:
                                    err_name = type(exc).__name__
                                    _note_target_fail(err_name)
                                    logger.warning(
                                        "DM_TARGET_FAIL | account=%s | err=%s | category=%s | %s",
                                        clean_phone, err_name, result.category.value,
                                        result.reason[:80],
                                    )
                                    self._set_worker_state(
                                        0, "TARGET_FAILED",
                                        phone=clean_phone, detail=result.category.value,
                                    )
                                    self._emit(
                                        "send_failure", phone=clean_phone,
                                        detail=f"{result.category.value}: {result.reason[:60]}",
                                    )
                                else:
                                    _requeue(target)
                                    await self._bounded_resource_wait()
                            finally:
                                if not is_restriction_retry:
                                    counter["inflight"] = max(0, counter["inflight"] - 1)
                                    if self._campaign_metrics is not None:
                                        self._campaign_metrics["inflight"] = counter["inflight"]
                        return sent_here, needs_replace
                except SessionAlreadyOwnedError:
                    self._set_worker_state(
                        0, "WAITING_FOR_ACCOUNT",
                        phone=clean_phone, detail="SessionAlreadyOwnedError",
                    )
                    self._handle_contended_lease(clean_phone)
                    await self._bounded_resource_wait()
                    continue
                except asyncio.CancelledError:
                    raise
            return sent_here, needs_replace

        async def run_slot(account_doc: dict) -> int:
            added = 0
            current: Optional[dict] = account_doc
            while current is not None and self.is_running:
                n, needs_replace = await run_one_account(current)
                added += n
                if not needs_replace or not self.is_running:
                    break
                if target_queue.empty():
                    break
                nxt = await _take_replacement()
                if nxt is None or _is_blocked(nxt):
                    break
                logger.info(
                    "DM_ACCOUNT_REPLACE | rested=%s | next=%s | targets_left=%s",
                    str(current.get("phone", "")).strip(),
                    str(nxt.get("phone", "")).strip(),
                    target_queue.qsize(),
                )
                try:
                    await ui_callback(
                        f"🔁 Replaced +{current.get('phone', '')} with "
                        f"+{nxt.get('phone', '')} (24h DM rest)"
                    )
                except Exception:
                    pass
                current = nxt
            return added

        async def run_batch(wave: List[dict], label: str) -> None:
            logger.info(
                "📦 DM Batch %s starting | accounts=%s | targets_left=%s",
                label, len(wave), target_queue.qsize(),
            )
            try:
                await ui_callback(
                    f"📦 DM Batch {label} started | accounts={len(wave)}"
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
                self._active_workers.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
            sent_this = 0
            for result in results:
                if isinstance(result, int):
                    sent_this += result
            logger.info(
                "📦 DM Batch %s sent %s (campaign total=%s)",
                label, sent_this, self.stats["total_sent"],
            )
            self.batch_reports.append({
                "label": label, "sent": sent_this, "accounts": len(wave),
            })
            try:
                await ui_callback(
                    f"📦 DM Batch {label} sent {sent_this} "
                    f"(campaign total={self.stats['total_sent']})"
                )
            except Exception:
                pass

        async def dispatch_batches() -> None:
            nonlocal accounts_in_flight
            while self.is_running:
                live = [t for t in self._batch_tasks if not t.done()]
                self._batch_tasks = live
                if target_queue.empty() and self._batch_index > 0:
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
                await asyncio.sleep(0)

            leftover = [t for t in self._batch_tasks if not t.done()]
            if leftover:
                await asyncio.gather(*leftover, return_exceptions=True)

        async def reporter_loop() -> None:
            self._emit("reporter_start", detail="reporter remains alive while campaign runs")
            while self.is_running:
                try:
                    await ui_callback(await self._generate_live_status())
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                await asyncio.sleep(self._reporter_interval_seconds)

        reporter_task = asyncio.create_task(reporter_loop())
        try:
            await dispatch_batches()
        except asyncio.CancelledError:
            pending = [
                t for t in list(self._active_workers) + list(self._batch_tasks)
                if t is not None and not t.done()
            ]
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if self._campaign_metrics is not None:
                self._campaign_metrics["cancelled"] += counter["inflight"]
        finally:
            self.is_running = False
            reporter_task.cancel()
            try:
                await reporter_task
            except asyncio.CancelledError:
                pass

        leftover_q = target_queue.qsize()
        if self._campaign_metrics is not None:
            self._campaign_metrics["unprocessed"] = leftover_q + counter["inflight"]
        remaining = leftover_q + counter["inflight"]
        if remaining == 0 and not self._campaign_halted:
            final_msg = "✅ **DM CAMPAIGN COMPLETED** ✅\n"
        else:
            final_msg = "⚠️ **DM CAMPAIGN HALTED** ⚠️\n"
            while True:
                try:
                    target_queue.get_nowait()
                    _note_target_fail()
                except asyncio.QueueEmpty:
                    break
        await ui_callback(final_msg + await self._generate_live_status())
        if remaining == 0 and not self._campaign_halted and media_path and os.path.exists(str(media_path)):
            try:
                os.remove(str(media_path))
            except Exception:
                pass
        self.worker_states.clear()
        return final_msg

    # ──────────────────────────────────────────────
    # Single-target processing
    # ──────────────────────────────────────────────

    async def _handle_target(
        self,
        worker_id: int,
        target: Any,
        final_text: Optional[str],
        media_path: str,
        candidate_accounts: list,
        busy_accounts: Dict[str, float],
        rr: Dict[str, int],
        rr_lock: asyncio.Lock,
        target_queue: asyncio.Queue,
        counter: dict,
    ) -> None:
        """Process one target: select account -> acquire -> connect -> send.

        Resource starvation (SessionAlreadyOwnedError / lease=None) is treated
        as WAITING_FOR_ACCOUNT / WAITING_FOR_PROXY and never as a target failure.
        """
        phone: Optional[str] = None
        try:
            account_doc = await self._select_eligible_account(
                candidate_accounts, busy_accounts, rr, rr_lock,
            )
            if account_doc is None:
                if not candidate_accounts or self._all_accounts_long_parked(
                    candidate_accounts, busy_accounts
                ):
                    # No account could ever serve this target -> loud failure
                    # instead of an infinite requeue loop.
                    self._set_worker_state(worker_id, "TARGET_FAILED",
                                           detail="no eligible accounts remaining")
                    self.stats["failed"] += 1
                    if self._campaign_metrics is not None:
                        self._campaign_metrics["failed"] += 1
                    self._emit("send_failure", worker=worker_id,
                               detail="no eligible accounts remaining")
                    return
                self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                       detail="no eligible account available")
                await self._bounded_resource_wait()
                self._requeue_target(target, target_queue, counter)
                return

            phone = str(account_doc.get("phone", "") or "").strip()
            clean_phone = phone.replace("+", "")
            self._set_worker_state(worker_id, "ACCOUNT_SELECTED",
                                   phone=clean_phone, detail=clean_phone)

            try:
                async with self.session_manager.acquire(
                    clean_phone,
                    module="dmsender",
                    worker_id=f"dm:{worker_id}",
                    auto_release=True,
                    timeout=10.0,
                ) as lease:
                    if lease is None:
                        busy_accounts[clean_phone] = (
                            time.monotonic() + self._busy_exclusion_seconds
                        )
                        if await self._proxies_exhausted():
                            self._set_worker_state(worker_id, "WAITING_FOR_PROXY",
                                                   phone=clean_phone, detail="lease=None")
                        else:
                            self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                                   phone=clean_phone, detail="lease=None")
                            self._handle_contended_lease(clean_phone)
                        await self._bounded_resource_wait()
                        self._requeue_target(target, target_queue, counter)
                        return

                    client = lease.client
                    self._set_worker_state(worker_id, "CONNECTING", phone=clean_phone)
                    if not client.is_connected():
                        await client.connect()

                    self._set_worker_state(worker_id, "AUTHORIZED", phone=clean_phone)
                    if not await client.is_user_authorized():
                        raise SessionRevokedError(request=None)

                    self._set_worker_state(worker_id, "PROCESSING", phone=clean_phone)
                    result = await self._process_target(
                        client, target, final_text, media_path,
                    )
                    self._record_result(worker_id, clean_phone, result)
                    # Hold the lease through the human delay so another worker
                    # cannot immediately reuse the same account/IP.
                    await asyncio.sleep(self._human_delay())

            except SessionAlreadyOwnedError:
                busy_accounts[clean_phone] = time.monotonic() + self._busy_exclusion_seconds
                self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                       phone=clean_phone,
                                       detail="SessionAlreadyOwnedError")
                self._handle_contended_lease(clean_phone)
                await self._bounded_resource_wait()
                self._requeue_target(target, target_queue, counter)

            except AuthKeyDuplicatedError:
                # SessionManager owns quarantine + proxy/client cleanup.
                # The same session is NEVER retried.
                self._set_worker_state(worker_id, "TERMINAL_ACCOUNT",
                                       phone=clean_phone, detail="auth_key_duplicated")
                await self._handle_terminal_account(
                    worker_id, clean_phone,
                    "AuthKeyDuplicatedError in dm_worker",
                    ErrorCategory.AUTH_KEY_DUPLICATED,
                    candidate_accounts,
                )
                busy_accounts[clean_phone] = time.monotonic() + 3600.0
                if self._maybe_requeue(worker_id, clean_phone, target):
                    self._requeue_target(target, target_queue, counter)

            except FloodWaitError as exc:
                seconds = int(getattr(exc, "seconds", 30) or 30)
                cap = int(getattr(self, "_flood_delay_cap", 60))
                busy_accounts[clean_phone] = time.monotonic() + max(seconds, 1)
                self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                       phone=clean_phone, detail=f"flood {seconds}s")
                # #region agent log
                _agent_dbg("H5", "dmsender.py:_handle_target", "FloodWait parked account", {
                    "seconds": seconds,
                    "cap": cap,
                    "consume_attempt": seconds <= cap,
                })
                # #endregion
                if seconds <= cap:
                    await asyncio.sleep(max(0, seconds))
                # Short waits were spent on this target; count them. Long waits
                # park the account so other workers can serve the target.
                if self._maybe_requeue(
                    worker_id, clean_phone, target,
                    consume_attempt=(seconds <= cap),
                ):
                    self._requeue_target(target, target_queue, counter)

            except PeerFloodError:
                park_hours = float(CONFIG.get("SPAM_RECHECK_HOURS", 24.0) or 24.0)
                busy_accounts[clean_phone] = time.monotonic() + park_hours * 3600
                await self._park_module_rest(clean_phone, "PeerFloodError", park_hours)
                self._drop_candidate(clean_phone, candidate_accounts)
                # #region agent log
                _agent_dbg("H4", "dmsender.py:_handle_target", "PeerFlood parked spam_until (not dead)", {
                    "park_hours": park_hours,
                    "marked_failed": False,
                    "dropped_candidate": True,
                })
                # #endregion
                self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                       phone=clean_phone, detail="peer_flood")
                if self._maybe_requeue(worker_id, clean_phone, target, consume_attempt=False):
                    self._requeue_target(target, target_queue, counter)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                if UserBannedInChannelError is not None and isinstance(exc, UserBannedInChannelError):
                    busy_accounts[clean_phone] = time.monotonic() + 6 * 3600
                    self._drop_candidate(clean_phone, candidate_accounts)
                    self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                           phone=clean_phone, detail="account_restricted")
                    if self._maybe_requeue(worker_id, clean_phone, target, consume_attempt=False):
                        self._requeue_target(target, target_queue, counter)
                    return
                await self._handle_operation_error(
                    worker_id, clean_phone, target, exc,
                    candidate_accounts, target_queue, counter,
                    busy_accounts,
                )

        except asyncio.CancelledError:
            raise

    def _handle_contended_lease(self, clean_phone: str) -> None:
        """Emit a structured waiting signal for a contended session/account."""
        self._emit("res_wait", phone=clean_phone,
                   detail="resource contention (temporary, not a target failure)")

    async def _process_target(
        self,
        client,
        target: Any,
        final_text: Optional[str],
        media_path: str,
    ) -> str:
        """Resolve the entity and deliver the DM payload. Returns "sent"."""
        entities = list(iter_dm_entities(target))
        if not entities:
            raise ValueError("unresolvable_peer: need access_hash or username")

        send_kwargs = {"link_preview": False, "parse_mode": None}
        last_exc: Optional[BaseException] = None
        for entity in entities:
            # #region agent log
            _agent_dbg("H2", "dmsender.py:_process_target", "entity resolved", {
                "target_is_dict": isinstance(target, dict),
                "entity_kind": type(entity).__name__,
                "used_input_peer": isinstance(entity, InputPeerUser),
                "used_raw_int_id": isinstance(entity, int),
                "used_username": isinstance(entity, str),
            })
            # #endregion
            try:
                if media_path and os.path.exists(str(media_path)):
                    is_voice = str(media_path).lower().endswith((".ogg", ".mp3", ".m4a"))
                    attributes = [DocumentAttributeAudio(voice=True)] if is_voice else None
                    await client.send_file(
                        entity, str(media_path), caption=final_text,
                        voice_note=is_voice, attributes=attributes,
                        parse_mode=None,
                    )
                else:
                    if final_text is None:
                        raise ValueError("Cannot send an empty text message.")
                    await client.send_message(entity, final_text, **send_kwargs)
                return "sent"
            except Exception as exc:
                if type(exc).__name__ not in _PEER_FALLBACK_ERRORS:
                    raise
                last_exc = exc
                logger.info(
                    "DM_PEER_FALLBACK | err=%s | tried=%s",
                    type(exc).__name__,
                    type(entity).__name__,
                )
                continue
        if last_exc is not None:
            raise last_exc
        raise ValueError("unresolvable_peer: need access_hash or username")


def setup_dmsender_handlers(bot: TelegramClient, db, proxy_manager=None,
                           proxy_lease_manager=None, session_manager=None,
                           account_lease_manager=None):
    sender_engine = EnterpriseDMSender(db, proxy_lease_manager, session_manager, account_lease_manager)

    def is_admin(sender_id):
        if ADMIN_ID:
            return str(sender_id) == str(ADMIN_ID)
        return True

    @bot.on(events.NewMessage(pattern='/send_dmsender'))
    async def wizard_start(event):
        if not is_admin(event.sender_id): return
        # Flush stale state data for this user to reclaim RAM
        sender_engine.wizard_state.pop(event.sender_id, None)

        if sender_engine.is_running:
            await event.reply("⚠️ **Engine Occupied:** Campaign background me active hai.")
            return

        try:
            group_stats = await sender_engine.db.get_group_stats()

            if not group_stats:
                msg = "📊 `scraped_data` collection is empty. \n\n👉 Direct single profile target karne ke liye `@username` type karein."
                group_list = []
            else:
                msg = "📊 **Scraped Database Summary:**\n━━━━━━━━━━━━━━━━━━━━━━\n"
                total_users = 0
                group_list = []
                for stat in group_stats:
                    grp = stat["_id"] if stat["_id"] else "Unknown Group"
                    cnt = stat["count"]
                    total_users += cnt
                    group_list.append(str(grp))
                    msg += f"🔹 `{grp}` : **{cnt} users**\n"

                msg += f"━━━━━━━━━━━━━━━━━━━━━━\n✨ **Total Available Users:** `{total_users}`\n\n"
                msg += "👉 **Type the EXACT Group Name** to fetch and send messages.\n"
                msg += "👉 **OR Type specific username/ID** to send an individual message."

            sender_engine.wizard_state[event.sender_id] = {
                "step": "AWAITING_TARGET_SELECTION",
                "available_groups": group_list,
                "targets": [],
                "text": "",
                "media": None,
                "limit": 0
            }
            await event.reply(msg)

        except Exception as e:
            await event.reply(f"❌ **Database Connection Error:** {e}")

    @bot.on(events.NewMessage)
    async def wizard_steps(event):
        if not is_admin(event.sender_id): return
        uid = event.sender_id
        if uid not in sender_engine.wizard_state:
            return

        if event.text and event.text.startswith('/'):
            return

        state = sender_engine.wizard_state[uid]
        step = state["step"]

        if step == "AWAITING_TARGET_SELECTION":
            if not event.text:
                await event.reply("❌ Text message chahiye. Group ka naam ya @username type karein.")
                return
            inp = event.text.strip()
            extracted_targets = []

            if inp in state.get("available_groups", []):
                cursor = await sender_engine.db.get_targets_by_group(inp)
                for doc in cursor:
                    extracted_targets.append({
                        "user_id": doc.get("user_id"),
                        "access_hash": doc.get("access_hash"),
                        "username": doc.get("username"),
                        "phone": doc.get("phone")
                    })

                if not extracted_targets:
                    await event.reply("❌ Is group me valid schema lines nahi mili. Phir se chunein.")
                    return

                ready, skipped_n = partition_dm_targets(extracted_targets)
                state["targets"] = extracted_targets
                state["step"] = "AWAITING_LIMIT"
                await event.reply(
                    f"✅ **{len(extracted_targets)} users extracted.**\n"
                    f"🟢 DM-ready (`@username`): `{len(ready)}`\n"
                    f"⏭️ Hidden username (skip): `{skipped_n}`\n\n"
                    f"Kitne logo ko message bhejna chahte hain? (Number daalein ya `all` likhein):"
                )
            else:
                state["targets"] = [inp]
                state["limit"] = 1
                state["step"] = "AWAITING_TEXT"
                await event.reply(
                    f"🎯 **Targeting individual:** {inp}\n\n"
                    "📝 Apna Promotional Message bhejein jo user ko DM karna hai.\n"
                    "*(Agar sirf media bhejna hai bina text ke, toh reply me `skip` type karein)*"
                )

        elif step == "AWAITING_LIMIT":
            if not event.text:
                await event.reply("❌ Number me type karein ya 'all' likhein.")
                return
            inp = event.text.strip().lower()
            if inp == "all":
                limit = len(state['targets'])
            elif inp.isdigit():
                limit = int(inp)
                if limit <= 0:
                    await event.reply("❌ Valid number daaliye.")
                    return
            else:
                await event.reply("❌ Invalid format. Number me type karein ya 'all' likhein.")
                return

            state["limit"] = limit
            state["step"] = "AWAITING_TEXT"
            await event.reply(
                f"⚙️ **Target Limit Set to:** {limit}\n\n"
                "📝 Ab apna Promotional Message bhejein jo users ko DM karna hai.\n"
                "*(Agar sirf media bhejna hai bina text ke, toh reply me `skip` type karein)*"
            )

        elif step == "AWAITING_TEXT":
            # #region agent log
            _agent_dbg("H3", "dmsender.py:wizard_steps", "AWAITING_TEXT input", {
                "text_is_none": event.text is None,
                "has_media": bool(getattr(event, "media", None)),
            })
            # #endregion
            msg_text = event.text.strip()
            state["text"] = msg_text

            state["step"] = "AWAITING_MEDIA"
            await event.reply(
                "🖼️ **Message Template Cached!**\n\n"
                "Ab media upload karein (Image/Video/Voice Note) ya aage badhne ke liye `skip` type karein:"
            )

        elif step == "AWAITING_MEDIA":
            if event.text and event.text.strip().lower() == "skip":
                state["media"] = None
            elif event.media:
                media_path = await bot.download_media(event.media)
                state["media"] = media_path
            else:
                await event.reply("❌ Media context not found. Re-send or type `skip`.")
                return

            ui_msg = await event.reply("⚡ Deploying DM Cluster Resources... Connecting to Accounts...")

            async def update_ui_status(text_payload):
                try:
                    await ui_msg.edit(text_payload)
                except Exception:
                    pass

            target_list = state["targets"]
            msg_txt = state["text"]
            media_pth = state["media"]
            limit_val = state["limit"]

            sender_engine.wizard_state.pop(uid)

            sender_engine.active_task = asyncio.create_task(
                sender_engine.execute_dm_campaign(target_list, msg_txt, media_pth, limit_val, update_ui_status)
            )

    @bot.on(events.NewMessage(pattern='/dm_status'))
    async def dm_status_check(event):
        if not is_admin(event.sender_id): return
        if not sender_engine.is_running:
            await event.reply("ℹ️ **No DM campaign is currently running.**\n\nUse `/send_dmsender` to start a new campaign.")
            return

        status_msg = await sender_engine._generate_detailed_status()
        await event.reply(status_msg)

    @bot.on(events.NewMessage(pattern='/stop_dmsender'))
    async def wizard_stop(event):
        if not is_admin(event.sender_id): return
        if not sender_engine.is_running:
            await event.reply("ℹ️ Koi DM process running nahi hai.")
            return
        sender_engine.halt_campaign()
        await event.reply("🛑 **Emergency Brake Engaged!** Engine fully stopped.")

    return sender_engine