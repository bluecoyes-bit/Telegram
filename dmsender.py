#!/usr/bin/env python3

import os
import time
import asyncio
import logging
import random
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeAudio, InputPeerUser
from telethon.errors import (
    FloodWaitError, SessionRevokedError, AuthKeyDuplicatedError,
)

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
from exception_classifier import ErrorCategory, classify_exception

logger = logging.getLogger("DMSenderEngine")

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
            "accounts_used": 0,
            "accounts_down": 0,
            "total_targets": 0
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
        count = 1
        if self.proxy_lease_manager is not None:
            try:
                count = max(1, int(self.proxy_lease_manager.get_available_count()))
            except Exception:
                count = 1
        base = max(0.5, 45.0 / count)
        return random.uniform(base, base + 1.0)

    # ──────────────────────────────────────────────
    # Account eligibility
    # ──────────────────────────────────────────────

    def _filter_eligible_accounts(self, account_docs: list) -> list:
        eligible = []
        for doc in account_docs or []:
            status = str(doc.get("status", "") or "").lower()
            if status in TERMINAL_STATUSES:
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

    # ──────────────────────────────────────────────
    # Target helpers
    # ──────────────────────────────────────────────

    def _requeue_target(self, target: Any, target_queue: asyncio.Queue, counter: dict) -> None:
        target_queue.put_nowait(target)
        counter["queued"] += 1
        if self._campaign_metrics is not None:
            self._campaign_metrics["queued"] = counter["queued"]

    def _maybe_requeue(self, worker_id: int, phone: str, target: Any) -> bool:
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
        if result.is_quarantinable:
            self._set_worker_state(worker_id, "TERMINAL_ACCOUNT",
                                   phone=phone, detail=category.value)
            await self._handle_terminal_account(
                worker_id, phone or "", result.reason, category, candidate_accounts,
            )
            if self._maybe_requeue(worker_id, phone or "", target):
                self._requeue_target(target, target_queue, counter)
            return
        if not result.retryable:
            # Target-level permanent error (privacy/blocked/invalid target, ...).
            self._set_worker_state(worker_id, "TARGET_FAILED",
                                   phone=phone, detail=category.value)
            self.stats["failed"] += 1
            if self._campaign_metrics is not None:
                self._campaign_metrics["failed"] += 1
            self._emit("send_failure", worker=worker_id, phone=phone,
                       detail=f"{category.value}: {result.reason[:60]}")
            return
        # Transient Telegram/network error: bounded retry keeps the failure visible.
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

        remaining = max(0, self.stats['total_targets'] - self.stats['total_sent'] - self.stats['failed'])
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
            f"🎯 Targets: `{self.stats['total_targets']}`\n"
            f"   Progress: `{round((self.stats['total_sent'] + self.stats['failed']) / max(1, self.stats['total_targets']) * 100, 1)}%`\n"
            f"⚡ Rate: `{rate} msgs/min` | Runtime: `{elapsed // 60}m {elapsed % 60}s` | ETA: `{eta_str}`\n"
            f"👷 Active Workers: `{active_workers}` | Waiting: `{waiting}` | Processing: `{processing}`\n"
            f"👥 Accounts: used=`{self.stats['accounts_used']}` down=`{self.stats['accounts_down']}`\n"
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

        remaining = max(0, self.stats['total_targets'] - self.stats['total_sent'] - self.stats['failed'])
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
            f"   🎯 Total Targets: `{self.stats['total_targets']}`\n"
            f"   📈 Progress: `{round((self.stats['total_sent'] + self.stats['failed']) / max(1, self.stats['total_targets']) * 100, 1)}%`\n"
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
            "accounts_used": 0,
            "accounts_down": 0,
            "total_targets": 0
        }

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

    # ──────────────────────────────────────────────
    # Core engine
    # ──────────────────────────────────────────────

    async def _dynamic_rolling_worker(
        self, target_list: list, final_text: Optional[str], media_path: str,
        limit: int, ui_callback, candidate_accounts: list
    ):
        targets = list(target_list[:limit] if limit > 0 else target_list)
        candidate_accounts = self._filter_eligible_accounts(candidate_accounts)
        self.stats["total_targets"] = len(targets)
        self.stats["accounts_used"] = len(candidate_accounts)
        self._emit("target_queue_created",
                   detail=f"targets={len(targets)}, accounts={len(candidate_accounts)}")

        await ui_callback(f"🚀 **DM Engine Started (Dynamic Rolling Batch)!**\n"
                          f"Targets: `{len(targets)}`, Accounts: `{len(candidate_accounts)}`\n"
                          f"Concurrency dictated by available proxies.")

        # Campaign resource snapshot (PATCH 5).
        self._campaign_metrics = {
            "eligible_accounts": len(candidate_accounts),
            "available_proxies": 0,
            "available_session_capacity": 0,
            "effective_worker_capacity": 0,
            "target_count": len(targets),
            "queued": len(targets),
            "inflight": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "cancelled": 0,
            "unprocessed": 0,
        }

        target_queue = asyncio.Queue()
        for t in targets:
            target_queue.put_nowait(t)

        counter = {"queued": len(targets), "inflight": 0}
        busy_accounts: Dict[str, float] = {}
        active_workers: List[asyncio.Task] = []
        self._active_workers = active_workers

        _rr = {"idx": 0}
        _rr_lock = asyncio.Lock()

        try:
            _configured_limit = int(CONFIG.get("DM_MAX_WORKERS", 20))
        except (TypeError, ValueError):
            _configured_limit = 20
        try:
            _available_proxies = await self._get_available_proxies()
        except Exception:
            _available_proxies = 0
        try:
            _session_capacity = await self._get_session_capacity(_configured_limit)
        except Exception:
            _session_capacity = _configured_limit

        num_workers = (
            compute_dm_worker_capacity(
                num_accounts=len(candidate_accounts),
                available_proxies=_available_proxies,
                session_capacity=_session_capacity,
                configured_limit=_configured_limit,
            )
            if candidate_accounts else 0
        )
        self._campaign_metrics.update({
            "available_proxies": _available_proxies,
            "available_session_capacity": _session_capacity,
            "effective_worker_capacity": num_workers,
        })
        self._emit("worker_started",
                   detail=f"effective_capacity={num_workers} "
                          f"(accounts={len(candidate_accounts)}, proxies={_available_proxies}, "
                          f"session_cap={_session_capacity}, limit={_configured_limit})")

        async def dm_worker(worker_id: int) -> None:
            self._set_worker_state(worker_id, "STARTED")
            try:
                while True:
                    # Stop path: let SessionManager contexts unwind; no new work.
                    if not self.is_running and counter["inflight"] == 0:
                        break
                    try:
                        target = await asyncio.wait_for(
                            target_queue.get(),
                            timeout=self._queue_poll_seconds,
                        )
                    except asyncio.TimeoutError:
                        # Queue is temporarily empty. Only exit when nothing is
                        # queued AND nothing is in flight (no silent stall).
                        if counter["queued"] == 0 and counter["inflight"] == 0:
                            break
                        continue
                    if target is None:
                        break

                    counter["queued"] = max(0, counter["queued"] - 1)
                    counter["inflight"] += 1
                    if self._campaign_metrics is not None:
                        self._campaign_metrics["queued"] = counter["queued"]
                        self._campaign_metrics["inflight"] = counter["inflight"]
                    try:
                        await self._handle_target(
                            worker_id, target, final_text, media_path,
                            candidate_accounts, busy_accounts, _rr, _rr_lock,
                            target_queue, counter,
                        )
                    finally:
                        counter["inflight"] = max(0, counter["inflight"] - 1)
                        if self._campaign_metrics is not None:
                            self._campaign_metrics["inflight"] = counter["inflight"]
            except asyncio.CancelledError:
                raise
            finally:
                self.worker_states.pop(worker_id, None)
                self._emit("worker_exit", worker=worker_id)

        async def reporter_loop() -> None:
            """Reporter stays alive for the whole campaign, even while every
            worker is temporarily waiting for a resource."""
            self._emit("reporter_start", detail="reporter remains alive while campaign runs")
            while self.is_running:
                try:
                    await ui_callback(await self._generate_live_status())
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                await asyncio.sleep(self._reporter_interval_seconds)

        # PATCH 5: workers are created BEFORE the reporter starts (no race), and
        # the reporter is driven by self.is_running, never by bool(workers).
        for i in range(num_workers):
            task = asyncio.create_task(dm_worker(i))
            active_workers.append(task)
        reporter_task = asyncio.create_task(reporter_loop())

        try:
            await asyncio.gather(*active_workers)
        except asyncio.CancelledError:
            if self._campaign_metrics is not None:
                self._campaign_metrics["cancelled"] += counter["inflight"]
            pass
        finally:
            self.is_running = False
            reporter_task.cancel()
            try:
                await reporter_task
            except asyncio.CancelledError:
                pass

        if self._campaign_metrics is not None:
            self._campaign_metrics["unprocessed"] = counter["queued"] + counter["inflight"]

        remaining = counter["queued"] + counter["inflight"]
        if remaining == 0 and not self._campaign_halted:
            final_msg = "✅ **DM CAMPAIGN COMPLETED** ✅\n"
        else:
            final_msg = "⚠️ **DM CAMPAIGN HALTED** ⚠️\n"
        await ui_callback(final_msg + await self._generate_live_status())

        # Only clean up the media file when the campaign finished cleanly —
        # a halted campaign may still need it on resume.
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
                if not candidate_accounts:
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
                delay = min(max(seconds, 1), int(getattr(self, "_flood_delay_cap", 60)))
                busy_accounts[clean_phone] = time.monotonic() + delay
                self._set_worker_state(worker_id, "WAITING_FOR_ACCOUNT",
                                       phone=clean_phone, detail=f"flood {seconds}s")
                # SessionManager already released client+proxy on context exit.
                await asyncio.sleep(max(0, delay))
                if self._maybe_requeue(worker_id, clean_phone, target):
                    self._requeue_target(target, target_queue, counter)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                await self._handle_operation_error(
                    worker_id, clean_phone, target, exc,
                    candidate_accounts, target_queue, counter,
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
        entity = None
        if isinstance(target, dict):
            user_id = target.get("user_id")
            access_hash = target.get("access_hash")
            username = target.get("username")

            # Prefer the pre-resolved InputPeerUser: no extra API call, less
            # flood exposure. Username is the fallback.
            if user_id and access_hash and str(access_hash) != "0":
                try:
                    entity = InputPeerUser(int(user_id), int(access_hash))
                except Exception:
                    entity = None
            if not entity and username and str(username).strip() and str(username).lower() != "none":
                u_str = str(username).strip()
                entity = u_str if u_str.startswith("@") else f"@{u_str}"
            if not entity and user_id:
                entity = int(user_id)
        else:
            target_str = str(target).strip()
            if target_str.isdigit():
                entity = int(target_str)
            else:
                entity = target_str if target_str.startswith("@") else f"@{target_str}"

        if not entity:
            raise ValueError("Could not construct entity tokens.")

        if media_path and os.path.exists(str(media_path)):
            is_voice = str(media_path).lower().endswith((".ogg", ".mp3", ".m4a"))
            attributes = [DocumentAttributeAudio(voice=True)] if is_voice else None
            await client.send_file(
                entity, str(media_path), caption=final_text,
                voice_note=is_voice, attributes=attributes,
            )
        else:
            if final_text is None:
                raise ValueError("Cannot send an empty text message.")
            await client.send_message(entity, final_text)
        return "sent"


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

                state["targets"] = extracted_targets
                state["step"] = "AWAITING_LIMIT"
                await event.reply(
                    f"✅ **{len(state['targets'])} Users extracted mapping metadata structural array successfully!**\n\n"
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