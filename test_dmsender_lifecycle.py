"""
DM Engine lifecycle + resource scheduling test suite (PATCH 5).

Verifies that the runtime DM sender:
  1. consumes SessionManager exclusively via ``async with acquire(...)``
  2. never constructs TelegramClient / sessions itself
  3. never touches DB runtime locks
  4. treats SessionAlreadyOwnedError / lease=None as RESOURCE conditions
     (WAITING_FOR_ACCOUNT / WAITING_FOR_PROXY), never permanent failures
  5. quarantines AuthKeyDuplicated sessions and never retries the same session
  6. unwinds every acquire context on cancellation
  7. keeps a reporter alive while workers wait (no silent stall)
  8. waits for inflight work before declaring the campaign complete
  9. bounds worker concurrency by proxy/session/account capacity
 10. bounds wizard state with a TTL store that is still dict-compatible
 11. recovers from temporary exhaustive contention without mass target failure
 12. returns every lease to zero after completion / cancellation

Usage: python -m pytest test_dmsender_lifecycle.py -v
"""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dmsender as dmsender_module
from dmsender import EnterpriseDMSender, _WizardStateStore
from session_manager import SessionAlreadyOwnedError

pytest_plugins = ["pytest_asyncio"]


def make_phone(n):
    return f"+1555000{n:04d}"


def account(n, status="active"):
    return {
        "phone": make_phone(n),
        "status": status,
        "session": f"session_{n}",
        "api_id": 12345,
        "api_hash": "abc123",
    }


TARGET = {"user_id": "1001", "access_hash": "0", "username": "alice"}
TEXT = "promo message"


def target(i):
    return {"user_id": str(1000 + i), "access_hash": "0", "username": f"user{i}"}


def make_lease(client, phone="15550000001"):
    return SimpleNamespace(
        phone=phone,
        client=client,
        proxy_url="socks5://proxy-node:1080",
        proxy_id="proxy-1",
        proxy_lease_id="lease-1",
        owner="dmsender",
        worker_id=f"dm:{phone}",
        lease_id=f"L-{phone}",
        released=False,
    )


def dm_client():
    """A client that connects, authorizes, and delivers without media."""
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=False)
    client.connect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=True)
    client.send_message = AsyncMock()
    client.send_file = AsyncMock()
    return client


def dm_client_blocking(started=None, hold=None):
    """Client whose send blocks until `hold` is set, signalling via `started`."""
    client = dm_client()
    async def _send(entity, text, *a, **k):
        if started is not None:
            started.append((entity, text))
        if hold is not None:
            await hold.wait()
        return SimpleNamespace(id=1)
    client.send_message.side_effect = _send
    return client


class FakeDB:
    def __init__(self, accounts=None):
        self.accounts = accounts or []

    async def get_active_target_sessions(self):
        return list(self.accounts)

    async def get_group_stats(self):
        return []

    async def get_targets_by_group(self, name):
        return []


class FakeProxyLeaseManager:
    def __init__(self, count=10):
        self._count = count
        self.calls = []

    async def get_available_count_async(self):
        self.calls.append(("count", self._count))
        return self._count

    def get_available_count(self):
        return self._count

    async def get_stats(self):
        return {
            "available_proxies": self._count,
            "current_active_leases": 0,
            "proxies_in_cooldown": 0,
            "total_acquires": 0,
        }


class ScriptedAcquire:
    """
    A proper @asynccontextmanager mock of SessionManager.acquire.

    The outcome callable returns one of:
      * an Exception instance   -> __aenter__ raises it (no body, no exit)
      * None                    -> __aenter__ yields None (lease=None)
      * a lease object          -> __aenter__ yields the lease

    Records enter/exit and the max number of concurrently-held holds so tests
    can prove the whole worker lifecycle is wrapped by the context manager and
    that concurrency respects capacity.
    """

    def __init__(self, outcome):
        self.outcome = outcome
        self.enter_count = 0
        self.exit_count = 0
        self.current = 0
        self.max_concurrent = 0
        self.args = []

    @asynccontextmanager
    async def __call__(self, *args, **kwargs):
        self.enter_count += 1
        self.args.append((args, kwargs))
        self.current += 1
        self.max_concurrent = max(self.max_concurrent, self.current)
        try:
            result = self.outcome()
        except Exception:
            # outcome() raised directly during __aenter__ -> no body, no exit.
            self.current -= 1
            raise
        if isinstance(result, Exception):
            self.current -= 1  # returned exception -> raised at __aenter__
            raise result
        try:
            yield result
        finally:
            self.exit_count += 1
            self.current -= 1


def build_sender(accounts, recorder, proxy_manager=None, phones_for_contentions=None):
    db = FakeDB(accounts)
    session_manager = MagicMock()
    session_manager.acquire = recorder
    session_manager.mark_quarantined = AsyncMock()
    session_manager.get_stats = AsyncMock(return_value={"active_clients": 0})
    session_manager._max_active_clients = 200
    sender = EnterpriseDMSender(db, proxy_manager, session_manager)
    tune(sender)
    return db, session_manager, sender


def tune(sender):
    """Shrink engine timing for fast, deterministic tests."""
    sender._queue_poll_seconds = 0.01
    sender._resource_wait_bounds = (0.0, 0.01)
    sender._busy_exclusion_seconds = 0.0
    sender._reporter_interval_seconds = 0.05
    sender._human_delay_override = (0.0, 0.001)
    sender._flood_delay_cap = 1
    sender.max_target_attempts = 5


def ok_outcome(phone):
    return lambda: make_lease(dm_client(), phone=phone)


def run_campaign(sender, targets, text=TEXT, limit=0, ui_callback=None):
    ui = ui_callback or AsyncMock()
    return sender.execute_dm_campaign(list(targets), text, "", limit, ui)


def events(sender, name):
    return [e for e in sender.get_lifecycle_events() if e["event"] == name]


async def wait_until(predicate, timeout=10.0, interval=0.01):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _src():
    return Path(dmsender_module.__file__).read_text(encoding="utf-8")


def _src_code():
    """Source with the module docstring stripped (docstring may mention
    forbidden patterns as prose)."""
    src = _src()
    return src.split('"""', 2)[2]


# ---------------------------------------------------------------------------
# Static contract: DM runtime must stay inside SessionManager ownership
# ---------------------------------------------------------------------------

class TestStaticContract:

    def test_acquire_used_exclusively_as_context_manager(self):
        code = _src_code()
        assert "async with self.session_manager.acquire(" in code
        assert "await self.session_manager.acquire(" not in code

    def test_no_direct_client_construction(self):
        code = _src_code()
        # Imports/annotations may reference the TelegramClient type, but the
        # runtime code must never construct one.
        assert "from telethon import TelegramClient" in code
        assert "TelegramClient(" not in code
        assert "_create_client(" not in code
        assert "._create_client(" not in code

    def test_no_db_runtime_locks(self):
        src = _src()
        assert "acquire_lock(" not in src
        assert "release_lock(" not in src
        assert "is_locked(" not in src

    def test_no_manual_proxy_release_in_worker_lifecycle(self):
        src = _src()
        assert "release_proxy(" not in src

    def test_no_time_sleep_in_async_code(self):
        code = _src_code()
        assert "time.sleep(" not in code

    def test_terminal_status_prefilter_defined(self):
        src = _src()
        assert "TERMINAL_STATUSES" in src
        for status in ("revoked", "banned", "deactivated", "invalid",
                       "auth_key_duplicated", "permanently_failed", "quarantined"):
            assert status in src


# ---------------------------------------------------------------------------
# Account eligibility prefilter
# ---------------------------------------------------------------------------

class TestEligibilityPrefilter:

    @pytest.mark.asyncio
    async def test_terminal_accounts_never_reach_session_manager_acquisition(self):
        docs = [
            account(1, status="banned"),
            account(2, status="active"),
            account(3, status="revoked"),
            account(4, status="active"),
        ]
        recorder = ScriptedAcquire(ok_outcome("15550000002"))
        _db, _sm, sender = build_sender(docs, recorder, FakeProxyLeaseManager(10))

        await run_campaign(sender, [target(1)], limit=1)

        # Only the active accounts were ever passed to SessionManager.
        phones = [args[0] for args, _kw in recorder.args]
        assert phones == ["15550000002"]
        assert sender.stats["accounts_used"] == 2
        assert sender.stats["total_sent"] == 1


# ---------------------------------------------------------------------------
# Context manager lifecycle
# ---------------------------------------------------------------------------

class TestContextManagerLifecycle:

    @pytest.mark.asyncio
    async def test_dm_uses_session_manager_context_manager(self):
        db = FakeDB([account(1)])
        recorder = ScriptedAcquire(
            lambda: make_lease(dm_client(), phone="15550000001")
        )
        session_manager = MagicMock()
        session_manager.acquire = recorder
        session_manager._max_active_clients = 200
        session_manager.get_stats = AsyncMock(return_value={"active_clients": 0})
        sender = EnterpriseDMSender(db, FakeProxyLeaseManager(10), session_manager)
        tune(sender)

        await run_campaign(sender, [target(1)], limit=1)

        assert recorder.enter_count == 1
        assert recorder.exit_count == 1
        for args, kwargs in recorder.args:
            assert kwargs["module"] == "dmsender"
            assert kwargs["auto_release"] is True

    @pytest.mark.asyncio
    async def test_worker_cancellation_exits_session_context(self):
        started, hold = [], asyncio.Event()
        client = dm_client_blocking(started, hold)
        recorder = ScriptedAcquire(lambda: make_lease(client, phone="15550000001"))
        db = FakeDB([account(1)])
        session_manager = MagicMock()
        session_manager.acquire = recorder
        session_manager._max_active_clients = 200
        session_manager.get_stats = AsyncMock(return_value={"active_clients": 0})
        sender = EnterpriseDMSender(db, FakeProxyLeaseManager(10), session_manager)
        tune(sender)

        task = asyncio.create_task(
            sender.execute_dm_campaign([target(1)], TEXT, "", 1, AsyncMock())
        )
        sender.active_task = task
        assert await wait_until(lambda: bool(started)), "send never started"

        sender.halt_campaign()
        result = await asyncio.wait_for(task, timeout=10)

        assert isinstance(result, str)
        assert recorder.enter_count == 1
        assert recorder.exit_count == 1  # context unwound on cancellation


# ---------------------------------------------------------------------------
# Resource-starvation is a waiting signal, never a permanent failure
# ---------------------------------------------------------------------------

class TestResourceStarvation:

    @pytest.mark.asyncio
    async def test_session_already_owned_is_waiting_not_failure(self):
        attempts = {"n": 0}

        def outcome():
            attempts["n"] += 1
            if attempts["n"] <= 2:
                raise SessionAlreadyOwnedError("owned by auditor")
            return make_lease(dm_client(), phone="15550000001")

        recorder = ScriptedAcquire(outcome)
        db = FakeDB([account(1)])
        session_manager = MagicMock()
        session_manager.acquire = recorder
        session_manager._max_active_clients = 200
        session_manager.get_stats = AsyncMock(return_value={"active_clients": 0})
        sender = EnterpriseDMSender(db, FakeProxyLeaseManager(10), session_manager)
        tune(sender)

        result = await run_campaign(sender, [target(1)], limit=1)

        stall = [e for e in sender.get_lifecycle_events()
                 if e["event"] in ("res_wait", "WAITING_FOR_ACCOUNT")]
        assert stall, "no WAITING/res_wait signal emitted"
        assert "COMPLETED" in result
        assert sender.stats["total_sent"] == 1
        assert sender.stats["failed"] == 0
        assert sender.stats["accounts_down"] == 0
        assert recorder.enter_count == 3
        assert recorder.exit_count == 1  # only the successful enter had a body

    @pytest.mark.asyncio
    async def test_proxy_exhaustion_is_waiting_not_target_failure(self):
        proxy = FakeProxyLeaseManager(count=0)
        state = {"enters": 0}

        def outcome():
            state["enters"] += 1
            if state["enters"] <= 2:
                return None  # lease=None while proxies exhausted
            proxy._count = 1  # a proxy frees up
            return make_lease(dm_client(), phone="15550000001")

        recorder = ScriptedAcquire(outcome)
        db, _sm, sender = build_sender([account(1)], recorder, proxy)

        result = await run_campaign(sender, [target(1)], limit=1)

        assert events(sender, "WAITING_FOR_PROXY"), "no WAITING_FOR_PROXY signal"
        assert "COMPLETED" in result
        assert sender.stats["total_sent"] == 1
        assert sender.stats["failed"] == 0
        assert sender.stats["accounts_down"] == 0


# ---------------------------------------------------------------------------
# AuthKeyDuplicated terminal handling
# ---------------------------------------------------------------------------

class TestAuthKeyDuplicated:

    @pytest.mark.asyncio
    async def test_auth_key_duplicated_quarantines_and_removes_candidate(self):
        from telethon.errors import AuthKeyDuplicatedError

        attempts = {"n": 0}
        client = dm_client()

        def outcome():
            attempts["n"] += 1
            return make_lease(client, phone="15550000001")

        async def _boom(entity, text, *a, **k):
            if attempts["n"] == 1:
                raise AuthKeyDuplicatedError(request=None)
            return SimpleNamespace(id=1)

        client.send_message.side_effect = _boom
        recorder = ScriptedAcquire(outcome)
        db, session_manager, sender = build_sender([account(1)], recorder, FakeProxyLeaseManager(10))

        result = await run_campaign(sender, [target(1)], limit=1)

        session_manager.mark_quarantined.assert_awaited_once()
        assert "TERMINAL_ACCOUNT" in {
            e["event"] for e in sender.get_lifecycle_events()
        }
        # Same phone never re-enters SessionManager.
        phones = [args[0] for args, _kw in recorder.args]
        assert all(p == "15550000001" for p in phones)
        assert len(phones) == 1
        assert sender.stats["accounts_down"] == 1
        assert sender.stats["failed"] == 1  # target could not be served
        assert "HALTED" in result or "COMPLETED" in result


# ---------------------------------------------------------------------------
# Reporter lifecycle / completion semantics
# ---------------------------------------------------------------------------

class TestReporterAndCompletion:

    @pytest.mark.asyncio
    async def test_reporter_alive_while_workers_wait(self):
        proxy = FakeProxyLeaseManager(count=0)
        recorder = ScriptedAcquire(lambda: None)  # always lease=None
        db, _sm, sender = build_sender([account(1)], recorder, proxy)

        ui = AsyncMock()
        task = asyncio.create_task(
            sender.execute_dm_campaign([target(1)], TEXT, "", 1, ui)
        )
        sender.active_task = task

        assert await wait_until(lambda: events(sender, "WAITING_FOR_PROXY")), \
            "worker never reached WAITING_FOR_PROXY"
        await asyncio.sleep(0.25)  # a few reporter ticks while waiting

        ev = sender.get_lifecycle_events()
        names = [e["event"] for e in ev]
        assert "reporter_start" in names
        # Reporter (driven by is_running) fired at least its start + an update.
        assert ui.call_count >= 2
        # No startup race: worker/capacity event precedes the reporter start.
        assert names.index("worker_started") < names.index("reporter_start")

        sender.halt_campaign()
        await asyncio.wait_for(task, timeout=10)

    @pytest.mark.asyncio
    async def test_completion_waits_for_inflight_work(self):
        started, hold = [], asyncio.Event()
        client = dm_client_blocking(started, hold)
        recorder = ScriptedAcquire(lambda: make_lease(client, phone="15550000001"))
        db = FakeDB([account(1)])
        session_manager = MagicMock()
        session_manager.acquire = recorder
        session_manager._max_active_clients = 200
        session_manager.get_stats = AsyncMock(return_value={"active_clients": 0})
        sender = EnterpriseDMSender(db, FakeProxyLeaseManager(10), session_manager)
        tune(sender)

        task = asyncio.create_task(
            sender.execute_dm_campaign([target(1)], TEXT, "", 1, AsyncMock())
        )
        sender.active_task = task
        assert await wait_until(lambda: bool(started)), "send never started"

        # Queue is now empty, one target in flight -> campaign must NOT end.
        assert sender._campaign_metrics["queued"] == 0
        assert sender._campaign_metrics["inflight"] == 1
        assert not task.done()
        assert sender.is_running is True

        hold.set()
        result = await asyncio.wait_for(task, timeout=10)

        assert "COMPLETED" in result
        assert sender.stats["total_sent"] == 1
        assert sender._campaign_metrics["inflight"] == 0


# ---------------------------------------------------------------------------
# Campaign control: stop waits for cleanup
# ---------------------------------------------------------------------------

class TestCampaignControl:

    @pytest.mark.asyncio
    async def test_stop_cancels_workers_and_unwinds_all_contexts(self):
        started, hold = [], asyncio.Event()
        client = dm_client_blocking(started, hold)
        recorder = ScriptedAcquire(lambda: make_lease(client, phone="15550000001"))
        db, _sm, sender = build_sender([account(1)], recorder, FakeProxyLeaseManager(10))

        task = asyncio.create_task(
            sender.execute_dm_campaign([target(1)], TEXT, "", 1, AsyncMock())
        )
        sender.active_task = task
        assert await wait_until(lambda: bool(started)), "send never started"
        assert recorder.enter_count == 1
        assert recorder.current == 1  # one context body live

        sender.halt_campaign()
        result = await asyncio.wait_for(task, timeout=10)

        assert isinstance(result, str)
        assert "HALTED" in result
        assert recorder.exit_count == 1
        assert recorder.current == 0
        assert sender.is_running is False
        assert not sender._active_workers or all(t.done() for t in sender._active_workers)


# ---------------------------------------------------------------------------
# Worker capacity bounds
# ---------------------------------------------------------------------------

class TestWorkerCapacity:

    @pytest.mark.asyncio
    async def test_14_accounts_10_proxy_capacity_bounds_workers(self):
        accounts = [account(i) for i in range(1, 15)]
        targets = [target(i) for i in range(20)]
        phones = {a["phone"].replace("+", "") for a in accounts}

        def outcome():
            # Any account may be picked; every lease is a valid fresh client.
            return make_lease(dm_client(), phone=sorted(phones)[0])

        recorder = ScriptedAcquire(outcome)
        db, _sm, sender = build_sender(accounts, recorder, FakeProxyLeaseManager(10))

        result = await run_campaign(sender, targets)

        assert sender._campaign_metrics["effective_worker_capacity"] == 10
        assert recorder.max_concurrent <= 10
        assert recorder.enter_count == 20
        assert recorder.exit_count == 20
        assert sender.stats["total_sent"] == 20
        assert sender.stats["failed"] == 0
        assert "COMPLETED" in result

    @pytest.mark.asyncio
    async def test_160_accounts_10_proxy_capacity_does_not_create_160_clients(self):
        accounts = [account(i) for i in range(1, 161)]
        targets = [target(i) for i in range(160)]
        phones = {a["phone"].replace("+", "") for a in accounts}

        def outcome():
            return make_lease(dm_client(), phone=sorted(phones)[0])

        recorder = ScriptedAcquire(outcome)
        db, _sm, sender = build_sender(accounts, recorder, FakeProxyLeaseManager(10))

        result = await run_campaign(sender, targets)

        assert sender._campaign_metrics["effective_worker_capacity"] == 10
        assert recorder.max_concurrent <= 10
        assert recorder.enter_count == 160
        assert recorder.exit_count == 160
        assert sender.stats["total_sent"] == 160
        assert sender.stats["failed"] == 0
        assert "COMPLETED" in result


# ---------------------------------------------------------------------------
# Wizard state store: bounded + TTL, still dict-compatible
# ---------------------------------------------------------------------------

class TestWizardState:

    def test_wizard_state_bounded_and_ttl_managed(self):
        class FakeClock:
            def __init__(self):
                self.t = 0.0

            def __call__(self):
                return self.t

        clock = FakeClock()
        sender = EnterpriseDMSender(FakeDB())
        store = _WizardStateStore(max_items=5, ttl_seconds=10.0)
        with patch.object(dmsender_module.time, "monotonic", clock):
            for i in range(20):
                store[i] = f"value-{i}"

            assert len(store) == 5
            assert 19 in store
            assert 14 not in store  # oldest entries evicted

            # dict compatibility
            store["admin"] = {"step": "AWAITING_TEXT"}
            assert sender.wizard_state.__class__ is not None
            assert store["admin"]["step"] == "AWAITING_TEXT"
            assert store.pop("admin", None) == {"step": "AWAITING_TEXT"}
            assert "admin" not in store

            # TTL expiry: an entry vanishes on access after it ages out.
            store.clear()
            store["fresh"] = "v"
            clock.t = 1000.0
            assert "fresh" not in store
            assert store.get("fresh", "default") == "default"
            store.set("after", 1)  # triggers cleanup of expired entries
            assert "fresh" not in store
            assert store["after"] == 1

    def test_wizard_state_is_dict_compatible_for_main_bot(self):
        sender = EnterpriseDMSender(FakeDB())
        sender.wizard_state[12345] = {"step": "AWAITING_TARGET_SELECTION"}
        assert 12345 in sender.wizard_state
        assert sender.wizard_state[12345]["step"] == "AWAITING_TARGET_SELECTION"
        assert sender.wizard_state.pop(12345, None)["step"] == "AWAITING_TARGET_SELECTION"


# ---------------------------------------------------------------------------
# PATCH 5 regression: temporary exhaustive contention then recovery
# ---------------------------------------------------------------------------

class TestRegressionContentionRecovery:

    @pytest.mark.asyncio
    async def test_20_targets_14_accounts_10_proxies_contention_recovers(self):
        accounts = [account(i) for i in range(1, 15)]
        targets = [target(i) for i in range(20)]
        proxy = FakeProxyLeaseManager(count=10)
        phones = [a["phone"].replace("+", "") for a in accounts]
        state = {"enters": 0}

        def outcome():
            state["enters"] += 1
            if state["enters"] <= 8:
                # Simulate heavy contention: other modules own the sessions.
                raise SessionAlreadyOwnedError("owned by another worker")
            if proxy._count <= 0:
                # Proxy pool temporarily exhausted -> lease=None.
                return None
            phone = phones[(state["enters"] - 1) % len(phones)]
            return make_lease(dm_client(), phone=phone)

        recorder = ScriptedAcquire(outcome)
        db, _sm, sender = build_sender(accounts, recorder, proxy)

        ui = AsyncMock()
        task = asyncio.create_task(
            sender.execute_dm_campaign(targets, TEXT, "", 20, ui)
        )
        sender.active_task = task

        # Workers start; contention shows up as WAITING_FOR_ACCOUNT.
        assert await wait_until(lambda: events(sender, "WAITING_FOR_ACCOUNT")), \
            "no WAITING_FOR_ACCOUNT during contention"
        assert sender._campaign_metrics["effective_worker_capacity"] == 10

        # Drive the proxy pool to 0 temporarily -> WAITING_FOR_PROXY.
        proxy._count = 0
        assert await wait_until(lambda: events(sender, "WAITING_FOR_PROXY")), \
            "no WAITING_FOR_PROXY during proxy exhaustion"

        # Reporter stays alive the whole time (no silent stall).
        assert events(sender, "reporter_start")

        # Release one resource; the engine must resume and finish.
        proxy._count = 1
        result = await asyncio.wait_for(task, timeout=15)

        assert "COMPLETED" in result
        assert sender.stats["total_sent"] == 20
        assert sender.stats["failed"] == 0, "resource starvation must not fail targets"
        assert sender.stats["accounts_down"] == 0
        assert not events(sender, "TARGET_FAILED")
        # Every context body that opened was unwound (raises at __aenter__
        # never open a body, so current == 0 is the leak-free invariant).
        assert recorder.current == 0


# ---------------------------------------------------------------------------
# Resource cleanup after completion / cancellation
# ---------------------------------------------------------------------------

class TestResourceCleanup:

    @pytest.mark.asyncio
    async def test_all_leases_returned_after_completion(self):
        accounts = [account(i) for i in range(1, 6)]
        targets = [target(i) for i in range(5)]
        proxy = FakeProxyLeaseManager(count=5)
        phones = [a["phone"].replace("+", "") for a in accounts]

        def outcome():
            phone = phones[len(recorder.args) % len(phones)]
            return make_lease(dm_client(), phone=phone)

        recorder = ScriptedAcquire(outcome)
        db, session_manager, sender = build_sender(accounts, recorder, proxy)

        result = await run_campaign(sender, targets)

        assert "COMPLETED" in result
        assert recorder.enter_count == recorder.exit_count == 5
        assert recorder.current == 0
        assert sender._campaign_metrics["inflight"] == 0
        assert sender.is_running is False
        assert sender._active_workers and all(t.done() for t in sender._active_workers)
        # SessionManager was asked to create no extra capacity.
        assert session_manager.get_stats.await_count == 1

    @pytest.mark.asyncio
    async def test_all_leases_returned_after_cancellation(self):
        started, hold = [], asyncio.Event()
        client = dm_client_blocking(started, hold)
        recorder = ScriptedAcquire(lambda: make_lease(client, phone="15550000001"))
        db, _sm, sender = build_sender([account(1)], recorder, FakeProxyLeaseManager(2))

        task = asyncio.create_task(
            sender.execute_dm_campaign([target(1), target(2)], TEXT, "", 2, AsyncMock())
        )
        sender.active_task = task
        assert await wait_until(lambda: bool(started)), "send never started"

        sender.halt_campaign()
        await asyncio.wait_for(task, timeout=10)

        assert recorder.current == 0
        assert recorder.exit_count <= recorder.enter_count
        assert recorder.enter_count >= 1
        assert sender.is_running is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])