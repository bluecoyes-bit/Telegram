"""
P0-CLOSEOUT Test Suite — proxy matrix, module collisions, cancellation,
terminal zero-creation, shutdown, and resource invariants.

Builds on test_p0_lifecycle.py infrastructure but uses enhanced configurable
fakes so we can exercise proxy exhaustion/scaling and per-phase cancellation.

Run: python -m pytest test_p0_closeout.py -v
"""
import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

pytest_plugins = ["pytest_asyncio"]

# ---------------------------------------------------------------------------
# Reusable fake DB
# ---------------------------------------------------------------------------

class FakeDB:
    def __init__(self):
        self.sessions = {}
        self.statuses = {}

    def _norm(self, phone):
        return str(phone).strip().replace(" ", "").replace("+", "")

    def get_session_by_phone(self, phone):
        return self.sessions.get(self._norm(phone))

    async def get_session_by_phone_async(self, phone):
        return self.sessions.get(self._norm(phone))

    def update_session_status(self, phone, status, reason=None):
        self.statuses[self._norm(phone)] = status

    def mark_account_failed(self, phone, reason=""):
        self.statuses[self._norm(phone)] = "permanently_failed"

    def mark_account_revoked(self, phone, reason=""):
        self.statuses[self._norm(phone)] = "revoked"

    def get_all_active_sessions(self):
        return [doc for doc in self.sessions.values() if doc.get("status") == "active"]


class FakeProxyManager:
    def __init__(self):
        self.working_count = 5

    def get_proxy(self):
        return None


class ConfigurableProxyLeaseManager:
    """Proxy pool with a fixed size; returns None when exhausted (no blocking)."""

    def __init__(self, pool_size, block_on_exhausted=False):
        self.pool_size = pool_size
        self.block_on_exhausted = block_on_exhausted
        self._leased = {}        # phone -> proxy dict
        self._counter = 0
        self._total_acquires = 0
        self._double_release_log = []
        self._release_count = 0

    def get_available_count(self):
        return max(0, self.pool_size - len(self._leased))

    async def acquire_proxy(self, phone, timeout=None):
        if not self.block_on_exhausted and len(self._leased) >= self.pool_size:
            return None
        if self.block_on_exhausted and len(self._leased) >= self.pool_size:
            await asyncio.sleep(999)  # block until cancelled -> tests cancellation
        self._counter += 1
        self._total_acquires += 1
        proxy = {
            "url": f"socks5://proxy{self._counter}:1080",
            "addr": f"proxy{self._counter}",
            "port": 1080,
            "username": "u",
            "password": "p",
        }
        self._leased[phone] = proxy
        return proxy

    async def release_proxy(
        self,
        *,
        proxy_url,
        phone,
        proxy_id=None,
        lease_id=None,
        should_cooldown=False,
        cooldown_reason="",
    ):
        self._release_count += 1
        cur = self._leased.get(phone)
        if cur is None:
            self._double_release_log.append((phone, proxy_url))
            return
        self._leased.pop(phone, None)

    def get_stats(self):
        return {
            "available_proxies": self.get_available_count(),
            "current_active_leases": len(self._leased),
            "proxies_in_cooldown": 0,
            "total_acquires": self._total_acquires,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_phone(n):
    return f"+1555000{n:05d}"


def _make_session_doc(phone, **overrides):
    normalized = str(phone).strip().replace("+", "")
    doc = {
        "phone": normalized,
        "status": "active",
        "session_string": f"session_{normalized}",
        "api_id": 12345,
        "api_hash": "abc123",
        "device_metadata": {
            "device_model": "PC 64bit",
            "system_version": "Windows 11",
            "app_version": "5.1.0",
        },
        **overrides,
    }
    return doc


def _store_session(db, phone, doc):
    normalized = str(phone).strip().replace("+", "")
    db.sessions[normalized] = doc


def _mock_client(phone="test", connected=False):
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=connected)
    client.is_user_authorized.return_value = True
    client.session = MagicMock()
    client.session.save.return_value = f"session_str_{phone}"
    client.disconnect = AsyncMock()
    return client


def _build_sm(pool_size=100, block_on_exhausted=False, db=None):
    from session_manager import SessionManager
    _db = db or FakeDB()
    pm = FakeProxyManager()
    plm = ConfigurableProxyLeaseManager(pool_size, block_on_exhausted=block_on_exhausted)
    sm = SessionManager(_db, pm, plm)
    # count client creations; return a healthy connected client by default
    create_counter = {"n": 0}
    def _fake_create(**kwargs):
        create_counter["n"] += 1
        return _mock_client(kwargs.get("session_str", "test"), connected=True)
    sm._create_client = _fake_create
    return sm, _db, plm, create_counter


def _resource_counts(sm, plm):
    """Snapshot of key resource counters for invariant checks."""
    return {
        "active_clients": sm._active_count,
        "active_session_leases": len([i for i in sm._sessions.values()
                                      if i.lifecycle.value in ("busy", "reserved")]),
        "active_account_leases": 0,  # SessionManager holds session ownership
        "active_proxy_leases": len(plm._leased),
    }


async def _assert_clean_resources(sm, plm, tasks=()):
    """After graceful teardown every resource must return to zero.

    SessionManager intentionally keeps idle connected clients alive for reuse
    (TTL 600s), so we first cancel any held workers and call disconnect_all()
    — mirroring a graceful shutdown — then assert all counters are zero.
    """
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    await sm.disconnect_all()
    await asyncio.sleep(0.05)
    counts = _resource_counts(sm, plm)
    assert counts["active_clients"] == 0, f"active_clients != 0: {counts}"
    assert counts["active_session_leases"] == 0, f"session leases != 0: {counts}"
    assert counts["active_proxy_leases"] == 0, f"proxy leases != 0: {counts}"


# ---------------------------------------------------------------------------
# D) Proxy matrix
# ---------------------------------------------------------------------------

class TestProxyMatrix:

    @pytest.mark.asyncio
    async def test_1_proxy_14_accounts_serial(self):
        # 1 proxy, 14 accounts: only one account holds a lease at a time,
        # others yield None. After release, a new account can acquire.
        sm, db, plm, cc = _build_sm(pool_size=1)
        phones = [_make_phone(i) for i in range(14)]
        for p in phones:
            _store_session(db, p, _make_session_doc(p))

        # Acquire 14 concurrently: only 1 should get a lease at any instant
        results = {}

        async def grab(i):
            p = phones[i]
            got = False
            async with sm.acquire(p, module=f"m{i}", auto_release=True) as lease:
                if lease is not None:
                    got = True
            results[p] = got

        await asyncio.gather(*[grab(i) for i in range(14)])

        # With serial proxy use, at least one should succeed
        assert any(results.values()), "no account got the single proxy"

        # Invariant: first lease acquired then released; nothing leaks
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_10_proxies_14_accounts(self):
        sm, db, plm, cc = _build_sm(pool_size=10)
        phones = [_make_phone(i) for i in range(14)]
        for p in phones:
            _store_session(db, p, _make_session_doc(p))

        got = [False] * 14
        async def grab(i):
            p = phones[i]
            async with sm.acquire(p, module=f"m{i}", auto_release=True) as lease:
                if lease is not None:
                    got[i] = True

        await asyncio.gather(*[grab(i) for i in range(14)])
        # 10 proxies -> 10 should succeed concurrently
        assert sum(got) >= 10, f"expected >=10 success, got {sum(got)}"
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_10_proxies_160_accounts(self):
        sm, db, plm, cc = _build_sm(pool_size=10)
        phones = [_make_phone(i) for i in range(160)]
        for p in phones:
            _store_session(db, p, _make_session_doc(p))

        got = [False] * 160
        async def grab(i):
            async with sm.acquire(phones[i], module=f"m{i}", auto_release=True) as lease:
                if lease is not None:
                    got[i] = True

        await asyncio.gather(*[grab(i) for i in range(160)])
        assert sum(got) >= 10, f"expected >=10 success, got {sum(got)}"
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_100_proxies_160_accounts(self):
        sm, db, plm, cc = _build_sm(pool_size=100)
        phones = [_make_phone(i) for i in range(160)]
        for p in phones:
            _store_session(db, p, _make_session_doc(p))

        got = [False] * 160
        async def grab(i):
            async with sm.acquire(phones[i], module=f"m{i}", auto_release=True) as lease:
                if lease is not None:
                    got[i] = True

        await asyncio.gather(*[grab(i) for i in range(160)])
        assert sum(got) >= 100, f"expected >=100 success, got {sum(got)}"
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_all_proxies_busy_returns_none(self):
        # Pool fully leased by other holders -> new acquire yields None, no client
        sm, db, plm, cc = _build_sm(pool_size=10)
        # Lease 10 accounts (fills the pool)
        phones = [_make_phone(i) for i in range(10)]
        for p in phones:
            _store_session(db, p, _make_session_doc(p))

        held = []
        async def hold(i):
            async with sm.acquire(phones[i], module=f"m{i}", auto_release=False) as lease:
                if lease is not None:
                    held.append(lease)
                await asyncio.sleep(5)

        tasks = [asyncio.create_task(hold(i)) for i in range(10)]
        await asyncio.sleep(0.3)

        # 11th account: no proxy available -> lease None, zero client creation
        p11 = _make_phone(100)
        _store_session(db, p11, _make_session_doc(p11))
        before = cc["n"]
        async with sm.acquire(p11, module="m11", auto_release=True) as lease:
            assert lease is None
            assert cc["n"] == before, "no client should be created when proxy is exhausted"
            assert _resource_counts(sm, plm)["active_proxy_leases"] == 10

        # graceful teardown: cancel holders, disconnect all, assert zero
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await sm.disconnect_all()
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_release_frees_proxy_for_next_acquire(self):
        sm, db, plm, cc = _build_sm(pool_size=1)
        p1 = _make_phone(1)
        p2 = _make_phone(2)
        _store_session(db, p1, _make_session_doc(p1))
        _store_session(db, p2, _make_session_doc(p2))

        key1 = sm._session_key(p1)
        # Hold p1 on the only proxy
        async with sm.acquire(p1, module="m1", auto_release=False) as lease1:
            assert lease1 is not None
            # p2 blocked (no proxy)
            async with sm.acquire(p2, module="m2", auto_release=True) as l2:
                assert l2 is None
            # release p1 -> proxy freed
            await sm._release_lease(key1, lease1.owner)

        # now p2 can acquire
        async with sm.acquire(p2, module="m2", auto_release=False) as lease2:
            assert lease2 is not None
            key2 = sm._session_key(p2)
            await sm._release_lease(key2, lease2.owner)
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_double_release_of_proxy_is_harmless(self):
        # VoiceCluster/other modules releasing a proxy that's already free
        sm, db, plm, cc = _build_sm(pool_size=5)
        p1 = _make_phone(1)
        _store_session(db, p1, _make_session_doc(p1))
        async with sm.acquire(p1, module="m1", auto_release=False) as lease:
            key1 = sm._session_key(p1)
            await sm._release_lease(key1, lease.owner)
            # second release same owner
            await sm._release_lease(key1, lease.owner)
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_owner_mismatch_proxy_release_does_not_pop(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        p1 = _make_phone(1)
        _store_session(db, p1, _make_session_doc(p1))
        async with sm.acquire(p1, module="m1", auto_release=False) as lease:
            # a DIFFERENT phone tries to release p1's proxy
            await plm.release_proxy(proxy_url=None, phone=_make_phone(999))
            await sm._release_lease(sm._session_key(p1), lease.owner)
        await _assert_clean_resources(sm, plm)


# ---------------------------------------------------------------------------
# C) Terminal accounts -> zero client creation, zero proxy
# ---------------------------------------------------------------------------

class TestTerminalZeroCreation:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("terminal_status", [
        "revoked", "banned", "deactivated", "invalid",
        "auth_key_duplicated", "permanently_failed", "quarantined",
    ])
    async def test_terminal_no_client_no_proxy(self, terminal_status):
        sm, db, plm, cc = _build_sm(pool_size=10)
        p = _make_phone(1)
        _store_session(db, p, _make_session_doc(p, status=terminal_status))
        before_clients = cc["n"]
        before_proxy = plm._total_acquires

        async with sm.acquire(p, module="m", auto_release=True) as lease:
            assert lease is None
            assert cc["n"] == before_clients, "zero client creation for terminal account"
            assert plm._total_acquires == before_proxy, "zero proxy acquisition for terminal account"

        await _assert_clean_resources(sm, plm)


# ---------------------------------------------------------------------------
# E) Module collisions (all enforced via SessionManager ownership)
# ---------------------------------------------------------------------------

class TestModuleCollisions:

    async def _collision(self, module1, module2, hold_during):
        from session_manager import SessionAlreadyOwnedError
        sm, db, plm, cc = _build_sm(pool_size=20)
        p = _make_phone(1)
        _store_session(db, p, _make_session_doc(p))

        async with sm.acquire(p, module=module1, auto_release=False) as lease:
            assert lease is not None
            with pytest.raises(SessionAlreadyOwnedError):
                async with sm.acquire(p, module=module2, auto_release=True) as l2:
                    pass
            key = sm._session_key(p)
            await sm._release_lease(key, lease.owner)
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_dm_vs_auditor(self):
        await self._collision("dmsender", "auditor", None)

    @pytest.mark.asyncio
    async def test_dm_vs_web_console(self):
        await self._collision("dmsender", "web_console", None)

    @pytest.mark.asyncio
    async def test_dm_vs_videochat(self):
        await self._collision("dmsender", "videochat", None)

    @pytest.mark.asyncio
    async def test_dm_vs_adder(self):
        await self._collision("dmsender", "adder", None)

    @pytest.mark.asyncio
    async def test_scraper_vs_dm(self):
        await self._collision("scraper_standard", "dmsender", None)

    @pytest.mark.asyncio
    async def test_login_blocks_auditor(self):
        sm, db, plm, cc = _build_sm(pool_size=10)
        from session_manager import SessionAlreadyOwnedError
        p = _make_phone(1)
        login_owner = f"login:{p.replace('+', '')}"

        reserved = await sm.reserve_login(p, login_owner)
        assert reserved is True

        # auditor cannot acquire while login is pending
        with pytest.raises(SessionAlreadyOwnedError):
            async with sm.acquire(_make_phone(1).replace("+", ""), module="auditor", auto_release=True) as l2:
                pass

        await sm.release_login(p.replace("+", ""), login_owner)
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_login_otp_2fa_stage_transitions(self):
        from session_manager import SessionLifecycleState
        sm, db, plm, cc = _build_sm(pool_size=10)
        p = _make_phone(1)
        norm = p.replace("+", "")
        owner = f"login:{norm}"

        assert await sm.reserve_login(p, owner) is True
        # LOGIN_PENDING by default
        assert sm._sessions[norm].lifecycle.value == "login_pending"
        # advance to OTP then 2FA
        assert await sm.set_login_stage(norm, owner, SessionLifecycleState.OTP_WAITING) is True
        assert sm._sessions[norm].lifecycle.value == "otp_waiting"
        assert await sm.set_login_stage(norm, owner, SessionLifecycleState.TWOFA_WAITING) is True
        assert sm._sessions[norm].lifecycle.value == "twofa_waiting"

        # wrong owner cannot advance
        assert await sm.set_login_stage(norm, "wrong", SessionLifecycleState.TWOFA_WAITING) is False

        await sm.release_login(norm, owner)
        assert norm not in sm._sessions
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_login_cannot_double_reserve(self):
        sm, db, plm, cc = _build_sm(pool_size=10)
        p = _make_phone(1)
        norm = p.replace("+", "")
        owner = f"login:{norm}"

        assert await sm.reserve_login(p, owner) is True
        assert await sm.reserve_login(p, "login:other") is False
        await sm.release_login(norm, owner)
        # after release, can reserve again
        assert await sm.reserve_login(p, "login:new") is True
        await sm.release_login(norm, "login:new")
        await _assert_clean_resources(sm, plm)


# ---------------------------------------------------------------------------
# F) Cancellation at every phase
# ---------------------------------------------------------------------------

class TestCancellationPhases:

    @pytest.mark.asyncio
    async def test_stale_acquisition_rollback_cannot_clear_live_lease(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        p = _make_phone(1)
        _store_session(db, p, _make_session_doc(p))

        async with sm.acquire(
            p,
            module="m",
            worker_id="same-worker",
            auto_release=False,
        ) as lease:
            assert lease is not None
            info = sm._sessions[sm.normalize_phone(p)]
            live_client = info.client

            await sm._rollback_acquire_failure(
                clean_phone=sm.normalize_phone(p),
                owner_key=lease.owner,
                reservation_id="stale-reservation",
                proxy_record=None,
                client=None,
            )

            assert info.client is live_client
            assert info.owner == lease.owner
            assert sm._active_count == 1

        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_cancel_during_proxy_acquisition(self):
        # proxy acquire blocks (block_on_exhausted) -> cancel -> nothing leaks
        sm, db, plm, cc = _build_sm(pool_size=0, block_on_exhausted=True)
        p = _make_phone(1)
        _store_session(db, p, _make_session_doc(p))

        release = asyncio.Event()
        async def worker():
            async with sm.acquire(p, module="m", auto_release=True) as lease:
                release.set()
                await asyncio.sleep(999)

        task = asyncio.create_task(worker())
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.2)
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_cancel_during_connect(self):
        blocking = asyncio.Event()
        sm, db, plm, cc = _build_sm(pool_size=5)
        p = _make_phone(1)
        _store_session(db, p, _make_session_doc(p))

        real_client = AsyncMock()
        real_client.is_connected = MagicMock(return_value=False)
        real_client.connect.side_effect = lambda: blocking.wait()
        real_client.is_user_authorized.return_value = True
        real_client.session = MagicMock()
        real_client.disconnect = AsyncMock()

        sm._create_client = lambda **k: real_client

        async def worker():
            async with sm.acquire(p, module="m", auto_release=True) as lease:
                if lease:
                    await asyncio.sleep(999)

        task = asyncio.create_task(worker())
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.3)
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_cancel_during_operation_honest_hold(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        p = _make_phone(1)
        _store_session(db, p, _make_session_doc(p))

        async def worker():
            async with sm.acquire(p, module="m", auto_release=True) as lease:
                if lease:
                    await asyncio.sleep(60)

        task = asyncio.create_task(worker())
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.3)
        await _assert_clean_resources(sm, plm)


# ---------------------------------------------------------------------------
# G) Shutdown with active workers/clients/proxies
# ---------------------------------------------------------------------------

class TestShutdownWithResources:

    @pytest.mark.asyncio
    async def test_shutdown_returns_all_to_zero(self):
        sm, db, plm, cc = _build_sm(pool_size=50)
        phones = [_make_phone(i) for i in range(20)]
        for p in phones:
            _store_session(db, p, _make_session_doc(p))

        held = []
        async def hold(i):
            async with sm.acquire(phones[i], module=f"m{i}", auto_release=False) as lease:
                if lease is not None:
                    held.append(lease)
                await asyncio.sleep(30)

        tasks = [asyncio.create_task(hold(i)) for i in range(20)]
        await asyncio.sleep(0.4)
        assert len(held) >= 10, "expected at least 10 simultaneous leases"
        assert _resource_counts(sm, plm)["active_clients"] >= 10
        assert _resource_counts(sm, plm)["active_proxy_leases"] >= 10

        # graceful shutdown: first cancel active workers so their leases release,
        # then disconnect_all so idle connected clients are torn down.
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await sm.disconnect_all()
        assert sm._active_count == 0
        assert len(plm._leased) == 0
        assert sm._sessions == {} or all(
            i.lifecycle.value in ("available", "disconnected")
            for i in sm._sessions.values()
        )

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await _assert_clean_resources(sm, plm)


# ---------------------------------------------------------------------------
# DM instrumentation smoke test (root-cause regression)
# ---------------------------------------------------------------------------

class TestDMInstrumentation:

    @pytest.mark.asyncio
    async def test_generate_live_status_exists_and_returns_str(self):
        from dmsender import EnterpriseDMSender
        sender = EnterpriseDMSender(FakeDB())
        sender.is_running = True
        sender.stats["total_targets"] = 10
        sender.stats["total_sent"] = 2
        sender.stats["failed"] = 0
        text = await sender._generate_live_status()
        assert isinstance(text, str) and len(text) > 20

    @pytest.mark.asyncio
    async def test_emit_logs_structured_events(self):
        from dmsender import EnterpriseDMSender
        sender = EnterpriseDMSender(FakeDB())
        sender._emit("campaign_start", detail="x")
        sender._emit("send_success", worker=1, phone="123", detail="ok")
        events = sender.get_lifecycle_events()
        assert [e["event"] for e in events] == ["campaign_start", "send_success"]
        assert events[1]["worker"] == 1
        assert events[1]["phone"] == "123"


# ---------------------------------------------------------------------------
# Phase 3.4 — public lease-release interface
# ---------------------------------------------------------------------------

class TestReleaseLeasePublicInterface:

    @pytest.mark.asyncio
    async def test_release_lease_public_releases_correctly(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        p = _make_phone(1)
        _store_session(db, p, _make_session_doc(p))
        async with sm.acquire(p, module="m", auto_release=False) as lease:
            assert lease is not None
            phone_key = sm._session_key(lease.phone)
            # session is BUSY while held
            assert sm._sessions[phone_key].lifecycle.value == "busy"
            await sm.release_lease(lease)
            assert sm._sessions[phone_key].lifecycle.value in ("available",)
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_release_lease_idempotent_double_release(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        p = _make_phone(2)
        _store_session(db, p, _make_session_doc(p))
        async with sm.acquire(p, module="m", auto_release=False) as lease:
            await sm.release_lease(lease)
            # second release of the same lease is a harmless no-op
            await sm.release_lease(lease)
            assert sm._sessions[sm._session_key(p)].lifecycle.value == "available"
            # client remains tracked (idle), but the session is not owned
            assert sm._sessions[sm._session_key(p)].owner is None
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_release_lease_none_is_noop(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        await sm.release_lease(None)  # must not raise
        assert sm._active_count == 0

    @pytest.mark.asyncio
    async def test_release_lease_wrong_owner_rejected(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        p = _make_phone(3)
        _store_session(db, p, _make_session_doc(p))
        async with sm.acquire(p, module="m1", auto_release=False) as lease:
            # a forged lease with a different owner must not release m1's session
            from dataclasses import replace
            forged = replace(lease, owner="intruder")
            await sm.release_lease(forged)
            assert sm._sessions[sm._session_key(p)].lifecycle.value == "busy"
        await _assert_clean_resources(sm, plm)

    @pytest.mark.asyncio
    async def test_release_lease_correct_owner_stale_lease_id_rejected(self):
        sm, db, plm, cc = _build_sm(pool_size=5)
        p = _make_phone(4)
        _store_session(db, p, _make_session_doc(p))
        async with sm.acquire(p, module="m1", auto_release=False) as lease:
            # correct owner but stale lease epoch -> rejected, resources stay
            from dataclasses import replace
            stale = replace(lease, lease_id="stale-lease")
            await sm.release_lease(stale)
            key = sm._session_key(p)
            assert sm._sessions[key].lifecycle.value == "busy"
            assert sm._sessions[key].lease_id == lease.lease_id
            assert sm._active_count == 1
        await _assert_clean_resources(sm, plm)

    def test_feature_modules_use_public_not_private(self):
        # Regression: feature modules must never call the private _release_lease.
        import os
        here = os.path.dirname(os.path.abspath(__file__))
        offenders = []
        for mod in ("adder", "dmsender", "web_console", "videochat", "main_bot"):
            path = os.path.join(here, f"{mod}.py")
            with open(path, encoding="utf-8") as fh:
                for lineno, line in enumerate(fh, 1):
                    if "._release_lease(" in line and not line.lstrip().startswith("#"):
                        offenders.append(f"{mod}.py:{lineno}")
        assert not offenders, f"private _release_lease still called in: {offenders}"


# ---------------------------------------------------------------------------
# Phase 13 — resource-aware DM scheduler capacity
# ---------------------------------------------------------------------------

class TestResourceAwareScheduling:

    def test_dm_capacity_bounded_by_proxies(self):
        from dmsender import compute_dm_worker_capacity
        # 20 accounts, but only 5 proxies -> 5 workers
        assert compute_dm_worker_capacity(20, 5, 20, 20) == 5

    def test_dm_capacity_bounded_by_session_capacity(self):
        from dmsender import compute_dm_worker_capacity
        # many proxies but limited session capacity
        assert compute_dm_worker_capacity(20, 20, 4, 20) == 4

    def test_dm_capacity_respects_configured_limit(self):
        from dmsender import compute_dm_worker_capacity
        # plenty of resources, configured limit is the binding constraint
        assert compute_dm_worker_capacity(100, 50, 50, 10) == 10

    def test_dm_capacity_at_least_one_when_accounts_exist(self):
        from dmsender import compute_dm_worker_capacity
        # Temporarily zero proxies/session capacity: still keep 1 worker so the
        # engine blocks (WAITING_FOR_PROXY) instead of dropping valid work.
        assert compute_dm_worker_capacity(5, 0, 0, 20) == 1

    def test_dm_capacity_zero_when_no_accounts(self):
        from dmsender import compute_dm_worker_capacity
        assert compute_dm_worker_capacity(0, 10, 10, 20) == 0

    def test_dm_capacity_never_exceeds_accounts(self):
        from dmsender import compute_dm_worker_capacity
        assert compute_dm_worker_capacity(3, 20, 20, 20) == 3


# ---------------------------------------------------------------------------
# Phase 13b — real ProxyLeaseManager + SessionManager count invariants
# ---------------------------------------------------------------------------

class TestResourceInvariants:

    @pytest.mark.asyncio
    async def test_session_and_proxy_counts_match(self):
        from proxy_manager import ProxyLeaseManager
        from session_manager import SessionManager

        class _WorkingProxyManager:
            def __init__(self):
                self.working_proxies = [
                    {
                        "proxy_id": f"real-{i}",
                        "url": f"socks5://p{i}.example:1080",
                        "host": f"p{i}.example",
                        "addr": f"p{i}.example",
                        "port": 1080,
                        "type": "socks5",
                        "proxy_type": "socks5",
                    }
                    for i in range(3)
                ]
                self.provider = type("Provider", (), {"name": "test"})()

        pm = _WorkingProxyManager()
        plm = ProxyLeaseManager(pm)
        await plm.start()

        db = FakeDB()
        phones = [_make_phone(i) for i in range(3)]
        for p in phones:
            _store_session(db, p, _make_session_doc(p))

        sm = SessionManager(db, pm, plm)
        create_counter = {"n": 0}

        def _fake_create(**kwargs):
            create_counter["n"] += 1
            return _mock_client(kwargs.get("session_str", "test"), connected=True)

        sm._create_client = _fake_create

        try:
            for i, p in enumerate(phones):
                async with sm.acquire(p, module=f"m{i}", auto_release=False):
                    pass

            live_clients = sum(
                1 for info in sm._sessions.values()
                if info.client is not None
            )
            assert sm._active_count == live_clients
            assert sm._active_count == 3

            leased_nodes = sum(
                1 for n in plm.proxy_nodes.values()
                if n.is_leased
            )
            assert plm.stats["current_active_leases"] == leased_nodes
            assert plm.stats["current_active_leases"] == 3
            assert (
                0
                <= plm.stats["current_active_leases"]
                <= len(plm.proxy_nodes)
            )

            await sm.disconnect_all()

            assert sm._active_count == 0
            assert sm._active_count == sum(
                1 for info in sm._sessions.values()
                if info.client is not None
            )
            assert plm.stats["current_active_leases"] == sum(
                1 for n in plm.proxy_nodes.values()
                if n.is_leased
            )
            assert plm.stats["current_active_leases"] == 0

            # repeated shutdown is idempotent
            await sm.disconnect_all()
            assert sm._active_count == 0
            assert plm.stats["current_active_leases"] == 0
        finally:
            await plm.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
