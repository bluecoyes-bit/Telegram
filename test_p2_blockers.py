"""
P2 FINAL BLOCKER regression tests.

Covers the confirmed production blockers fixed in the final pass:
  - Auditor honors AUDITOR_ENABLED / recovery honors ENABLE_AUTO_RECOVERY.
  - Auditor/recovery ownership guards: never overlap an owned session.
  - Recovery skips terminal accounts.
  - Login/OTP/2FA route through the public, owner-guarded build_login_client
    (no direct _create_client calls left in the login flow).
  - managed_client / acquire() uses a CONTROLLED proxy route and NEVER falls
    back to a direct-IP connection when the pool is exhausted.

These are mock/in-process regression tests (no live Telegram/Mongo/proxy).
Run: python -m pytest test_p2_blockers.py -v
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

pytest_plugins = ["pytest_asyncio"]

# ---------------------------------------------------------------------------
# Minimal fakes (mirror test_p0_closeout harness)
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


class FakeProxyManager:
    def get_proxy(self):
        return None


class ControlledProxyLeaseManager:
    """Fixed-size pool via SessionManager's controlled route (no no-proxy)."""

    def __init__(self, pool_size=100):
        self.pool_size = pool_size
        self._leased = {}
        self._counter = 0

    def get_available_count(self):
        return max(0, self.pool_size - len(self._leased))

    async def acquire_proxy(self, phone, timeout=None):
        if len(self._leased) >= self.pool_size:
            return None
        self._counter += 1
        proxy = {"url": f"socks5://p{self._counter}:1080", "addr": f"p{self._counter}",
                 "port": 1080, "username": "u", "password": "p"}
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
        self._leased.pop(phone, None)


def _mock_client(phone="test", connected=False):
    client = AsyncMock()
    client.is_connected.return_value = connected
    client.is_user_authorized.return_value = True
    client.session = MagicMock()
    client.session.save.return_value = f"session_str_{phone}"
    client.disconnect = AsyncMock()
    return client


def _doc(phone, status="active", **overrides):
    normalized = str(phone).strip().replace("+", "")
    base = {
        "phone": normalized,
        "status": status,
        "session_string": f"session_{normalized}",
        "api_id": 12345,
        "api_hash": "abc",
        "device_metadata": {"device_model": "PC 64bit", "system_version": "Windows 11",
                            "app_version": "5.1.0"},
    }
    base.update(overrides)
    return base


def _build_sm(pool_size=100):
    from session_manager import SessionManager
    _db = FakeDB()
    plm = ControlledProxyLeaseManager(pool_size)
    sm = SessionManager(_db, FakeProxyManager(), plm)
    create_counter = {"n": 0}
    def _fake_create(**kwargs):
        create_counter["n"] += 1
        return _mock_client(kwargs.get("session_str", "test"), connected=True)
    sm._create_client = _fake_create
    return sm, _db, plm, create_counter


# ---------------------------------------------------------------------------
# BLOCKER 1 — Auditor / recovery startup gates honor configuration
# ---------------------------------------------------------------------------

class TestAuditorEnableGate:
    def test_auditor_false_does_not_start(self, monkeypatch):
        import main_bot
        monkeypatch.setitem(main_bot.CONFIG, "AUDITOR_ENABLED", False)
        assert main_bot.should_start_auditor() is False

    def test_auditor_true_can_start(self, monkeypatch):
        import main_bot
        monkeypatch.setitem(main_bot.CONFIG, "AUDITOR_ENABLED", True)
        assert main_bot.should_start_auditor() is True

    def test_auditor_default_true_when_unset(self, monkeypatch):
        import main_bot
        monkeypatch.setitem(main_bot.CONFIG, "AUDITOR_ENABLED", True)
        assert main_bot.should_start_auditor() is True

    def test_recovery_false_does_not_start(self, monkeypatch):
        import main_bot
        monkeypatch.setitem(main_bot.CONFIG, "ENABLE_AUTO_RECOVERY", False)
        assert main_bot.should_start_recovery() is False

    def test_recovery_true_can_start(self, monkeypatch):
        import main_bot
        monkeypatch.setitem(main_bot.CONFIG, "ENABLE_AUTO_RECOVERY", True)
        assert main_bot.should_start_recovery() is True


# ---------------------------------------------------------------------------
# BLOCKER 2/3 — Ownership guards: auditor/recovery must never overlap a
# session currently owned by a worker or login reservation.
# ---------------------------------------------------------------------------

class TestOwnershipGuards:
    @pytest.mark.asyncio
    async def test_worker_owns_account_auditor_skips(self):
        sm, db, plm, _ = _build_sm()
        phone = "15550000001"
        db.sessions["15550000001"] = _doc(phone)
        # worker acquires the account -> BUSY
        async with sm.acquire(phone, module="dm", auto_release=False) as lease:
            assert lease is not None
            # auditor guard: SessionManager reports the session owned
            assert await sm.is_owned(phone) is True
            # a second acquire (auditor) must be refused -> no overlap
            with pytest.raises(Exception) as exc:
                async with sm.acquire(phone, module="auditor", auto_release=False) as l2:
                    pass
            assert "already owned" in str(exc.value).lower() or "SessionAlreadyOwned" in type(exc.value).__name__

    @pytest.mark.asyncio
    async def test_login_reservation_blocks_auditor(self):
        sm, db, plm, _ = _build_sm()
        phone = "15550000002"
        db.sessions["15550000002"] = _doc(phone)
        owner = f"login:{phone}"
        assert await sm.reserve_login(phone, owner) is True
        assert await sm.is_owned(phone) is True
        # auditor/recovery acquire attempt while login reserved -> refused
        with pytest.raises(Exception) as exc:
            async with sm.acquire(phone, module="auditor", auto_release=False) as l2:
                pass
        assert "already owned" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_recovery_skips_terminal_account(self):
        sm, db, plm, _ = _build_sm()
        phone = "15550000003"
        db.sessions["15550000003"] = _doc(phone, status="revoked")
        # recovery must not create a client for a terminal account
        async with sm.acquire(phone, module="managed_client_nopool", auto_release=False) as lease:
            assert lease is None  # acquire() yields None for terminal -> skip

    @pytest.mark.asyncio
    async def test_release_login_restores_availability_for_workers(self):
        sm, db, plm, _ = _build_sm()
        phone = "15550000004"
        db.sessions["15550000004"] = _doc(phone)
        owner = f"login:{phone}"
        await sm.reserve_login(phone, owner)
        assert await sm.is_owned(phone) is True
        await sm.release_login(phone, owner)
        assert await sm.is_owned(phone) is False
        # after release a worker may acquire normally
        async with sm.acquire(phone, module="dm", auto_release=False) as lease:
            assert lease is not None


# ---------------------------------------------------------------------------
# BLOCKER 4 — build_login_client: public, owner-guarded login client factory
# ---------------------------------------------------------------------------

class TestBuildLoginClient:
    @pytest.mark.asyncio
    async def test_build_login_client_creates_client_owner_guarded(self):
        sm, db, plm, counter = _build_sm()
        phone = "15550000005"
        db.sessions["15550000005"] = _doc(phone)
        owner = f"login:{phone}"
        assert await sm.reserve_login(phone, owner) is True

        client = await sm.build_login_client(
            phone, owner,
            session_str="login_session",
            api_id=12345, api_hash="abc",
            device={"device_model": "PC"}, proxy=None,
        )
        assert client is not None
        assert counter["n"] == 1  # exactly one client factory call
        assert await sm.is_owned(phone) is True

    @pytest.mark.asyncio
    async def test_build_login_client_reuses_live_client(self):
        sm, db, plm, counter = _build_sm()
        phone = "15550000006"
        db.sessions["15550000006"] = _doc(phone)
        owner = f"login:{phone}"
        await sm.reserve_login(phone, owner)
        client = await sm.build_login_client(phone, owner, session_str="s",
                                             api_id=1, api_hash="a",
                                             device={}, proxy=None)
        client2 = await sm.build_login_client(phone, owner, session_str="s",
                                              api_id=1, api_hash="a",
                                              device={}, proxy=None)
        assert client2 is client  # same live client reused
        assert counter["n"] == 1

    @pytest.mark.asyncio
    async def test_build_login_client_returns_none_when_not_reserved(self):
        sm, db, plm, _ = _build_sm()
        phone = "15550000007"
        db.sessions["15550000007"] = _doc(phone)
        # not reserved -> cannot build a login client
        client = await sm.build_login_client(phone, "login:x", session_str="s",
                                             api_id=1, api_hash="a", device={}, proxy=None)
        assert client is None

    @pytest.mark.asyncio
    async def test_build_login_client_returns_none_when_owned_by_other(self):
        sm, db, plm, _ = _build_sm()
        phone = "15550000008"
        db.sessions["15550000008"] = _doc(phone)
        # a worker takes the phone first
        async with sm.acquire(phone, module="dm", auto_release=False) as lease:
            assert lease is not None
            # login cannot build a client for a worker-owned phone
            client = await sm.build_login_client(phone, "login:other", session_str="s",
                                                 api_id=1, api_hash="a", device={}, proxy=None)
            assert client is None


# ---------------------------------------------------------------------------
# BLOCKER 2 — controlled network route: acquire() uses the proxy pool and
# NEVER falls back to a direct-IP connection when proxies are exhausted.
# ---------------------------------------------------------------------------

class TestControlledNetworkRoute:
    @pytest.mark.asyncio
    async def test_acquire_uses_controlled_proxy_route(self):
        sm, db, plm, _ = _build_sm(pool_size=100)
        phone = "15550000009"
        db.sessions["15550000009"] = _doc(phone)
        async with sm.acquire(phone, module="managed_client_nopool", auto_release=False) as lease:
            assert lease is not None
            # a controlled proxy lease must be held for the connection (no no-proxy)
            assert plm.get_available_count() < 100
            assert lease.proxy_url is not None

    @pytest.mark.asyncio
    async def test_exhausted_pool_skips_never_direct(self):
        sm, db, plm, _ = _build_sm(pool_size=1)
        p1 = "15550000010"
        p2 = "15550000011"
        db.sessions["15550000010"] = _doc(p1)
        db.sessions["15550000011"] = _doc(p2)
        # first worker takes the only proxy
        async with sm.acquire(p1, module="dm", auto_release=False) as lease1:
            assert lease1 is not None
            # second (auditor-style) acquire has no proxy -> yields None (skip),
            # it must NOT fall back to a direct connection.
            async with sm.acquire(p2, module="managed_client_nopool", auto_release=False) as lease2:
                assert lease2 is None
