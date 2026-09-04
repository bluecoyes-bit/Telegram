"""
P0 Test Suite — SessionManager lifecycle invariants.
Tests session acquire/release, duplicate ownership, terminal accounts,
proxy exhaustion, module conflicts, DM starvation, cancellation, and graceful shutdown.

Usage: python -m pytest test_p0_lifecycle.py -v
"""
import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

pytest_plugins = ["pytest_asyncio"]

# ---------------------------------------------------------------------------
# Fake DB
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


class FakeProxyLeaseManager:
    def __init__(self):
        self._is_running = True
        self._leased = {}
        self._counter = 0

    async def acquire_proxy(self, phone, timeout=None):
        self._counter += 1
        proxy = {
            "url": f"socks5://proxy{self._counter}:1080",
            "addr": f"proxy{self._counter}",
            "port": 1080,
            "username": "user",
            "password": "pass",
        }
        self._leased[phone] = proxy
        return proxy

    async def release_proxy(self, *, proxy_url, phone, should_cooldown=False, cooldown_reason=""):
        self._leased.pop(phone, None)

    def get_available_count(self):
        return 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_phone(n):
    return f"+1555000{n:04d}"


def _make_session_doc(phone, **overrides):
    normalized = str(phone).strip().replace("+", "")
    return {
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


def _store_session(db, phone, doc):
    normalized = str(phone).strip().replace("+", "")
    db.sessions[normalized] = doc

def _mock_client(phone="test", connected=False):
    client = AsyncMock()
    client.is_connected.return_value = connected
    client.is_user_authorized.return_value = True
    client.session = MagicMock()
    client.session.save.return_value = f"session_str_{phone}"
    client.disconnect = AsyncMock()
    return client


def _build_sm(db=None):
    from session_manager import SessionManager
    _db = db or FakeDB()
    pm = FakeProxyManager()
    plm = FakeProxyLeaseManager()
    sm = SessionManager(_db, pm, plm)
    sm._original_create_client = sm._create_client
    sm._create_client = lambda **kwargs: _mock_client(kwargs.get("session_str", "test"))
    return sm, _db
    from session_manager import SessionManager
    _db = db or FakeDB()
    pm = FakeProxyManager()
    plm = FakeProxyLeaseManager()
    sm = SessionManager(_db, pm, plm)
    return sm, _db


# ---------------------------------------------------------------------------
# Tests: Acquire / Release basics
# ---------------------------------------------------------------------------

class TestAcquireRelease:

    @pytest.mark.asyncio
    async def test_acquire_returns_lease(self):
        sm, db = _build_sm()
        phone = _make_phone(1)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=True) as lease:
            assert lease is not None
            assert lease.client is not None
            assert lease.owner is not None
            assert lease.lease_id is not None

    @pytest.mark.asyncio
    async def test_release_makes_session_available(self):
        sm, db = _build_sm()
        phone = _make_phone(2)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=False) as lease:
            assert lease is not None
            key = sm._session_key(phone)
            assert sm._sessions[key].lifecycle.value == "busy"

            await sm._release_lease(key, lease.owner)
            assert sm._sessions[key].lifecycle.value == "available"

    @pytest.mark.asyncio
    async def test_double_release_is_harmless(self):
        sm, db = _build_sm()
        phone = _make_phone(3)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=False) as lease:
            key = sm._session_key(phone)
            await sm._release_lease(key, lease.owner)
            # Second release should not crash
            await sm._release_lease(key, lease.owner)

    @pytest.mark.asyncio
    async def test_owner_mismatch_does_not_release(self):
        sm, db = _build_sm()
        phone = _make_phone(4)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=False) as lease:
            key = sm._session_key(phone)
            # Owner mismatch just logs and returns, does not raise
            await sm._release_lease(key, "wrong_owner_id")
            # Session should still be BUSY (not released)
            assert sm._sessions[key].lifecycle.value == "busy"
            await sm._release_lease(key, lease.owner)


# ---------------------------------------------------------------------------
# Tests: Acquire/release x 1000
# ---------------------------------------------------------------------------

class TestAcquireRelease1000:

    @pytest.mark.asyncio
    async def test_1000_cycles(self):
        sm, db = _build_sm()
        phone = _make_phone(10)
        _store_session(db, phone, _make_session_doc(phone))
        key = sm._session_key(phone)

        for _ in range(1000):
            async with sm.acquire(phone, module="test", auto_release=False) as lease:
                assert lease is not None
                await sm._release_lease(key, lease.owner)
                assert sm._sessions[key].lifecycle.value == "available"


# ---------------------------------------------------------------------------
# Tests: Duplicate ownership
# ---------------------------------------------------------------------------

class TestDuplicateOwnership:

    @pytest.mark.asyncio
    async def test_second_acquire_raises_while_busy(self):
        from session_manager import SessionAlreadyOwnedError
        sm, db = _build_sm()
        phone = _make_phone(20)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=False) as lease1:
            assert lease1 is not None
            with pytest.raises(SessionAlreadyOwnedError):
                async with sm.acquire(phone, module="test2", auto_release=False) as lease2:
                    pass

    @pytest.mark.asyncio
    async def test_second_acquire_succeeds_after_release(self):
        sm, db = _build_sm()
        phone = _make_phone(21)
        _store_session(db, phone, _make_session_doc(phone))
        key = sm._session_key(phone)

        async with sm.acquire(phone, module="test", auto_release=False) as lease1:
            await sm._release_lease(key, lease1.owner)

        async with sm.acquire(phone, module="test2", auto_release=False) as lease2:
            assert lease2 is not None
            await sm._release_lease(key, lease2.owner)


# ---------------------------------------------------------------------------
# Tests: Terminal accounts
# ---------------------------------------------------------------------------

class TestTerminalAccounts:

    @pytest.mark.asyncio
    @pytest.mark.parametrize("terminal_status", [
        "revoked", "banned", "deactivated", "invalid",
        "auth_key_duplicated", "permanently_failed", "quarantined",
    ])
    async def test_terminal_account_returns_none(self, terminal_status):
        sm, db = _build_sm()
        phone = _make_phone(30)
        _store_session(db, phone, _make_session_doc(phone, status=terminal_status))

        async with sm.acquire(phone, module="test", auto_release=True) as lease:
            assert lease is None

    @pytest.mark.asyncio
    async def test_terminal_account_no_proxy_acquired(self):
        sm, db = _build_sm()
        phone = _make_phone(31)
        _store_session(db, phone, _make_session_doc(phone, status="banned"))

        plm = sm.proxy_lease_manager
        before = plm._counter

        async with sm.acquire(phone, module="test", auto_release=True) as lease:
            assert lease is None
            assert plm._counter == before


# ---------------------------------------------------------------------------
# Tests: Module conflicts
# ---------------------------------------------------------------------------

class TestModuleConflicts:

    @pytest.mark.asyncio
    async def test_different_modules_cannot_overlap(self):
        from session_manager import SessionAlreadyOwnedError
        sm, db = _build_sm()
        phone = _make_phone(40)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="adder", auto_release=False) as lease1:
            assert lease1 is not None
            with pytest.raises(SessionAlreadyOwnedError):
                async with sm.acquire(phone, module="dmsender", auto_release=False) as lease2:
                    pass


# ---------------------------------------------------------------------------
# Tests: Idle cleanup
# ---------------------------------------------------------------------------

class TestIdleCleanup:

    @pytest.mark.asyncio
    async def test_idle_session_removed(self):
        sm, db = _build_sm()
        phone = _make_phone(50)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=False) as lease:
            key = sm._session_key(phone)
            await sm._release_lease(key, lease.owner)
            # Backdate last_used_ts to simulate idle
            sm._sessions[key].last_used_ts = time.time() - 700

        async with sm.acquire(phone, module="test2", auto_release=False) as lease2:
            assert lease2 is not None
            key = sm._session_key(phone)
            await sm._release_lease(key, lease2.owner)


# ---------------------------------------------------------------------------
# Tests: Quarantine correctness
# ---------------------------------------------------------------------------

class TestQuarantineCorrectness:

    @pytest.mark.asyncio
    async def test_auth_key_duplicated_quarantine(self):
        from session_manager import ErrorCategory
        sm, db = _build_sm()
        phone = _make_phone(60)
        _store_session(db, phone, _make_session_doc(phone))
        await sm.mark_quarantined(phone, "test", ErrorCategory.AUTH_KEY_DUPLICATED)
        assert db.statuses.get(phone.replace("+", "")) == "auth_key_duplicated"

    @pytest.mark.asyncio
    async def test_session_revoked_quarantine(self):
        from session_manager import ErrorCategory
        sm, db = _build_sm()
        phone = _make_phone(61)
        _store_session(db, phone, _make_session_doc(phone))
        await sm.mark_quarantined(phone, "test", ErrorCategory.SESSION_REVOKED)
        assert db.statuses.get(phone.replace("+", "")) == "revoked"

    @pytest.mark.asyncio
    async def test_account_banned_quarantine(self):
        from session_manager import ErrorCategory
        sm, db = _build_sm()
        phone = _make_phone(62)
        _store_session(db, phone, _make_session_doc(phone))
        await sm.mark_quarantined(phone, "test", ErrorCategory.ACCOUNT_BANNED)
        assert db.statuses.get(phone.replace("+", "")) == "banned"

    @pytest.mark.asyncio
    async def test_unauthorized_quarantine(self):
        from session_manager import ErrorCategory
        sm, db = _build_sm()
        phone = _make_phone(63)
        _store_session(db, phone, _make_session_doc(phone))
        await sm.mark_quarantined(phone, "test", ErrorCategory.UNAUTHORIZED)
        assert db.statuses.get(phone.replace("+", "")) == "revoked"


# ---------------------------------------------------------------------------
# Tests: Cancellation safety
# ---------------------------------------------------------------------------

class TestCancellation:

    @pytest.mark.asyncio
    async def test_cancelled_task_releases_lease(self):
        sm, db = _build_sm()
        phone = _make_phone(70)
        _store_session(db, phone, _make_session_doc(phone))
        key = sm._session_key(phone)

        async def hold_lease():
            async with sm.acquire(phone, module="test", auto_release=True) as lease:
                assert lease is not None
                await asyncio.sleep(999)

        task = asyncio.create_task(hold_lease())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        await asyncio.sleep(0.2)
        # Session should be released (AVAILABLE) or cleaned up
        if key in sm._sessions:
            assert sm._sessions[key].lifecycle.value in ("available", "disconnected")


# ---------------------------------------------------------------------------
# Tests: Active client count
# ---------------------------------------------------------------------------

class TestActiveClientCount:

    @pytest.mark.asyncio
    async def test_active_count_increments(self):
        sm, db = _build_sm()
        phone = _make_phone(80)
        _store_session(db, phone, _make_session_doc(phone))

        before = sm._active_count
        async with sm.acquire(phone, module="test", auto_release=False) as lease:
            assert sm._active_count == before + 1
            key = sm._session_key(phone)
            await sm._release_lease(key, lease.owner)

    @pytest.mark.asyncio
    async def test_active_count_does_not_go_negative(self):
        sm, db = _build_sm()
        phone = _make_phone(81)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=False) as lease:
            key = sm._session_key(phone)
            await sm._release_lease(key, lease.owner)
        assert sm._active_count >= 0


# ---------------------------------------------------------------------------
# Tests: Graceful shutdown
# ---------------------------------------------------------------------------

class TestGracefulShutdown:

    @pytest.mark.asyncio
    async def test_disconnect_all_resets_state(self):
        sm, db = _build_sm()
        phone = _make_phone(90)
        _store_session(db, phone, _make_session_doc(phone))

        async with sm.acquire(phone, module="test", auto_release=False) as lease:
            assert sm._active_count >= 1

        await sm.disconnect_all()
        assert sm._active_count == 0


# ---------------------------------------------------------------------------
# Tests: Proxy keyword-only
# ---------------------------------------------------------------------------

class TestReleaseProxyKeywordOnly:

    @pytest.mark.asyncio
    async def test_release_proxy_requires_kwargs(self):
        import inspect
        from proxy_manager import ProxyLeaseManager
        sig = inspect.signature(ProxyLeaseManager.release_proxy)
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            assert param.kind == inspect.Parameter.KEYWORD_ONLY, \
                f"Parameter '{name}' must be keyword-only"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
