"""
PATCH #8 Test Suite — main_bot orchestration lifecycle.

Covers:
  - Auditor: startup gates, bounded effective capacity, network fail-safe,
    deterministic LRU normalization, bounded run-pass (skip-on-no-capacity +
    bounded concurrency with human-like delay zeroed for tests).
  - Auto-recovery: startup gate, terminal/busy/owned guards, acquire-race skip,
    and recovery through a SessionManager-owned managed_client().
  - Login lifecycle: managed_client() delegation, shared_login_process rotation
    and exhaustion against build_login_client()/release_login()/reserve_login(),
    OTP listener idempotency, past-OTP capture.
  - Background tasks: bounded restarts (restart, stop-at-limit, cancel), tracked
    background-task cancellation, ordered + idempotent lifespan shutdown.
  - GlobalState: non-owning auth state replacement; stale-state release through
    the public SessionManager API.
  - Source audit: no private SessionManager access / no direct client-pool;
    2FA password never persisted, no disconnect in finally.
  - check_session_authorization() status matrix.

Run:        python -m pytest test_main_bot_lifecycle.py -v
Baselines:  python -m pytest test_p0_*.py test_proxy_lease.py \
                test_adder_lifecycle.py test_dmsender_lifecycle.py \
                test_videochat_lifecycle.py -q
"""
import asyncio
import io
import os
import time
from contextlib import suppress
from types import SimpleNamespace

import pytest

pytest_plugins = ["pytest_asyncio"]

# ---------------------------------------------------------------------------
# Neutralized import: main_bot must import without a live MongoDB connection.
# ---------------------------------------------------------------------------
import database as _database_mod

_database_mod.SuiteDatabase._init_mongo = lambda self: None
import main_bot
from telethon.errors import AuthKeyUnregisteredError
from session_manager import SessionAlreadyOwnedError

ROOT = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeCodeSent:
    def __init__(self, code_hash):
        self.phone_code_hash = code_hash


class FakeClient:
    """Async-mimicking Telethon client stand-in."""

    def __init__(self, authorized=True, code_hash="hash123",
                 fail_attempts=0):
        self.authorized = authorized
        self.code_hash = code_hash
        self.fail_attempts = fail_attempts
        self.send_calls = 0
        self.connected = False
        self.disconnect_calls = 0
        self.session_string = "FAKESESSIONSTR"
        self._connection = None

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connected = True

    async def send_code_request(self, phone):
        self.send_calls += 1
        if self.send_calls <= self.fail_attempts:
            raise ConnectionError("proxy timed out during send_code_request")
        return FakeCodeSent(self.code_hash)

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        return None

    async def send_message(self, *args, **kwargs):
        return None

    async def sign_in(self, password=None):
        self.connected = True

    async def disconnect(self):
        self.disconnect_calls += 1
        self.connected = False

    @property
    def session(self):
        return self

    def save(self):
        return self.session_string


class FakeLease:
    def __init__(self, client):
        self.client = client
        self.proxy_url = "socks5://lease:1080"


class _AsyncCM:
    def __init__(self, value=None, exc=None):
        self.value = value
        self.exc = exc

    async def __aenter__(self):
        if self.exc is not None:
            raise self.exc
        return self.value

    async def __aexit__(self, *exc):
        return False


class FakeSM:
    """Public-SessionManager-API fake with call recording."""

    def __init__(self, events=None):
        self.events = events if events is not None else []
        self.acquire_calls = []
        self.build_calls = []
        self.release_calls = []
        self.reserve_calls = []
        self.stage_calls = []
        self.owned_calls = []
        self.quarantine_calls = []
        self.disconnect_all_calls = 0
        self.acquire_result = "client"
        self.acquire_exc = None
        self.is_owned_result = False
        self.reserve_ok = True
        self.build_result = "client"

    def _record(self, tag):
        self.events.append(tag)

    def acquire(self, phone, module=None, worker_id=None, auto_release=True,
                **kwargs):
        self.acquire_calls.append((phone, module, worker_id, auto_release))
        if self.acquire_exc is not None:
            return _AsyncCM(exc=self.acquire_exc)
        client = self.acquire_result
        if client is None:
            return _AsyncCM(None)
        if isinstance(client, FakeClient):
            return _AsyncCM(FakeLease(client))
        return _AsyncCM(client)

    async def build_login_client(self, phone, owner, **kwargs):
        self._record("build_login_client")
        self.build_calls.append((phone, owner, kwargs))
        if self.build_result is None:
            return None
        if isinstance(self.build_result, FakeClient):
            return self.build_result
        return FakeClient()

    async def release_login(self, phone, owner):
        self._record("release_login")
        self.release_calls.append((phone, owner))

    async def reserve_login(self, phone, owner):
        self._record("reserve_login")
        self.reserve_calls.append((phone, owner))
        return self.reserve_ok

    async def set_login_stage(self, phone, owner, stage):
        self.stage_calls.append((phone, owner, stage))

    async def is_owned(self, phone):
        self._record("is_owned")
        self.owned_calls.append(phone)
        return self.is_owned_result

    async def mark_quarantined(self, phone, **kwargs):
        self.quarantine_calls.append((phone, kwargs))

    async def disconnect_all(self):
        self._record("disconnect_all")
        self.disconnect_all_calls += 1


class FakePLM:
    def __init__(self, available=7, events=None):
        self.available = available
        self.raise_error = False
        self.events = events if events is not None else []

    def get_available_count(self):
        if self.raise_error:
            raise RuntimeError("pool down")
        return self.available

    async def start(self):
        self.events.append("plm_start")

    async def stop(self):
        self.events.append("plm_stop")


class FakeALM:
    def __init__(self, events=None):
        self.events = events if events is not None else []
        self.busy = set()

    async def is_busy(self, phone):
        return phone in self.busy

    async def start(self):
        self.events.append("alm_start")

    async def stop(self):
        self.events.append("alm_stop")


class FakeDB:
    def __init__(self, events=None):
        self.events = events if events is not None else []
        self.sessions = {}
        self.pending = []
        self.otp_logs = []
        self.statuses = {}
        self.saved_authorized = []
        self.close_calls = 0

    def _norm(self, phone):
        return str(phone).replace(" ", "").replace("+", "")

    def get_session_by_phone(self, phone):
        return self.sessions.get(self._norm(phone))

    def save_pending_session(self, phone, session_str, status, code_hash,
                             device):
        self.pending.append((self._norm(phone), session_str, status))

    def log_received_otp(self, phone, sender, message):
        self.otp_logs.append((self._norm(phone), sender, message))

    def update_session_status(self, phone, status, session_str=None):
        self.statuses[self._norm(phone)] = status

    def set_account_state(self, phone, status):
        self.statuses[self._norm(phone)] = status

    def save_authorized_session(self, phone, session_str, status, device,
                                two_fa_password=None):
        self.saved_authorized.append((self._norm(phone), two_fa_password))

    def close(self):
        self.close_calls += 1
        self.events.append("db_close")


class FakeOtpClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.handlers = []

    def on(self, *args, **kwargs):
        def deco(fn):
            self.handlers.append(fn)
            return fn
        return deco


class FakeEvent:
    def __init__(self, text="OTP-12345 from Telegram"):
        self.message = SimpleNamespace(message=text)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def patch_deps(monkeypatch):
    """Patch main_bot module globals with recording fakes."""
    out = {}
    db = FakeDB()
    sm = FakeSM()
    plm = FakePLM()
    alm = FakeALM()
    monkeypatch.setattr(main_bot, "db", db)
    monkeypatch.setattr(main_bot, "session_manager", sm)
    monkeypatch.setattr(main_bot, "proxy_lease_manager", plm)
    monkeypatch.setattr(main_bot, "account_lease_manager", alm)
    out.update(db=db, sm=sm, plm=plm, alm=alm)
    return out


@pytest.fixture(autouse=True)
def _reset_global_state():
    main_bot._shutdown_done = False
    main_bot.GLOBAL.auth_states = {}
    main_bot.GLOBAL.background_tasks = set()
    yield
    main_bot._shutdown_done = False


# ---------------------------------------------------------------------------
# AUDITOR — 9 tests
# ---------------------------------------------------------------------------

def test_should_start_auditor_default_true(monkeypatch):
    monkeypatch.setattr(main_bot, "CONFIG", {})
    assert main_bot.should_start_auditor() is True


def test_should_start_auditor_disabled(monkeypatch):
    monkeypatch.setattr(main_bot, "CONFIG", {"AUDITOR_ENABLED": False})
    assert main_bot.should_start_auditor() is False


def test_effective_capacity_bounded_by_eligible():
    cap = main_bot._auditor_effective_capacity(
        eligible_accounts=3, available_network_capacity=50,
        configured_audit_concurrency=10)
    assert cap == 3


def test_effective_capacity_bounded_by_network():
    cap = main_bot._auditor_effective_capacity(
        eligible_accounts=50, available_network_capacity=2,
        configured_audit_concurrency=10)
    assert cap == 2


def test_effective_capacity_bounded_by_configured():
    cap = main_bot._auditor_effective_capacity(
        eligible_accounts=50, available_network_capacity=50,
        configured_audit_concurrency=4)
    assert cap == 4


def test_effective_capacity_zero_when_no_eligible():
    cap = main_bot._auditor_effective_capacity(
        eligible_accounts=0, available_network_capacity=50,
        configured_audit_concurrency=4)
    assert cap == 0


def test_auditor_network_capacity_failsafe_zero(monkeypatch):
    plm = FakePLM(available=12)
    monkeypatch.setattr(main_bot, "proxy_lease_manager", plm)
    assert main_bot._auditor_network_capacity() == 12
    plm.raise_error = True
    assert main_bot._auditor_network_capacity() == 0


def test_normalize_check_time_mixed_types():
    cases = [
        ({"last_checked_time": 100.0}, 100.0),
        ({"last_checked_time": "42"}, 42.0),
        ({"last_checked_time": "garbage"}, 0.0),
        ({"last_updated": 55}, 55.0),
        ({}, 0.0),
    ]
    for doc, expected in cases:
        assert main_bot._normalize_check_time(doc) == expected


@pytest.mark.asyncio
async def test_auditor_run_pass_skip_and_bounded(monkeypatch):
    monkeypatch.setattr(main_bot.random, "uniform", lambda a, b: 0.0)
    ca = []

    async def fake_audit(acc):
        ca.append(acc.get("phone"))
        if acc.get("phone") == "b":
            raise RuntimeError("audit failure")
        return True

    monkeypatch.setattr(main_bot, "_audit_single_account", fake_audit)
    accounts = [{"phone": "1"}, {"phone": "b"}, {"phone": "2"}]

    # No capacity -> zero clients created, everything reported as skipped.
    assert await main_bot._auditor_run_pass(accounts, 0) == (0, 0, 3)
    assert ca == []

    # Bounded concurrency path with mixed outcomes.
    ok, failed, skipped = await main_bot._auditor_run_pass(accounts, 2)
    assert ok == 2 and failed == 1 and skipped == 0
    assert len(ca) == 3


# ---------------------------------------------------------------------------
# RECOVERY — 7 tests
# ---------------------------------------------------------------------------

def test_should_start_recovery_default_true(monkeypatch):
    monkeypatch.setattr(main_bot, "CONFIG", {})
    assert main_bot.should_start_recovery() is True


def test_should_start_recovery_disabled(monkeypatch):
    monkeypatch.setattr(main_bot, "CONFIG", {"ENABLE_AUTO_RECOVERY": False})
    assert main_bot.should_start_recovery() is False


@pytest.mark.asyncio
async def test_recover_skips_terminal_account(patch_deps):
    acc = {"phone": "+919999999991", "status": "auth_key_duplicated",
           "session_string": "SESSTOKEN"}
    assert await main_bot._recover_failed_accounts([acc]) == 0
    assert patch_deps["sm"].owned_calls == []


@pytest.mark.asyncio
async def test_recover_skips_busy_account(patch_deps):
    patch_deps["alm"].busy = {"919999999992"}
    acc = {"phone": "+919999999992", "status": "failed",
           "session_string": "SESSTOKEN"}
    assert await main_bot._recover_failed_accounts([acc]) == 0
    assert patch_deps["sm"].acquire_calls == []


@pytest.mark.asyncio
async def test_recover_skips_owned_session(patch_deps):
    patch_deps["sm"].is_owned_result = True
    acc = {"phone": "+919999999993", "status": "failed",
           "session_string": "SESSTOKEN"}
    assert await main_bot._recover_failed_accounts([acc]) == 0
    assert patch_deps["sm"].acquire_calls == []


@pytest.mark.asyncio
async def test_recover_recovers_via_managed_client(patch_deps):
    client = FakeClient()
    patch_deps["sm"].acquire_result = client
    acc = {"phone": "+919999999994", "status": "failed",
           "session_string": "SESSTOKEN"}
    recovered = await main_bot._recover_failed_accounts([acc])
    assert recovered == 1
    assert patch_deps["sm"].acquire_calls[0][0] == "919999999994"
    assert patch_deps["sm"].acquire_calls[0][1] == "managed_client"
    assert client.disconnect_calls == 0
    assert patch_deps["db"].statuses.get("919999999994") == \
        main_bot.AccountStatus.ACTIVE


@pytest.mark.asyncio
async def test_recover_skips_owned_at_acquire(patch_deps, monkeypatch):
    class RaiseCM:
        async def __aenter__(self):
            raise SessionAlreadyOwnedError("busy")

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(main_bot, "managed_client", lambda acc: RaiseCM())
    acc = {"phone": "+919999999995", "status": "failed",
           "session_string": "SESSTOKEN"}
    assert await main_bot._recover_failed_accounts([acc]) == 0


# ---------------------------------------------------------------------------
# LOGIN LIFECYCLE — 8 tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_managed_client_raises_when_no_lease(patch_deps):
    patch_deps["sm"].acquire_result = None
    with pytest.raises(ConnectionError):
        async with main_bot.managed_client({"phone": "+919999999999"}):
            pass
    assert patch_deps["sm"].acquire_calls[0][1] == "managed_client"
    assert patch_deps["sm"].acquire_calls[0][2] == "managed_client"
    assert patch_deps["sm"].acquire_calls[0][3] is True


@pytest.mark.asyncio
async def test_managed_client_yields_lease_client(patch_deps):
    client = FakeClient()
    patch_deps["sm"].acquire_result = client
    async with main_bot.managed_client({"phone": "+919999999998"}) as c:
        assert c is client
    assert patch_deps["sm"].acquire_calls[0][1] == "managed_client"
    assert patch_deps["sm"].acquire_calls[0][2] == "managed_client"
    assert patch_deps["sm"].acquire_calls[0][3] is True


@pytest.mark.asyncio
async def test_shared_login_process_code_sent(patch_deps, monkeypatch):
    client = FakeClient()
    client._connection = SimpleNamespace(
        _proxy={"addr": "p1", "port": 1080})
    patch_deps["sm"].build_result = client
    monkeypatch.setattr(main_bot, "DEVICE_PROFILES", [])

    res = await main_bot.shared_login_process(
        "+919999999990", "login:919999999990")
    assert res["status"] == "code_sent"
    assert res["code_hash"] == "hash123"
    assert res["client"] is client
    assert res["proxy_used"] == "p1:1080"
    phone, owner, kwargs = patch_deps["sm"].build_calls[0]
    assert owner == "login:919999999990"
    assert kwargs["proxy"] is None
    assert len(patch_deps["db"].pending) == 1
    assert patch_deps["sm"].release_calls == []


@pytest.mark.asyncio
async def test_shared_login_process_rotates_proxy_on_failure(patch_deps,
                                                             monkeypatch):
    client = FakeClient(fail_attempts=1)
    patch_deps["sm"].build_result = client
    monkeypatch.setattr(main_bot, "DEVICE_PROFILES", [])

    res = await main_bot.shared_login_process(
        "+919999999989", "login:919999999989")
    assert res["status"] == "code_sent"
    assert len(patch_deps["sm"].build_calls) == 2     # second proxy attempt
    assert len(patch_deps["sm"].release_calls) == 1   # failed lease released
    assert len(patch_deps["sm"].reserve_calls) == 1   # re-reserved before retry
    assert client.disconnect_calls == 0               # never disconnected directly


@pytest.mark.asyncio
async def test_shared_login_process_releases_on_exhaustion(patch_deps,
                                                           monkeypatch):
    client = FakeClient(fail_attempts=999)
    patch_deps["sm"].build_result = client
    monkeypatch.setattr(main_bot, "DEVICE_PROFILES", [])

    with pytest.raises(Exception) as e:
        await main_bot.shared_login_process(
            "+919999999988", "login:919999999988")
    assert "Proxy Connection Failed" in str(e.value)
    assert len(patch_deps["sm"].build_calls) == 4
    assert len(patch_deps["sm"].release_calls) == 4
    assert len(patch_deps["sm"].reserve_calls) == 3   # no re-reserve after last
    assert client.disconnect_calls == 0


def test_ensure_otp_listener_idempotent():
    client = FakeOtpClient()
    main_bot.ensure_otp_listener(client, "9123456789")
    main_bot.ensure_otp_listener(client, "9123456789")
    assert len(client.handlers) == 1


@pytest.mark.asyncio
async def test_otp_handler_captures_message(patch_deps):
    client = FakeOtpClient()
    main_bot.ensure_otp_listener(client, "9123456789")
    await client.handlers[0](FakeEvent("Your code is 123456"))
    assert patch_deps["db"].otp_logs[-1][2] == "Your code is 123456"


@pytest.mark.asyncio
async def test_fetch_past_otps_logs_and_handles_errors(patch_deps):
    class Getter(FakeClient):
        def __init__(self, exc=False):
            super().__init__()
            self.exc = exc

        async def get_messages(self, entity, limit=None):
            if self.exc:
                raise ConnectionError("no network")
            return [
                SimpleNamespace(message="OLD-OTP-1"),
                SimpleNamespace(message="OLD-OTP-2"),
                SimpleNamespace(message=None),
            ]

    await main_bot.fetch_past_otps(Getter(), "9123456789")
    assert len(patch_deps["db"].otp_logs) == 2
    await main_bot.fetch_past_otps(Getter(exc=True), "9123456789")
    assert len(patch_deps["db"].otp_logs) == 2  # no crash, nothing logged


# ---------------------------------------------------------------------------
# BACKGROUND TASKS & LIFESPAN — 5 tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bounded_restarts_restarts_on_error():
    calls = []

    async def factory():
        calls.append(1)
        if len(calls) < 2:
            raise RuntimeError("boom")
        await asyncio.Future()  # keep alive forever

    task = asyncio.create_task(main_bot._run_with_bounded_restarts(
        "demo", factory, max_restarts=3, base_delay=0.01))
    await asyncio.sleep(0.05)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_bounded_restarts_stops_at_limit():
    calls = []

    async def factory():
        calls.append(1)
        raise RuntimeError("always failing")

    await main_bot._run_with_bounded_restarts(
        "demo", factory, max_restarts=2, base_delay=0.01)
    assert len(calls) == 3  # attempts 1,2,3 (3 > max_restarts=2 -> stop)


@pytest.mark.asyncio
async def test_bounded_restarts_propagates_cancel():
    async def factory():
        await asyncio.Future()

    task = asyncio.create_task(main_bot._run_with_bounded_restarts(
        "demo", factory, max_restarts=3, base_delay=0.01))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cancel_tracked_background_tasks():
    await main_bot._cancel_tracked_background_tasks()  # empty: no-op

    async def block():
        await asyncio.sleep(3600)

    t = asyncio.create_task(block())
    main_bot.GLOBAL.background_tasks.add(t)
    await main_bot._cancel_tracked_background_tasks()
    assert t.cancelled()


@pytest.mark.asyncio
async def test_lifespan_shutdown_ordered_and_idempotent(monkeypatch):
    events = []

    class FakeRealBot:
        def __init__(self):
            self.disconnect_calls = 0

        async def start(self, bot_token=None):
            events.append("bot_start")

        async def disconnect(self):
            self.disconnect_calls += 1
            events.append("bot_disconnect")

    class FakeBot:
        def initialize(self, ss, api_id, api_hash):
            events.append("bot_initialize")
            return FakeRealBot()

    class FakeProxyMg:
        def start_background_testing(self):
            events.append("proxy_start_test")

        def stop_background_testing(self):
            events.append("proxy_stop_test")

    class FakeEngine:
        is_running = False

    plm = FakePLM(events=events)
    alm = FakeALM(events=events)
    db = FakeDB(events=events)
    bot = FakeBot()
    sm = FakeSM(events=events)

    monkeypatch.setattr(main_bot, "bot", bot)
    monkeypatch.setattr(main_bot, "proxy_manager", FakeProxyMg())
    monkeypatch.setattr(main_bot, "proxy_lease_manager", plm)
    monkeypatch.setattr(main_bot, "account_lease_manager", alm)
    monkeypatch.setattr(main_bot, "db", db)
    monkeypatch.setattr(main_bot, "adder_engine", FakeEngine())
    monkeypatch.setattr(main_bot, "dm_engine", FakeEngine())
    monkeypatch.setattr(main_bot, "voice_engine", FakeEngine())
    monkeypatch.setattr(main_bot, "shutdown_background_tasks",
                        lambda: events.append("shutdown_bg"))
    monkeypatch.setattr(main_bot, "session_manager", sm)
    monkeypatch.setattr(main_bot, "CONFIG", {
        "API_ID": 1, "API_HASH": "a", "BOT_TOKEN": "tok",
        "AUDITOR_ENABLED": False, "ENABLE_AUTO_RECOVERY": False,
    })

    main_bot._shutdown_done = False

    # --- run #1: full ordered shutdown ---
    gen = main_bot.lifespan(None)
    await gen.__anext__()          # startup phase
    assert "bot_initialize" in events
    assert "plm_start" in events
    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()      # shutdown phase
    assert events.index("disconnect_all") < events.index("plm_stop")
    assert events.index("plm_stop") < events.index("alm_stop")
    assert events.index("alm_stop") < events.index("db_close")
    assert events.index("db_close") < events.index("bot_disconnect")
    assert db.close_calls == 1
    assert sm.disconnect_all_calls == 1
    assert main_bot._shutdown_done is True

    # --- guard: shutdown already completed -> steps skipped (idempotent) ---
    main_bot._shutdown_done = True     # simulate a completed shutdown
    gen2 = main_bot.lifespan(None)
    await gen2.__anext__()             # startup resets the flag
    main_bot._shutdown_done = True     # re-arm the guard
    before = len(events)
    with pytest.raises(StopAsyncIteration):
        await gen2.__anext__()         # guard fires: early return
    assert len(events) == before       # no extra shutdown side effects
    assert db.close_calls == 1
    assert sm.disconnect_all_calls == 1


# ---------------------------------------------------------------------------
# GLOBALSTATE — 2 tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_set_auth_state_replaces_without_disconnect():
    old = main_bot.AuthState(client=FakeClient(), phone_code_hash="a",
                             device={}, created_at=time.time() - 1)
    new = main_bot.AuthState(client=FakeClient(), phone_code_hash="b",
                             device={}, created_at=time.time())
    await main_bot.GLOBAL.set_auth_state("9123456789", old)
    await main_bot.GLOBAL.set_auth_state("9123456789", new)
    assert main_bot.GLOBAL.auth_states["9123456789"] is new
    assert old.client.disconnect_calls == 0  # GlobalState never disconnects


@pytest.mark.asyncio
async def test_cleanup_stale_auth_states_releases_login(patch_deps):
    fresh = main_bot.AuthState(client=FakeClient(), phone_code_hash="fresh",
                               device={}, created_at=time.time())
    stale = main_bot.AuthState(client=FakeClient(), phone_code_hash="stale",
                               device={}, created_at=time.time() - 9999)
    await main_bot.GLOBAL.set_auth_state("9123456789", fresh)
    await main_bot.GLOBAL.set_auth_state("9876543210", stale)
    removed = await main_bot.GLOBAL.cleanup_stale_auth_states()
    assert removed == 1
    assert "9123456789" in main_bot.GLOBAL.auth_states
    assert "9876543210" not in main_bot.GLOBAL.auth_states
    assert ("9876543210", "login:9876543210") in patch_deps["sm"].release_calls
    assert stale.client.disconnect_calls == 0


# ---------------------------------------------------------------------------
# SOURCE AUDIT + AUTH CHECK — 2 tests
# ---------------------------------------------------------------------------

def test_source_audit_no_private_access_or_client_pool():
    with io.open(os.path.join(ROOT, "main_bot.py"), encoding="utf-8") as f:
        src = f.read()
    for forbidden in [
        "use_pool", "client_pool", "ClientPoolEntry", "_pool_lock",
        "session_manager._", "release_lease(", "create_authenticated_client",
        "_no_proxy", "proxy_lease_manager.release_proxy(",
    ]:
        assert forbidden not in src, f"forbidden token present: {forbidden!r}"
    # 2FA path: password not persisted, no disconnect/finally cleanup.
    marker = "async def verify_2fa_handler"
    start = src.index(marker)
    end = src.find("\n\nasync def", start)
    body = src[start:end] if end != -1 else src[start:]
    assert "two_fa_password=None" in body
    assert "two_fa_password=password" not in body
    assert "client.disconnect(" not in body
    assert "finally:" not in body


@pytest.mark.asyncio
async def test_check_session_authorization_status_matrix(monkeypatch):
    class AuthClient:
        def __init__(self, connected=True, authorized=True, exc=None):
            self.connected = connected
            self.authorized = authorized
            self.exc = exc

        def is_connected(self):
            return self.connected

        async def is_user_authorized(self):
            if self.exc is not None:
                raise self.exc
            return self.authorized

    cases = [
        (AuthClient(connected=False), "disconnected"),
        (AuthClient(authorized=True), "authorized"),
        (AuthClient(authorized=False), "unauthorized"),
        (AuthClient(exc=AuthKeyUnregisteredError("x")), "revoked"),
        (AuthClient(exc=asyncio.TimeoutError()), "timeout"),
        (AuthClient(exc=OSError("net")), "connection_error"),
        (AuthClient(exc=ValueError("weird")), "unknown"),
    ]
    for client, expected in cases:
        ok, status = await main_bot.check_session_authorization(client)
        assert status == expected, expected
        assert ok is (status == "authorized")


# ---------------------------------------------------------------------------
# IMPORT — 1 test
# ---------------------------------------------------------------------------

def test_main_bot_imports_without_live_mongo():
    assert hasattr(main_bot, "managed_client")
    assert hasattr(main_bot, "continuous_session_auditor")
    assert hasattr(main_bot, "_recover_failed_accounts")
    assert hasattr(main_bot, "lifespan")
    assert main_bot.db is not None