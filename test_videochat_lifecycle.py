"""
VideoChat engine lifecycle + resource scheduling test suite (PATCH 7).

Verifies the refactored CloudVoiceChatEngine:
  1. never creates TelegramClient / touches SessionManager internals directly
  2. consumes every session exclusively via ``async with acquire(...)``
  3. never calls manual __aenter__/__aexit__, release_lease/_release_lease,
     release_proxy, client.disconnect or DB runtime locks
  4. keeps the session lease alive for the WHOLE active call and releases it
     on completion
  5. unwinds everything (client/session/proxy/account) on cancellation
  6. reports PyTgCalls start failure as FAILED with zero leaks
  7. reports a missing PyTgCalls engine as explicit VIDEO_ENGINE_UNAVAILABLE
     (never fakes a running call)
  8. terminate_voice_cluster() AWAITS teardown (no fire-and-forget shutdown)
  9. repeated start/stop returns every resource to baseline
 10. rejects concurrent start of the same account (no second client)
 11. quarantines AuthKeyDuplicated and never retries the same session
 12. 10 start/stop cycles show no monotonic task/resource growth

Usage: python -m pytest test_videochat_lifecycle.py -v
"""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from session_manager import SessionLifecycleState, SessionManager
from videochat import CloudVoiceChatEngine, VoiceClusterState

pytest_plugins = ["pytest_asyncio"]

SOURCE = Path(__file__).parent / "videochat.py"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeDB:
    def __init__(self):
        self.sessions = {}
        self.targets = []
        self.failed = []

    def _norm(self, p):
        return str(p).replace(" ", "").replace("+", "")

    def get_session_by_phone(self, p):
        return self.sessions.get(self._norm(p))

    async def get_session_by_phone_async(self, p):
        return self.sessions.get(self._norm(p))

    def update_session_status(self, p, s, r=None):
        pass

    def mark_account_failed(self, p, reason=""):
        self.failed.append((p, reason))

    def mark_account_revoked(self, p, reason=""):
        pass

    def get_all_active_sessions(self):
        return []

    async def get_active_target_sessions(self):
        return list(self.targets)


class FakeProxyManager:
    def get_proxy(self):
        return None


class FakeProxyLeaseManager:
    def __init__(self):
        self.counter = 0
        self.released = []

    async def acquire_proxy(self, phone, timeout=None):
        self.counter += 1
        return {
            "url": f"socks5://p{self.counter}:1080",
            "__proxy_id": f"p{self.counter}",
            "__lease_id": f"l{self.counter}",
            "addr": "p", "port": 1080, "username": "u", "password": "x",
        }

    async def release_proxy(self, *, proxy_url, phone, proxy_id=None, lease_id=None,
                            should_cooldown=False, cooldown_reason=""):
        self.released.append((phone, proxy_id, lease_id))

    def get_available_count(self):
        return 5


def mock_client(phone="t"):
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    me = SimpleNamespace(id=770001, first_name="VC")
    client.get_me = AsyncMock(return_value=me)
    client.is_user_authorized = AsyncMock(return_value=True)
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.session = MagicMock()
    client.session.save = MagicMock(return_value=f"ss_{phone}")
    client.on = MagicMock(side_effect=lambda evt: (lambda f: f))
    return client


class FakeApp:
    last_instance = None

    def __init__(self, client):
        self.client = client
        self.stops = 0
        FakeApp.last_instance = self

    async def start(self):
        pass

    async def play(self, chat_id, stream):
        pass

    async def change_volume(self, chat_id, vol):
        pass

    async def stop(self):
        self.stops += 1


class FailingStartApp(FakeApp):
    async def start(self):
        raise RuntimeError("pytgcalls start failed")


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def _make_phone(n):
    return f"+155599{n:05d}"


def _store(db, phone):
    norm = str(phone).replace("+", "")
    db.sessions[norm] = {
        "phone": norm,
        "status": "active",
        "session_string": f"session_{norm}",
        "api_id": 12345,
        "api_hash": "abc",
        "device_metadata": {"device_model": "PC", "system_version": "W11", "app_version": "5"},
    }


def build_engine(app_factory=None, engine_available=True):
    db = FakeDB()
    plm = FakeProxyLeaseManager()
    sm = SessionManager(db, FakeProxyManager(), plm, max_active_clients=10)
    sm._create_client = lambda **kw: mock_client(kw.get("session_str", "t"))
    eng = CloudVoiceChatEngine(db, FakeProxyManager(), plm, sm, None)
    eng._voice_engine_available = engine_available
    eng._app_factory = app_factory or FakeApp
    eng._sleep_scale = 0.0005
    eng._keepalive_max_cycles = 1

    async def fake_prep(phone, client, group_link):
        entity = SimpleNamespace(id=321, broadcast=True, megagroup=False)
        return entity, -100321

    eng._prepare_voice_session = fake_prep
    return db, plm, sm, eng


async def wait_active(eng, phone, task, tries=500):
    for _ in range(tries):
        if eng.voice_state(phone) == VoiceClusterState.ACTIVE:
            return True
        if task.done():
            return False
        await asyncio.sleep(0.005)
    return False


async def run_one(eng, phone, replacement_queue=None, audio="silent.mp3"):
    q = replacement_queue if replacement_queue is not None else asyncio.Queue()
    return await eng._execute_single_stream({"phone": phone}, "t.me/g", audio, q)


def assert_baseline(sm, plm, eng, phones=()):
    assert sm._active_count == 0
    assert eng._active_calls == {}
    assert eng._cluster_tasks == {}
    assert len(eng._active_tasks) == 0
    assert plm.counter == len(plm.released)
    assert eng.running_voice_clusters() == 0
    for p in phones:
        assert eng.voice_state(p) in (VoiceClusterState.STOPPED,
                                      VoiceClusterState.FAILED,
                                      VoiceClusterState.TERMINAL,
                                      VoiceClusterState.VIDEO_ENGINE_UNAVAILABLE)


# ---------------------------------------------------------------------------
# 1 + 2. Static ownership audit
# ---------------------------------------------------------------------------

class TestStaticOwnershipAudit:

    def test_no_direct_client_creation_no_manual_context_no_release(self):
        src = SOURCE.read_text(encoding="utf-8")
        for forbidden in (
            "TelegramClient(",
            ".__aenter__(",
            ".__aexit__(",
            "release_lease(",
            "_release_lease(",
            "release_proxy(",
            ".disconnect()",
            "acquire_lock",
            "release_lock",
            "is_locked",
        ):
            assert forbidden not in src, f"forbidden pattern present: {forbidden}"

    def test_no_global_disconnect_all_no_fire_and_forget_teardown(self):
        src = SOURCE.read_text(encoding="utf-8")
        assert "asyncio.create_task(_graceful_teardown())" not in src
        assert "session_manager.disconnect_all()" not in src
        tasks = [l.strip() for l in src.splitlines() if "asyncio.create_task(" in l]
        # Exactly three owned task-creations: the migration batch loop,
        # the replacement spawn, and the initial launch. Each spawned stream
        # task is strongly registered; none is a detached fire-and-forget.
        assert len(tasks) == 3, tasks
        spawned = [t for t in tasks if "self._execute_single_stream" in t]
        assert len(spawned) == 2
        # The migration loop's task list is awaited via gather.
        loop_task = [t for t in tasks if "process_single_account" in t]
        assert len(loop_task) == 1
        # Every spawned stream task is strongly registered -> awaitable teardown.
        assert "self._register_voice_task(" in src

    def test_every_session_acquisition_is_async_with(self):
        src = SOURCE.read_text(encoding="utf-8")
        acquire_lines = [l for l in src.splitlines() if "session_manager.acquire(" in l]
        assert len(acquire_lines) == 3, acquire_lines  # audit + migration + stream
        for l in acquire_lines:
            assert "async with" in l, l
        assert "lease_context = self.session_manager.acquire(" not in src


# ---------------------------------------------------------------------------
# 3. Runtime: acquire consumed via async-with
# ---------------------------------------------------------------------------

class TestAcquireDiscipline:

    @pytest.mark.asyncio
    async def test_runtime_acquire_is_context_manager_only(self):
        db, plm, sm, eng = build_engine()
        phone = _make_phone(1)
        _store(db, phone)
        eng.is_running = True

        counters = {"entered": 0, "exited": 0, "kwargs": None}
        original = sm.acquire

        @asynccontextmanager
        async def counting(*args, **kwargs):
            counters["entered"] += 1
            counters["kwargs"] = kwargs
            async with original(*args, **kwargs) as lease:
                yield lease
            counters["exited"] += 1

        sm.acquire = counting
        outcome = await run_one(eng, phone)
        assert outcome == "STOPPED"
        assert counters["entered"] == 1
        assert counters["exited"] == 1
        assert counters["kwargs"] is not None
        assert counters["kwargs"]["module"] == "videochat_stream"
        assert counters["kwargs"]["auto_release"] is True
        assert counters["kwargs"]["worker_id"] == f"vc:{phone}"
        sm.acquire = original


# ---------------------------------------------------------------------------
# 4. Session owned during active call, released afterwards
# ---------------------------------------------------------------------------

class TestLeaseLifetime:

    @pytest.mark.asyncio
    async def test_session_owned_during_active_call_released_after(self):
        db, plm, sm, eng = build_engine()
        phone = _make_phone(2)
        _store(db, phone)
        eng.is_running = True

        task = asyncio.create_task(
            eng._execute_single_stream({"phone": phone}, "t.me/g", "silent.mp3", asyncio.Queue())
        )
        assert await wait_active(eng, phone, task)
        assert sm._active_count == 1, "session must remain owned during ACTIVE"
        info = sm._sessions[phone.replace("+", "")]
        assert info.lifecycle == SessionLifecycleState.BUSY
        assert info.worker_id == f"vc:{phone}"
        assert eng.running_voice_clusters() == 1

        outcome = await asyncio.wait_for(task, timeout=15)
        assert outcome == "STOPPED"
        assert_baseline(sm, plm, eng, phones=(phone,))


# ---------------------------------------------------------------------------
# 5. Cancellation unwinds everything
# ---------------------------------------------------------------------------

class TestCancellation:

    @pytest.mark.asyncio
    async def test_cancellation_releases_client_session_proxy_account(self):
        db, plm, sm, eng = build_engine()
        phone = _make_phone(3)
        _store(db, phone)
        eng.is_running = True

        task = asyncio.create_task(
            eng._execute_single_stream({"phone": phone}, "t.me/g", "silent.mp3", asyncio.Queue())
        )
        assert await wait_active(eng, phone, task)
        assert sm._active_count == 1
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=15)
        except asyncio.CancelledError:
            pass

        assert_baseline(sm, plm, eng, phones=(phone,))
        assert eng.voice_state(phone) == VoiceClusterState.STOPPED
        assert plm.counter == 1 and len(plm.released) == 1


# ---------------------------------------------------------------------------
# 6. PyTgCalls start failure -> FAILED, zero leaks
# ---------------------------------------------------------------------------

class TestPyTgCallsFailures:

    @pytest.mark.asyncio
    async def test_pytgcalls_start_failure_no_leak(self):
        db, plm, sm, eng = build_engine(app_factory=FailingStartApp)
        phone = _make_phone(4)
        _store(db, phone)
        eng.is_running = True

        outcome = await run_one(eng, phone)
        assert outcome.startswith("FAILED")
        assert eng.voice_state(phone) == VoiceClusterState.FAILED
        assert_baseline(sm, plm, eng, phones=(phone,))

    @pytest.mark.asyncio
    async def test_engine_unavailable_is_explicit_not_a_fake_success(self):
        db, plm, sm, eng = build_engine(engine_available=False)
        phone = _make_phone(5)
        _store(db, phone)
        eng.is_running = True

        outcome = await run_one(eng, phone)
        assert outcome == "VIDEO_ENGINE_UNAVAILABLE: PyTgCalls engine is not available on this host"
        assert eng.voice_state(phone) == VoiceClusterState.VIDEO_ENGINE_UNAVAILABLE
        # No session/proxy was ever acquired -> nothing to leak.
        assert sm._active_count == 0
        assert plm.counter == 0
        assert eng._active_calls == {}

        launch = await eng.launch_voice_cluster("t.me/g", "silent.mp3", desired_count=50)
        assert "VIDEO_ENGINE_UNAVAILABLE" in launch
        # launch returns early without spawning tasks or starting streams.
        assert eng._cluster_tasks == {}


# ---------------------------------------------------------------------------
# 7. Termination is awaited and deterministic
# ---------------------------------------------------------------------------

class TestTermination:

    @pytest.mark.asyncio
    async def test_terminate_awaits_cleanup_of_launched_cluster(self):
        db, plm, sm, eng = build_engine()
        p1, p2 = _make_phone(10), _make_phone(11)
        _store(db, p1)
        _store(db, p2)
        db.targets = [{"phone": p1}, {"phone": p2}]
        eng._launch_stagger = (0.0, 0.0)

        result = await eng.launch_voice_cluster("t.me/g", "silent.mp3", desired_count=50)
        assert "Active Matrix Initiated" in result

        for p in (p1, p2):
            task = eng._cluster_tasks.get(p)
            assert task is not None
            assert await wait_active(eng, p, task)
        assert sm._active_count == 2

        res = await eng.terminate_voice_cluster()
        assert res == "OK: cluster terminated."
        assert_baseline(sm, plm, eng, phones=(p1, p2))
        assert eng.is_running is False
        assert eng.voice_state(p1) == VoiceClusterState.STOPPED


# ---------------------------------------------------------------------------
# 8. Repeated start/stop returns to baseline
# ---------------------------------------------------------------------------

class TestRepeatedCycles:

    @pytest.mark.asyncio
    async def test_repeated_start_stop_returns_to_baseline(self):
        db, plm, sm, eng = build_engine()
        phone = _make_phone(20)
        _store(db, phone)
        eng.is_running = True

        for _ in range(3):
            outcome = await run_one(eng, phone)
            assert outcome == "STOPPED"
            assert_baseline(sm, plm, eng, phones=(phone,))

        assert plm.counter == len(plm.released) == 3


# ---------------------------------------------------------------------------
# 9. Concurrent start of the same account is rejected
# ---------------------------------------------------------------------------

class TestConcurrency:

    @pytest.mark.asyncio
    async def test_concurrent_start_same_account_rejected_no_second_client(self):
        db, plm, sm, eng = build_engine()
        phone = _make_phone(30)
        _store(db, phone)
        eng.is_running = True

        # Keep t1 deterministically ACTIVE across the whole assertion window:
        # block its keepalive change_volume (only invoked after ACTIVE) until
        # we are done probing. The blocking call never reaches an owner-write,
        # so it cannot be confused with the concurrent acquisition.
        release_hold = asyncio.Event()
        hold_client = mock_client(phone)

        class HoldApp(FakeApp):
            async def change_volume(self, chat_id, vol):
                await release_hold.wait()

        eng._app_factory = HoldApp

        # Count actual client fabrications: the point is that the rejected
        # concurrent start must NEVER create a second TelegramClient.
        client_creates = []

        def counting_client_factory(**kw):
            client_creates.append(kw.get("session_str", "t"))
            return mock_client(kw.get("session_str", "t"))

        sm._create_client = counting_client_factory

        t1 = asyncio.create_task(
            eng._execute_single_stream({"phone": phone}, "t.me/g", "silent.mp3", asyncio.Queue())
        )
        eng._register_voice_task(phone, t1)
        assert await wait_active(eng, phone, t1)
        assert sm._active_count == 1
        assert eng.running_voice_clusters() == 1
        assert len(client_creates) == 1

        t2 = asyncio.create_task(
            eng._execute_single_stream({"phone": phone}, "t.me/g", "silent.mp3", asyncio.Queue())
        )
        outcome2 = await asyncio.wait_for(t2, timeout=15)
        assert outcome2.startswith("FAILED")
        # Rejected at the engine ownership layer: no second TelegramClient was
        # fabricated, the cluster state was untouched, and the already-running
        # stream was not torn down.
        assert len(client_creates) == 1, client_creates
        assert eng.running_voice_clusters() == 1
        assert not t1.done(), "the running stream must survive the rejected start"

        release_hold.set()
        t1.cancel()
        try:
            await asyncio.wait_for(t1, timeout=15)
        except asyncio.CancelledError:
            pass
        assert_baseline(sm, plm, eng, phones=(phone,))


# ---------------------------------------------------------------------------
# 10. AuthKeyDuplicated -> quarantine, cleanup, no retry
# ---------------------------------------------------------------------------

class TestAuthKeyDuplicated:

    @pytest.mark.asyncio
    async def test_authkeyduplicated_quarantines_and_never_retries(self):
        from telethon.errors import AuthKeyDuplicatedError

        db, plm, sm, eng = build_engine()
        phone = _make_phone(40)
        _store(db, phone)
        eng.is_running = True

        # The leased client raises a duplicated-auth-key error mid session.
        async def boom_get_me():
            raise AuthKeyDuplicatedError(request=None)

        # Patch the client factory so the freshly leased client fails in get_me.
        def failing_client(phone="t"):
            c = mock_client(phone)
            c.get_me = AsyncMock(side_effect=boom_get_me)
            return c

        sm._create_client = lambda **kw: failing_client(kw.get("session_str", "t"))

        backup = {"phone": _make_phone(41)}
        q = asyncio.Queue()
        await q.put(backup)  # a queued replacement MUST NOT be spawned for terminal errors

        outcome = await run_one(eng, phone, replacement_queue=q)
        assert outcome.startswith("TERMINAL")
        assert eng.voice_state(phone) == VoiceClusterState.TERMINAL
        assert sm._sessions[phone.replace("+", "")].lifecycle == SessionLifecycleState.QUARANTINED
        assert db.failed and db.failed[-1][0] == phone.replace("+", "")
        # No retry loop: replacement queue untouched, no new cluster tasks.
        assert q.qsize() == 1
        assert eng._cluster_tasks == {}
        assert_baseline(sm, plm, eng, phones=(phone,))


# ---------------------------------------------------------------------------
# 11. Memory / task: 10 start-stop cycles, no monotonic growth
# ---------------------------------------------------------------------------

class TestMemoryTaskCycles:

    @pytest.mark.asyncio
    async def test_ten_start_stop_cycles_show_no_growth(self):
        db, plm, sm, eng = build_engine()
        phone = _make_phone(50)
        _store(db, phone)
        eng.is_running = True

        cycles = 10
        for i in range(cycles):
            outcome = await run_one(eng, phone)
            assert outcome == "STOPPED"
            assert sm._active_count == 0
            assert eng._active_calls == {}
            assert eng._cluster_tasks == {}
            assert len(eng._active_tasks) == 0
            assert plm.counter == len(plm.released) == (i + 1)
            assert len(eng._cluster_state) == 1  # state dict never grows per cycle
            assert eng.voice_state(phone) == VoiceClusterState.STOPPED

        # Baselines identical to a fresh engine -> no monotonic growth.
        assert sm._active_count == 0
        assert len(eng._active_tasks) == 0
        assert len(eng._cluster_tasks) == 0
        assert len(eng._active_calls) == 0
        assert plm.counter == cycles and len(plm.released) == cycles


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))