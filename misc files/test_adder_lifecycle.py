"""
Adder lifecycle test suite — context-manager correctness for account_context.

Verifies that the runtime Adder:
  * consumes account_context via ``async with ... as worker_account``
  * never awaits account_context() directly
  * performs SessionManager acquire/release ONLY through the context manager
  * keeps the context alive for the complete worker operation
  * releases resources on every failure path and on cancellation
  * never calls SessionManager.release_lease / _release_lease / release manually

Usage: python -m pytest test_adder_lifecycle.py -v
"""
import asyncio
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest

import adder as adder_module
from adder import EnterpriseMemberAdder
from session_manager import SessionAlreadyOwnedError

pytest_plugins = ["pytest_asyncio"]

ACCOUNT = {
    "phone": "+15550000001",
    "status": "active",
    "session_string": "session_15550000001",
    "api_id": 12345,
    "api_hash": "abc123",
    "device_metadata": {
        "device_model": "PC 64bit",
        "system_version": "Windows 11",
        "app_version": "5.1.0",
    },
}

MEMBER = {"username": "alice", "user_id": "1", "access_hash": "0"}


def make_phone(n):
    return f"+1555000{n:04d}"


def make_lease(client, phone=ACCOUNT["phone"]):
    return SimpleNamespace(
        phone=str(phone),
        client=client,
        proxy_url="socks5://proxy-node:1080",
        proxy_id="proxy-1",
        proxy_lease_id="lease-1",
        owner="adder",
        worker_id=f"adder:{phone}",
        lease_id="L1",
        released=False,
    )


def good_client():
    """A client that connects, authorizes, resolves the target, and invites."""
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=False)
    client.connect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=True)
    client.get_entity = AsyncMock(
        return_value=SimpleNamespace(id=987, access_hash=654321)
    )
    client.get_input_entity = AsyncMock(
        return_value=SimpleNamespace(id=1, access_hash=0)
    )
    return client


class FakeDB:
    def __init__(self):
        self.accounts = []
        self.members = []
        self.pages = []
        self.added = []
        self.failed = []
        self.revoked = []

    async def fetch_unprocessed_scraped_pool(self):
        return list(self.members)

    async def get_active_target_sessions(self):
        return list(self.accounts)

    async def fetch_unprocessed_scraped_pool_paginated(self, skip, limit):
        if not self.pages:
            return []
        page, *rest = self.pages
        self.pages = rest
        return page

    def log_addition_state(self, user_id, username, outcome):
        self.added.append((user_id, username, outcome))

    def mark_account_failed(self, phone, reason=""):
        self.failed.append((phone, reason))

    def mark_account_revoked(self, phone, reason=""):
        self.revoked.append((phone, reason))


class AcquireRecorder:
    """
    A proper @asynccontextmanager mock of SessionManager.acquire.

    Records enter/exit so tests can prove the whole worker lifecycle is
    wrapped by ``async with self.session_manager.acquire(...)``.
    """

    def __init__(self, lease, error=None):
        self.lease = lease
        self.error = error
        self.enter_count = 0
        self.exit_count = 0
        self.args = []

    @asynccontextmanager
    async def __call__(self, *args, **kwargs):
        self.enter_count += 1
        self.args.append((args, kwargs))
        if self.error is not None:
            raise self.error
        try:
            yield self.lease
        finally:
            self.exit_count += 1


def build_adder(db, recorder):
    session_manager = MagicMock()
    session_manager.acquire = recorder
    adder = EnterpriseMemberAdder(db=db, session_manager=session_manager)
    return session_manager, adder


async def run_pipeline(adder):
    return await adder.execute_adding_pipeline(
        "@target_group",
        update_callback=AsyncMock(),
        adder_state=None,
    )


@pytest.fixture(autouse=True)
def _fast_config():
    with patch.dict(
        adder_module.CONFIG,
        {
            "ADDER_MAX_WORKER_SESSIONS": 4,
            "ADDER_HUMAN_ADD_INTERVAL": (0.0, 0.001),
            "ADDER_BURST_ADD_LIMIT": 100000,
            "ADDER_BURST_COOLDOWN_TIME": (0.0, 0.001),
            "ADDER_PROGRESS_UPDATE_INTERVAL": 100000,
        },
    ):
        yield


# ---------------------------------------------------------------------------
# Runtime: context manager lifecycle
# ---------------------------------------------------------------------------

class TestContextManagerLifecycle:

    @pytest.mark.asyncio
    async def test_context_manager_enters_exits(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = []  # empty member queue -> worker still enters account_context

        client = good_client()
        recorder = AcquireRecorder(make_lease(client))
        _sm, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert recorder.enter_count == 1
        assert recorder.exit_count == 1
        client.connect.assert_awaited_once()
        client.is_user_authorized.assert_awaited_once()
        assert adder.accounts_down == 0

    @pytest.mark.asyncio
    async def test_context_exit_releases_resources(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = [[MEMBER]]

        client = good_client()
        recorder = AcquireRecorder(make_lease(client))
        session_manager, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert recorder.exit_count == 1
        assert adder.total_added == 1
        assert db.added[-1] == ("1", "alice", "success_added")
        # Release went through the acquire context manager, never manual API.
        session_manager.release_lease.assert_not_called()
        session_manager._release_lease.assert_not_called()
        session_manager.release.assert_not_called()


# ---------------------------------------------------------------------------
# Runtime: successful account lifecycle
# ---------------------------------------------------------------------------

class TestSuccess:

    @pytest.mark.asyncio
    async def test_successful_account_lifecycle(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = [[MEMBER]]

        client = good_client()
        recorder = AcquireRecorder(make_lease(client))
        _sm, adder = build_adder(db, recorder)

        result = await run_pipeline(adder)

        assert adder.total_added == 1
        assert adder.accounts_down == 0
        assert db.added[-1][2] == "success_added"
        assert "Completed Successfully" in result
        # Lifecycle: connect -> authorize -> target prep -> member op -> exit.
        client.connect.assert_awaited_once()
        client.is_user_authorized.assert_awaited_once()
        client.get_input_entity.assert_awaited_once_with("alice")
        assert recorder.enter_count == 1
        assert recorder.exit_count == 1


# ---------------------------------------------------------------------------
# Runtime: failure paths
# ---------------------------------------------------------------------------

class TestFailurePaths:

    @pytest.mark.asyncio
    async def test_client_connect_failure(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = []

        client = good_client()
        client.is_connected = MagicMock(return_value=False)
        client.connect = AsyncMock(side_effect=OSError("connection refused"))
        recorder = AcquireRecorder(make_lease(client))
        _sm, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert adder.accounts_down == 1
        client.connect.assert_awaited_once()
        # Even on failure the acquire context manager released the lease.
        assert recorder.enter_count == 1
        assert recorder.exit_count == 1

    @pytest.mark.asyncio
    async def test_authorization_failure(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = []

        client = good_client()
        client.is_connected = MagicMock(return_value=True)
        client.is_user_authorized = AsyncMock(return_value=False)
        recorder = AcquireRecorder(make_lease(client))
        _sm, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert adder.accounts_down == 1
        client.is_user_authorized.assert_awaited_once()
        assert recorder.exit_count == 1

    @pytest.mark.asyncio
    async def test_target_preparation_failure(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = []

        client = good_client()
        client.is_connected = MagicMock(return_value=True)
        client.is_user_authorized = AsyncMock(return_value=True)
        calls = []

        async def _boom(request=None, *args, **kwargs):
            calls.append(request)
            raise PermissionError("join failed")

        client.side_effect = _boom
        client.get_entity = AsyncMock(side_effect=ValueError("entity not found"))
        recorder = AcquireRecorder(make_lease(client))
        _sm, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert adder.accounts_down == 1
        assert calls and isinstance(calls[0], JoinChannelRequest)
        assert recorder.exit_count == 1

    @pytest.mark.asyncio
    async def test_terminal_account_lease_none(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = []

        recorder = AcquireRecorder(None)
        _sm, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert adder.accounts_down == 1
        assert recorder.enter_count == 1
        assert recorder.exit_count == 1


# ---------------------------------------------------------------------------
# Runtime: SessionAlreadyOwnedError and cancellation
# ---------------------------------------------------------------------------

class TestOwnershipAndCancellation:

    @pytest.mark.asyncio
    async def test_session_already_owned_error(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = []

        recorder = AcquireRecorder(
            make_lease(good_client()),
            error=SessionAlreadyOwnedError("already owned by another worker"),
        )
        _sm, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert adder.accounts_down == 1
        assert recorder.enter_count == 1
        # __aenter__ raised before the yield -> no body, no exit.
        assert recorder.exit_count == 0

    @pytest.mark.asyncio
    async def test_worker_cancellation_releases_via_context(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = [[MEMBER]]

        client = good_client()
        started = []
        hold = asyncio.Event()

        async def _fake_call(request=None, *args, **kwargs):
            if isinstance(request, JoinChannelRequest):
                return None
            if isinstance(request, InviteToChannelRequest):
                started.append(1)
                await hold.wait()
                return None
            return None

        client.side_effect = _fake_call
        recorder = AcquireRecorder(make_lease(client))
        session_manager, adder = build_adder(db, recorder)

        task = asyncio.create_task(run_pipeline(adder))
        deadline = time.monotonic() + 5
        while not started and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert started, "worker never reached the invite stage"

        task.cancel()
        try:
            result = await asyncio.wait_for(task, timeout=5)
        except asyncio.CancelledError:
            result = None

        # Cancellation cleaned up through the context manager, not a manual call.
        assert isinstance(result, str)
        assert recorder.exit_count == 1
        session_manager.release_lease.assert_not_called()
        session_manager._release_lease.assert_not_called()


# ---------------------------------------------------------------------------
# Static contract: bad patterns must never appear in Adder runtime
# ---------------------------------------------------------------------------

class TestStaticContract:

    def _src(self):
        return Path(adder_module.__file__).read_text(encoding="utf-8")

    def test_account_context_consumed_exclusively_via_async_with(self):
        src = self._src()
        assert src.count("account_context(") == 2
        assert "async with account_context(account_doc) as worker_account:" in src
        assert "await account_context(" not in src
        assert "await self.account_context(" not in src

    def test_acquire_is_used_as_context_manager_only(self):
        src = self._src()
        assert "async with self.session_manager.acquire(" in src
        assert "await self.session_manager.acquire(" not in src

    def test_no_manual_session_manager_release(self):
        src = self._src()
        assert "release_lease(" not in src
        assert "._release_lease(" not in src
        assert "await self.session_manager.release(" not in src

    def test_no_direct_client_construction_or_locks(self):
        src = self._src()
        assert "TelegramClient(" not in src
        assert "_create_client(" not in src
        assert "._create_client(" not in src
        assert "acquire_lock(" not in src
        assert "release_lock(" not in src
        assert "is_locked(" not in src

    def test_worker_account_never_used_after_context_exit(self):
        src = self._src()
        head = src.partition(
            "async with account_context(account_doc) as worker_account:"
        )
        assert head[1], "account_context async-with block not found"
        _, _, tail = head[2].partition(
            "# 🔥 FIX: Launch workers concurrently"
        )
        # Everything after the worker_loop definition must not touch
        # worker_account / lease resources handed out by the context.
        assert "worker_account" not in tail
        assert '"lease"' not in tail

    @pytest.mark.asyncio
    async def test_runtime_never_releases_session_manually(self):
        db = FakeDB()
        db.accounts = [ACCOUNT]
        db.members = [MEMBER]
        db.pages = [[MEMBER]]

        client = good_client()
        recorder = AcquireRecorder(make_lease(client))
        session_manager, adder = build_adder(db, recorder)

        await run_pipeline(adder)

        assert adder.total_added == 1
        assert recorder.exit_count == 1
        assert session_manager.release_lease.call_count == 0
        assert session_manager._release_lease.call_count == 0
        assert session_manager.release.call_count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])