import pytest

from proxy_manager import ProxyLeaseManager


class FakeProxyManager:
    def __init__(self, proxies):
        self.working_proxies = list(proxies)
        self.provider = type("Provider", (), {"name": "test"})()


def _proxy(proxy_id, url="socks5://proxy.example:1080"):
    return {
        "proxy_id": proxy_id,
        "url": url,
        "host": "proxy.example",
        "addr": "proxy.example",
        "port": 1080,
        "type": "socks5",
    }


async def _manager(proxies):
    manager = ProxyLeaseManager(FakeProxyManager(proxies))
    await manager.start()
    return manager


async def _stop(manager):
    await manager.stop()


@pytest.mark.asyncio
async def test_proxy_lease_has_unique_lease_id():
    manager = await _manager([_proxy("node-1")])
    try:
        lease = await manager.acquire_proxy("account-a")
        assert lease is not None
        assert lease["__lease_id"]
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_correct_owner_release_frees_node_and_counter():
    manager = await _manager([_proxy("node-1")])
    try:
        lease = await manager.acquire_proxy("account-a")
        assert lease is not None
        proxy_url = manager.proxy_nodes[lease["__proxy_id"]].url

        await manager.release_proxy(
            proxy_url=proxy_url,
            proxy_id=lease["__proxy_id"],
            lease_id=lease["__lease_id"],
            phone="account-a",
        )

        node = manager.proxy_nodes[lease["__proxy_id"]]
        assert not node.is_leased
        assert node.leased_to is None
        assert manager.stats["current_active_leases"] == 0
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_all_proxies_busy_timeout_returns_none_counter_stable():
    manager = await _manager([_proxy("node-1")])
    try:
        first = await manager.acquire_proxy("account-a", timeout=1.0)
        assert first is not None

        node = manager.proxy_nodes[first["__proxy_id"]]
        # Every available node is leased -> a new acquire must WAIT, then time
        # out and return None without touching counters or the existing lease.
        result = await manager.acquire_proxy("account-b", timeout=0.2)
        assert result is None

        assert manager.stats["current_active_leases"] == 1
        assert node.is_leased
        assert node.leased_to == "account-a"
        assert node.lease_id == first["__lease_id"]
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_proxy_lease_double_release_keeps_active_count_zero():
    manager = await _manager([_proxy("node-1")])
    try:
        lease = await manager.acquire_proxy("account-a")
        assert lease is not None
        proxy_url = manager.proxy_nodes[lease["__proxy_id"]].url

        await manager.release_proxy(
            proxy_url=proxy_url,
            proxy_id=lease["__proxy_id"],
            lease_id=lease["__lease_id"],
            phone="account-a",
        )
        await manager.release_proxy(
            proxy_url=proxy_url,
            proxy_id=lease["__proxy_id"],
            lease_id=lease["__lease_id"],
            phone="account-a",
        )

        assert manager.stats["current_active_leases"] == 0
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_proxy_lease_wrong_account_cannot_release():
    manager = await _manager([_proxy("node-1")])
    try:
        lease = await manager.acquire_proxy("account-a")
        assert lease is not None
        proxy_url = manager.proxy_nodes[lease["__proxy_id"]].url

        await manager.release_proxy(
            proxy_url=proxy_url,
            proxy_id=lease["__proxy_id"],
            lease_id=lease["__lease_id"],
            phone="account-b",
        )

        node = manager.proxy_nodes[lease["__proxy_id"]]
        assert node.is_leased
        assert node.leased_to == "account-a"
        assert manager.stats["current_active_leases"] == 1
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_proxy_lease_stale_epoch_cannot_release_new_lease():
    manager = await _manager([_proxy("node-1")])
    try:
        first = await manager.acquire_proxy("account-a")
        assert first is not None
        proxy_url = manager.proxy_nodes[first["__proxy_id"]].url
        await manager.release_proxy(
            proxy_url=proxy_url,
            proxy_id=first["__proxy_id"],
            lease_id=first["__lease_id"],
            phone="account-a",
        )

        second = await manager.acquire_proxy("account-a")
        assert second is not None
        assert second["__lease_id"] != first["__lease_id"]

        await manager.release_proxy(
            proxy_url=proxy_url,
            proxy_id=second["__proxy_id"],
            lease_id=first["__lease_id"],
            phone="account-a",
        )

        node = manager.proxy_nodes[second["__proxy_id"]]
        assert node.is_leased
        assert node.lease_id == second["__lease_id"]
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_leased_proxy_survives_working_registry_removal():
    proxy = _proxy("node-1")
    manager = await _manager([proxy])
    try:
        lease = await manager.acquire_proxy("account-a")
        assert lease is not None
        proxy_url = manager.proxy_nodes[lease["__proxy_id"]].url

        manager.proxy_manager.working_proxies.clear()
        await manager._sync_proxies()
        assert lease["__proxy_id"] in manager.proxy_nodes

        await manager.release_proxy(
            proxy_url=proxy_url,
            proxy_id=lease["__proxy_id"],
            lease_id=lease["__lease_id"],
            phone="account-a",
        )
        await manager._sync_proxies()
        assert lease["__proxy_id"] not in manager.proxy_nodes
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_unleased_proxy_removed_from_registry_when_working_registry_removes_it():
    proxy = _proxy("node-1")
    manager = await _manager([proxy])
    try:
        assert "node-1" in manager.proxy_nodes
        manager.proxy_manager.working_proxies.clear()
        await manager._sync_proxies()
        assert "node-1" not in manager.proxy_nodes
    finally:
        await _stop(manager)


@pytest.mark.asyncio
async def test_same_url_keeps_ten_logical_proxy_ids():
    manager = await _manager([_proxy(f"node-{index}") for index in range(10)])
    try:
        leases = [
            await manager.acquire_proxy(f"account-{index}")
            for index in range(10)
        ]

        assert all(leases)
        assert len({lease["__proxy_id"] for lease in leases if lease}) == 10
        assert len(manager.proxy_nodes) == 10
    finally:
        await _stop(manager)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))