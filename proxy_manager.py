#!/usr/bin/env python3
"""
proxy_manager.py — Proxy management with rotation, health checking, and leasing.

Redesigned:
  - ONE internal representation: ProxyNode dataclass (no dict/dataclass mixing)
  - Provider abstraction: FileProxyProvider, DecodoProxyProvider, WebshareProxyProvider
  - All credentials from environment variables (never hardcoded in source)
  - ProxyLeaseManager uses ProxyNode objects consistently, exposes to_telethon_proxy()
  - Async-compatible lease acquire/release with Condition-based signaling
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Callable, Set
from urllib.parse import urlparse

import requests
import uuid
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("ProxyManager")

# ── Constants ──
PROXY_TEST_URL: str = "https://httpbin.org/ip"
PROXY_TEST_TIMEOUT: float = 5.0
PROXY_TEST_TIMEOUT_SLOW: float = 15.0
MAX_PROXY_FAILURES: int = 3
PROXY_ROTATION_WINDOW: float = 5 * 60
MIN_WORKING_PROXIES_TO_START: int = 1

TEST_BATCH_SIZE: int = 50
MAX_WORKERS_DEFAULT: int = 30
CONNECTION_POOL_SIZE: int = 20

PROXY_COOLDOWN_SECONDS: int = 600
ACCOUNT_COOLDOWN_SECONDS: int = 900
COOLDOWN_CHECK_INTERVAL: float = 5.0

# Provider selection from env
PROXY_PROVIDER: str = os.environ.get("PROXY_PROVIDER", "file").lower()


try:
    from colorama import Fore, Style
    G = Fore.GREEN
    Y = Fore.YELLOW
    R = Fore.RED
    RS = Style.RESET_ALL
except ImportError:
    G = Y = R = RS = ""


# ──────────────────────────────────────────────
# ProxyNode — single internal representation
# ──────────────────────────────────────────────


@dataclass
class ProxyNode:
    """
    One logical proxy resource.

    proxy_id identifies the logical node.
    lease_id identifies the current ownership epoch.
    leased_to identifies the current account owner.
    """

    proxy_id: str
    host: str
    port: int
    protocol: str = "socks5"
    username: Optional[str] = None
    password: Optional[str] = None
    url: str = ""
    latency: float = 5000.0
    health_state: str = "unknown"
    failure_count: int = 0

    is_leased: bool = False
    leased_to: Optional[str] = None
    lease_id: Optional[str] = None
    lease_time: float = 0.0

    cooldown_until: float = 0.0
    last_tested: float = 0.0
    provider: str = "file"
    added_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not self.url:
            self.url = self._build_url()

    def _build_url(self) -> str:
        if self.username and self.password:
            return (
                f"{self.protocol}://"
                f"{self.username}:{self.password}@"
                f"{self.host}:{self.port}"
            )

        return f"{self.protocol}://{self.host}:{self.port}"

    def to_telethon_proxy(self) -> Optional[Dict[str, Any]]:
        if not self.host or not self.port:
            return None

        return {
            "proxy_type": self.protocol,
            "addr": self.host,
            "port": self.port,
            "rdns": True,
            "username": self.username,
            "password": self.password,
        }

    def to_telethon_tuple(
        self,
    ) -> Optional[
        Tuple[
            str,
            str,
            int,
            bool,
            Optional[str],
            Optional[str],
        ]
    ]:
        if not self.host or not self.port:
            return None

        return (
            self.protocol,
            self.host,
            self.port,
            True,
            self.username,
            self.password,
        )

    def is_in_cooldown(self) -> bool:
        return time.time() < self.cooldown_until

    def acquire(
        self,
        phone: str,
        lease_id: str,
    ) -> bool:
        """
        Claim this proxy for exactly one ownership epoch.
        """
        if self.is_leased or self.is_in_cooldown():
            return False

        self.is_leased = True
        self.leased_to = phone
        self.lease_id = lease_id
        self.lease_time = time.time()

        return True

    def release(self) -> None:
        """
        Clear current ownership.
        """
        self.is_leased = False
        self.leased_to = None
        self.lease_id = None
        self.lease_time = 0.0

    def put_in_cooldown(
        self,
        duration_seconds: int = PROXY_COOLDOWN_SECONDS,
    ) -> None:
        self.release()

        self.cooldown_until = (
            time.time() + duration_seconds
        )

        self.failure_count += 1

        logger.warning(
            "Proxy %s cooldown for %ss "
            "(failure_count=%s)",
            self.safe_label(),
            duration_seconds,
            self.failure_count,
        )

    def safe_label(self) -> str:
        """
        Return host:port only.
        Never expose proxy credentials in logs.
        """
        return f"{self.host}:{self.port}"


# ──────────────────────────────────────────────
# Proxy Provider Abstraction
# ──────────────────────────────────────────────


class ProxyProvider:
    """
    Abstract base for proxy providers.

    Each provider returns a list of normalized ProxyNode records.
    Provider-specific details (credentials, endpoints) stay inside the
    provider implementation — never in application code.
    """

    name: str = "base"

    def load(self) -> List[ProxyNode]:
        raise NotImplementedError


@dataclass
class _FileProxyConfig:
    path: str = "proxies.txt"


class FileProxyProvider(ProxyProvider):
    """
    File-based proxy provider.

    Reads proxies from a file. Format:
      protocol://host:port:username:password
      host:port:username:password
      protocol://host:port
      host:port
    """

    name = "file"

    def __init__(self, file_path: str = "proxies.txt"):
        self.file_path = file_path

    def load(self) -> List[ProxyNode]:
        nodes: List[ProxyNode] = []
        if not os.path.exists(self.file_path):
            logger.warning(f"Proxy file not found: {self.file_path}")
            return nodes

        with open(self.file_path, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                node = self._parse_line(line, idx)
                if node:
                    nodes.append(node)

        logger.info(f"FileProxyProvider loaded {len(nodes)} proxies from {self.file_path}")
        return nodes

    def _parse_line(self, line: str, idx: int) -> Optional[ProxyNode]:
        protocol = "socks5"
        username: Optional[str] = None
        password: Optional[str] = None

        if "://" in line:
            proto_part, rest = line.split("://", 1)
            if proto_part.lower() in ("http", "https", "socks4", "socks5"):
                protocol = proto_part.lower()
                line = rest

        parts = line.split(":")
        if len(parts) < 2:
            return None

        try:
            host = parts[0]
            port = int(parts[1])
            if len(parts) >= 4:
                username = parts[2] or None
                password = parts[3] or None
        except (ValueError, IndexError):
            return None

        proxy_id = f"file_{idx}_{host}_{port}"
        if username and password:
            url = f"{protocol}://{username}:{password}@{host}:{port}"
        else:
            url = f"{protocol}://{host}:{port}"

        return ProxyNode(
            proxy_id=proxy_id,
            host=host,
            port=port,
            protocol=protocol,
            username=username,
            password=password,
            url=url,
            provider=self.name,
        )


class DecodoProxyProvider(ProxyProvider):
    """
    Decodo (BrightData) proxy provider.

    Fetches a batch of rotating proxies from the Decodo API.
    Each proxy gets a distinct session ID for IP rotation.
    Credentials are read from environment variables only:
      DECODO_USERNAME, DECODO_PASSWORD, DECODO_HOST, DECODO_PORT
      DECODO_PROXY_COUNT (optional, default 20)
    """

    name = "decodo"

    def __init__(self):
        self.username = os.environ.get("DECODO_USERNAME", "")
        self.password = os.environ.get("DECODO_PASSWORD", "")
        self.endpoint = os.environ.get("DECODO_HOST", "dc.decodo.com")
        self.port = int(os.environ.get("DECODO_PORT", "10001"))
        self.proxy_count = int(os.environ.get("DECODO_PROXY_COUNT", "10"))

    def load(self) -> List[ProxyNode]:
        if not self.username or not self.password:
            logger.warning("DecodoProxyProvider: DECODO_USERNAME/DECODO_PASSWORD not set")
            return []

        # URL-encode password (may contain special chars like =)
        from urllib.parse import quote
        enc_pass = quote(self.password, safe="")
        enc_user = quote(self.username, safe="")

        nodes = []
        for i in range(self.proxy_count):
            sid = random.randint(100000, 999999)
            proxy_id = f"decodo_{sid}"
            # Decodo uses the same credentials for all proxy slots; IP rotation
            # happens server-side. Each slot is a separate leaseable unit.
            nodes.append(ProxyNode(
                proxy_id=proxy_id,
                host=self.endpoint,
                port=self.port,
                protocol="http",
                username=self.username,
                password=self.password,
                url=f"http://{enc_user}:{enc_pass}@{self.endpoint}:{self.port}",
                provider=self.name,
            ))
        logger.info(f"DecodoProxyProvider loaded {len(nodes)} rotating proxies")
        return nodes

    def get_rotating_proxy(self) -> Optional[ProxyNode]:
        """Get a single fresh rotating proxy entry."""
        nodes = self.load()
        return nodes[0] if nodes else None


class WebshareProxyProvider(ProxyProvider):
    """
    Webshare proxy provider.

    Credentials are read from environment variables only:
      WEBSHARE_USERNAME, WEBSHARE_PASSWORD, WEBSHARE_ENDPOINT, WEBSHARE_PORT
    """

    name = "webshare"

    def __init__(self):
        self.username = os.environ.get("WEBSHARE_USERNAME", "")
        self.password = os.environ.get("WEBSHARE_PASSWORD", "")
        self.endpoint = os.environ.get("WEBSHARE_ENDPOINT", "")
        self.port = int(os.environ.get("WEBSHARE_PORT", "10000"))

    def load(self) -> List[ProxyNode]:
        if not self.username or not self.password or not self.endpoint:
            logger.warning("WebshareProxyProvider: credentials or endpoint not set")
            return []

        return [
            ProxyNode(
                proxy_id=f"webshare_{self.endpoint}_{self.port}",
                host=self.endpoint,
                port=self.port,
                protocol="socks5",
                username=self.username,
                password=self.password,
                url=f"socks5://{self.username}:{self.password}@{self.endpoint}:{self.port}",
                provider=self.name,
            )
        ]


def get_proxy_provider(provider_name: Optional[str] = None) -> ProxyProvider:
    """Factory: select provider from config/env."""
    name = (provider_name or PROXY_PROVIDER).lower()
    if name == "decodo":
        return DecodoProxyProvider()
    elif name == "webshare":
        return WebshareProxyProvider()
    else:
        return FileProxyProvider()


# ──────────────────────────────────────────────
# ProxyLeaseManager
# ──────────────────────────────────────────────


class ProxyLeaseManager:
    """
    Enterprise Proxy Lease & Cooldown Engine.

    Uses asyncio.Condition for zero-CPU blocking.
    Tracks proxy<->worker binding to prevent accidental release of another
    worker's lease.
    """

    def __init__(self, proxy_manager: "ProxyManager"):
        self.proxy_manager = proxy_manager
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self.proxy_nodes: Dict[str, ProxyNode] = {}
        self.proxy_cooldown: Set[str] = set()
        self.account_cooldown: Dict[str, float] = {}
        self._reaper_task: Optional[asyncio.Task] = None
        self._is_running = False
        self.stats: Dict[str, int] = {
            "total_acquires": 0,
            "total_releases": 0,
            "cooldown_activations": 0,
            "current_active_leases": 0,
        }

    def _proxy_id_from_record(
        self,
        proxy_dict: Dict[str, Any],
    ) -> Optional[str]:
        """
        Return stable provider/node identity.
    
        proxy_id is preferred.
        URL is only the final fallback for legacy file proxies.
        """
        proxy_id = proxy_dict.get("proxy_id")
    
        if proxy_id:
            return str(proxy_id)
    
        url = proxy_dict.get("url")
    
        if url:
            return str(url)
    
        host = proxy_dict.get(
            "host",
            proxy_dict.get("addr", ""),
        )
        port = proxy_dict.get("port")
    
        if host and port:
            return f"{host}:{port}"
    
        return None

    async def start(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        await self._sync_proxies()
        self._reaper_task = asyncio.create_task(self._auto_reaper_loop())
        logger.info("ProxyLeaseManager started with auto-reaper")

    async def stop(self) -> None:
        self._is_running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass
            self._reaper_task = None
        logger.info("ProxyLeaseManager stopped")

    async def _sync_proxies(self) -> None:
        """
        Reconcile runtime proxy registry with ProxyManager's working set.
    
        Important:
          - proxy_nodes are keyed by logical proxy_id
          - leased nodes are never deleted merely because the provider
            temporarily stopped reporting them
          - unleased stale nodes can be removed
        """
        working = list(
            self.proxy_manager.working_proxies
        )
    
        incoming: Dict[str, Dict[str, Any]] = {}
    
        for proxy_dict in working:
            proxy_id = self._proxy_id_from_record(
                proxy_dict
            )
    
            if not proxy_id:
                continue
    
            incoming[proxy_id] = proxy_dict
    
        async with self._condition:
    
            # ------------------------------------------
            # Add/update current nodes
            # ------------------------------------------
            for proxy_id, proxy_dict in incoming.items():
    
                node = self.proxy_nodes.get(proxy_id)
    
                if node is None:
                    node = ProxyNode(
                        proxy_id=proxy_id,
                        host=proxy_dict.get(
                            "host",
                            proxy_dict.get("addr", ""),
                        ),
                        port=int(
                            proxy_dict.get("port", 0)
                        ),
                        protocol=proxy_dict.get(
                            "type",
                            proxy_dict.get(
                                "proxy_type",
                                "socks5",
                            ),
                        ),
                        username=proxy_dict.get(
                            "username"
                        ),
                        password=proxy_dict.get(
                            "password"
                        ),
                        url=proxy_dict.get(
                            "url",
                            "",
                        ),
                        latency=float(
                            proxy_dict.get(
                                "latency",
                                5000.0,
                            )
                        ),
                        provider=proxy_dict.get(
                            "provider",
                            getattr(
                                self.proxy_manager.provider,
                                "name",
                                "unknown",
                            ),
                        ),
                    )
    
                    self.proxy_nodes[proxy_id] = node
    
                else:
                    # Update health/config metadata without
                    # destroying active lease state.
                    node.latency = float(
                        proxy_dict.get(
                            "latency",
                            node.latency,
                        )
                    )
    
                    node.health_state = (
                        "healthy"
                    )
    
            # ------------------------------------------
            # Remove stale UNLEASED nodes only
            # ------------------------------------------
            for proxy_id in list(
                self.proxy_nodes
            ):
                if proxy_id in incoming:
                    continue
    
                node = self.proxy_nodes[proxy_id]
    
                if node.is_leased:
                    # Keep leased resource alive until
                    # its owner explicitly releases it.
                    continue
    
                self.proxy_nodes.pop(
                    proxy_id,
                    None,
                )
    
                self.proxy_cooldown.discard(
                    proxy_id
                )

    async def _auto_reaper_loop(self) -> None:
        """Background task that wakes up exactly when cooldowns expire."""
        while self._is_running:
            try:
                now = time.time()
                expired_proxies: List[str] = []
                expired_accounts: List[str] = []


                for proxy_id in list(self.proxy_cooldown):
                    node = self.proxy_nodes.get(proxy_id)
                
                    if node and not node.is_in_cooldown():
                        expired_proxies.append(proxy_id)
                
                    elif node is None:
                        expired_proxies.append(proxy_id)

                for proxy_id in expired_proxies:
                    self.proxy_cooldown.discard(proxy_id)
                
                    logger.debug(
                        "Auto-Reaper: proxy_id=%s cooldown expired",
                        proxy_id,
                    )

                for phone, cooldown_until in list(self.account_cooldown.items()):
                    if now >= cooldown_until:
                        expired_accounts.append(phone)

                if expired_proxies or expired_accounts:
                    async with self._condition:
                        for url in expired_proxies:
                            self.proxy_cooldown.discard(url)
                            logger.debug(f"Auto-Reaper: Proxy {url} cooldown expired")
                        for phone in expired_accounts:
                            del self.account_cooldown[phone]
                            logger.debug(f"Auto-Reaper: Account +{phone} cooldown expired")
                        self._condition.notify_all()

                await asyncio.sleep(COOLDOWN_CHECK_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-Reaper error: {e}")
                await asyncio.sleep(COOLDOWN_CHECK_INTERVAL)

    async def acquire_proxy(
        self,
        phone: str,
        timeout: float = 10.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Acquire one logical proxy lease.
    
        Returns the Telethon proxy dictionary plus internal
        lease metadata used only for lifecycle management.
        """
        if not self._is_running:
            logger.warning(
                "PROXY_ACQUIRE_REJECTED | "
                "manager not running | phone=%s",
                phone,
            )
            return None
    
        await self._sync_proxies()
    
        deadline = (
            time.monotonic() + max(0.0, timeout)
        )
    
        async with self._condition:
            while self._is_running:
    
                # --------------------------------------
                # Account cooldown
                # --------------------------------------
                cooldown_until = (
                    self.account_cooldown.get(phone)
                )
    
                if cooldown_until is not None:
                    remaining = (
                        cooldown_until
                        - time.time()
                    )
    
                    if remaining <= 0:
                        self.account_cooldown.pop(
                            phone,
                            None,
                        )
                    else:
                        wait_for = min(
                            remaining,
                            max(
                                0.0,
                                deadline
                                - time.monotonic(),
                            ),
                        )
    
                        if wait_for <= 0:
                            return None
    
                        try:
                            await asyncio.wait_for(
                                self._condition.wait(),
                                timeout=wait_for,
                            )
                        except asyncio.TimeoutError:
                            if (
                                time.monotonic()
                                >= deadline
                            ):
                                return None
    
                        continue
    
                # --------------------------------------
                # Find available node
                # --------------------------------------
                for proxy_id, node in self.proxy_nodes.items():
    
                    if proxy_id in self.proxy_cooldown:
                        continue
    
                    if node.is_in_cooldown():
                        continue
    
                    if node.is_leased:
                        continue
    
                    lease_id = uuid.uuid4().hex[:16]
    
                    if not node.acquire(
                        phone,
                        lease_id,
                    ):
                        continue
    
                    self.stats[
                        "total_acquires"
                    ] += 1
    
                    self.stats[
                        "current_active_leases"
                    ] += 1
    
                    proxy = node.to_telethon_proxy()
    
                    if proxy is None:
                        node.release()
    
                        self.stats[
                            "current_active_leases"
                        ] = max(
                            0,
                            self.stats[
                                "current_active_leases"
                            ] - 1,
                        )
    
                        continue
    
                    # Internal metadata is deliberately
                    # kept alongside the Telethon proxy.
                    proxy["__proxy_id"] = proxy_id
                    proxy["__lease_id"] = lease_id
    
                    logger.debug(
                        "PROXY_ACQUIRED | "
                        "proxy=%s | phone=%s | lease=%s",
                        node.safe_label(),
                        phone,
                        lease_id,
                    )
    
                    return proxy
    
                # --------------------------------------
                # No capacity: efficiently wait
                # --------------------------------------
                remaining = (
                    deadline
                    - time.monotonic()
                )
    
                if remaining <= 0:
                    logger.warning(
                        "PROXY_ACQUIRE_TIMEOUT | "
                        "phone=%s | timeout=%ss",
                        phone,
                        timeout,
                    )
                    return None
    
                wait_for = min(
                    remaining,
                    5.0,
                )
    
                try:
                    await asyncio.wait_for(
                        self._condition.wait(),
                        timeout=wait_for,
                    )
                except asyncio.TimeoutError:
                    if (
                        time.monotonic()
                        >= deadline
                    ):
                        logger.warning(
                            "PROXY_ACQUIRE_TIMEOUT | "
                            "phone=%s | timeout=%ss",
                            phone,
                            timeout,
                        )
                        return None
    
        return None

    async def release_proxy(
        self,
        *,
        proxy_url: Optional[str],
        phone: str,
        proxy_id: Optional[str] = None,
        lease_id: Optional[str] = None,
        should_cooldown: bool = False,
        cooldown_reason: str = "",
    ) -> None:
        """
        Release exactly one proxy ownership epoch.
    
        Ownership validation:
            proxy_id
            + lease_id
            + phone
    
        A stale or duplicate release must NEVER affect
        another owner's lease or global counters.
        """
        async with self._condition:
    
            node: Optional[ProxyNode] = None
    
            # Preferred lookup: stable proxy ID.
            if proxy_id:
                node = self.proxy_nodes.get(
                    proxy_id
                )
    
            # Legacy fallback.
            if node is None and proxy_url:
                for candidate in self.proxy_nodes.values():
                    if candidate.url == proxy_url:
                        node = candidate
                        break
    
            if node is None:
                logger.warning(
                    "PROXY_RELEASE_UNKNOWN | "
                    "phone=%s | proxy_id=%s",
                    phone,
                    proxy_id,
                )
                return
    
            # ------------------------------------------
            # Must currently be leased
            # ------------------------------------------
            if not node.is_leased:
                logger.debug(
                    "PROXY_DOUBLE_RELEASE | "
                    "proxy=%s | phone=%s | lease=%s",
                    node.safe_label(),
                    phone,
                    lease_id,
                )
                return
    
            # ------------------------------------------
            # Verify account ownership
            # ------------------------------------------
            if node.leased_to != phone:
                logger.warning(
                    "PROXY_RELEASE_OWNER_MISMATCH | "
                    "proxy=%s | expected=%s | actual=%s",
                    node.safe_label(),
                    phone,
                    node.leased_to,
                )
                return
    
            # ------------------------------------------
            # Verify lease epoch
            # ------------------------------------------
            if (
                lease_id is not None
                and node.lease_id != lease_id
            ):
                logger.warning(
                    "PROXY_RELEASE_LEASE_MISMATCH | "
                    "proxy=%s | phone=%s | "
                    "expected_lease=%s | actual_lease=%s",
                    node.safe_label(),
                    phone,
                    lease_id,
                    node.lease_id,
                )
                return
    
            # ------------------------------------------
            # Perform exactly one release
            # ------------------------------------------
            if should_cooldown:
                node.put_in_cooldown()
    
                self.proxy_cooldown.add(
                    node.proxy_id
                )
    
                self.account_cooldown[
                    phone
                ] = (
                    time.time()
                    + ACCOUNT_COOLDOWN_SECONDS
                )
    
                self.stats[
                    "cooldown_activations"
                ] += 1
    
                logger.warning(
                    "PROXY_COOLDOWN | "
                    "proxy=%s | phone=%s | reason=%s",
                    node.safe_label(),
                    phone,
                    cooldown_reason,
                )
    
            else:
                node.release()
    
            self.stats[
                "total_releases"
            ] += 1
    
            self.stats[
                "current_active_leases"
            ] = max(
                0,
                self.stats[
                    "current_active_leases"
                ] - 1,
            )
    
            self._condition.notify_all()

    def get_available_count(self) -> int:
        """
        Fast read-only snapshot.
    
        Does not mutate proxy registry.
        """
        return sum(
            1
            for node in self.proxy_nodes.values()
            if (
                not node.is_leased
                and not node.is_in_cooldown()
                and node.proxy_id
                not in self.proxy_cooldown
            )
        )

    async def get_available_count_async(self) -> int:
        async with self._condition:
            return sum(
                1
                for node in self.proxy_nodes.values()
                if (
                    not node.is_leased
                    and not node.is_in_cooldown()
                    and node.proxy_id
                    not in self.proxy_cooldown
                )
            )

    def _sync_proxy_sync(self) -> None:
        """Sync from ProxyManager synchronously (for sync callers)."""
        for proxy_dict in self.proxy_manager.working_proxies:
            url = proxy_dict.get("url", "")
            if url and url not in self.proxy_nodes:
                node = ProxyNode(
                    proxy_id=url,
                    host=proxy_dict.get("host", proxy_dict.get("addr", "")),
                    port=int(proxy_dict.get("port", 0)),
                    protocol=proxy_dict.get("type", "socks5"),
                    username=proxy_dict.get("username"),
                    password=proxy_dict.get("password"),
                    url=url,
                    latency=proxy_dict.get("latency", 5000.0),
                    provider="file",
                )
                self.proxy_nodes[url] = node

    async def get_stats(self) -> Dict[str, Any]:
        async with self._condition:
            return {
                **self.stats,
                "available_proxies": sum(
                    1
                    for node in self.proxy_nodes.values()
                    if (
                        not node.is_leased
                        and not node.is_in_cooldown()
                        and node.proxy_id
                        not in self.proxy_cooldown
                    )
                ),
                "total_proxy_nodes": len(
                    self.proxy_nodes
                ),
                "proxies_in_cooldown": len(
                    self.proxy_cooldown
                ),
                "accounts_in_cooldown": len(
                    self.account_cooldown
                ),
            }


# ──────────────────────────────────────────────
# ProxyManager
# ──────────────────────────────────────────────


class ProxyManager:
    """Manages proxy list with rotation and health checking."""

    def __init__(self, proxy_file: Optional[str] = None, provider_name: Optional[str] = None):
        self.proxy_file: str = proxy_file or "proxies.txt"
        self.provider: ProxyProvider = get_proxy_provider(provider_name)
        self.proxies: List[Dict[str, Any]] = []
        self.working_proxies: List[Dict[str, Any]] = []
        self.failed_proxies: Dict[str, int] = {}
        self.last_rotation: Dict[str, float] = {}
        self.count: int = 0
        self.working_count: int = 0
        self._testing_thread: Optional[threading.Thread] = None
        self._stop_testing: bool = False
        self._tested_count: int = 0
        self._working_found: int = 0
        self._testing_active: bool = False
        self._testing_progress: Dict[str, Any] = {
            "tested": 0,
            "working": 0,
            "failed": 0,
            "percent": 0.0,
            "status": "idle",
            "error": None,
        }
        self._lock: threading.Lock = threading.Lock()
        self._session: requests.Session = self._create_session()
        self._load_proxies()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=1,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=CONNECTION_POOL_SIZE,
            pool_maxsize=CONNECTION_POOL_SIZE,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _load_proxies(self) -> None:
        """Load proxies via the provider abstraction, then convert to dict format
        for backward compatibility with existing code."""
        nodes = self.provider.load()
        self.proxies = []
        for node in nodes:
            self.proxies.append({
                "addr": node.host,
                "host": node.host,
                "port": node.port,
                "proxy_type": node.protocol,
                "type": node.protocol,
                "username": node.username,
                "password": node.password,
                "url": node.url,
                "added_at": node.added_at,
                "proxy_id": node.proxy_id,
                "latency": node.latency,
            })
        self.count = len(self.proxies)
        logger.info(f"Loaded {self.count} proxies from {self.provider.name} provider")

        if self.count == 0:
            logger.warning("No proxies found. Downloading from sources...")
            self._download_proxies()

    def _download_proxies(self) -> None:
        """Fallback: attempt to download proxies from external sources.
        
        Checks environment variables for Decodo or Webshare provider
        configuration. If no provider credentials are set, logs a warning.
        """
        provider_name = os.environ.get("PROXY_PROVIDER", "file").lower()
        if provider_name in ("decodo", "webshare"):
            logger.info(f"Switching to {provider_name} provider for proxy download")
            self.provider = get_proxy_provider(provider_name)
            nodes = self.provider.load()
            for node in nodes:
                self.proxies.append({
                    "addr": node.host,
                    "host": node.host,
                    "port": node.port,
                    "proxy_type": node.protocol,
                    "type": node.protocol,
                    "username": node.username,
                    "password": node.password,
                    "url": node.url,
                    "added_at": node.added_at,
                    "proxy_id": node.proxy_id,
                    "latency": node.latency,
                })
            self.count = len(self.proxies)
            logger.info(f"Downloaded {self.count} proxies from {provider_name} provider")
        else:
            logger.error(
                "No proxies found in file and no proxy provider configured. "
                "Set PROXY_PROVIDER=decodo or PROXY_PROVIDER=webshare with credentials, "
                "or add proxies to proxies.txt."
            )

    def _test_proxy_sync(self, proxy: Dict[str, Any], timeout: Optional[float] = None) -> bool:
        test_timeout = timeout or PROXY_TEST_TIMEOUT
        try:
            proxies = {
                "http": proxy["url"],
                "https": proxy["url"],
            }
            start_time = time.time()
            response = self._session.get(
                "https://core.telegram.org",
                proxies=proxies,
                timeout=test_timeout,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            )
            if response.status_code == 200:
                proxy["latency"] = (time.time() - start_time) * 1000
                return True
        except Exception:
            pass
        return False

    async def _test_proxy_async(self, proxy: Dict[str, Any]) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._test_proxy_sync, proxy, PROXY_TEST_TIMEOUT)

    def get_testing_progress(self) -> Dict[str, Any]:
        with self._lock:
            return self._testing_progress.copy()

    def start_background_testing(
        self,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        initial_batch: Optional[int] = None,
    ) -> None:
        if self._testing_active:
            logger.debug("Proxy testing already active")
            return
        max_workers = max_workers or MAX_WORKERS_DEFAULT
        initial_batch = initial_batch or min(100, len(self.proxies))
        self._stop_testing = False
        self._testing_active = True
        self._tested_count = 0
        self._working_found = 0

        def _update_progress(finished: bool = False) -> None:
            with self._lock:
                total = len(self.proxies)
                tested = self._tested_count
                working = self._working_found
                failed = tested - working
                self._testing_progress.update({
                    "tested": tested,
                    "working": working,
                    "failed": failed,
                    "percent": (tested / total * 100) if total > 0 else 0.0,
                    "status": "complete" if finished else "testing",
                    "error": None,
                })
                if progress_callback:
                    try:
                        progress_callback(self._testing_progress.copy())
                    except Exception as e:
                        logger.error(f"Progress callback error: {e}")

        def _test_batch(proxies_to_test: List[Dict[str, Any]]) -> None:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_proxy = {
                    executor.submit(self._test_proxy_sync, proxy): proxy
                    for proxy in proxies_to_test
                }
                for future in as_completed(future_to_proxy):
                    if self._stop_testing:
                        for f in future_to_proxy:
                            f.cancel()
                        break
                    proxy = future_to_proxy[future]
                    try:
                        result = future.result(timeout=PROXY_TEST_TIMEOUT_SLOW)
                        with self._lock:
                            self._tested_count += 1
                            if result:
                                if proxy not in self.working_proxies:
                                    self.working_proxies.append(proxy)
                                    self._working_found += 1
                                    self.working_count = len(self.working_proxies)
                            else:
                                url = proxy["url"]
                                self.failed_proxies[url] = self.failed_proxies.get(url, 0) + 1
                    except FuturesTimeoutError:
                        with self._lock:
                            self._tested_count += 1
                            url = proxy["url"]
                            self.failed_proxies[url] = self.failed_proxies.get(url, 0) + 1
                    except Exception:
                        with self._lock:
                            self._tested_count += 1
                    if self._tested_count % TEST_BATCH_SIZE == 0:
                        _update_progress()
            _update_progress()

        def _run_testing() -> None:
            try:
                if not self.proxies:
                    with self._lock:
                        self._testing_progress["status"] = "error"
                        self._testing_progress["error"] = "No proxies to test"
                    _update_progress(finished=True)
                    return
                initial_proxies = self.proxies[:initial_batch]
                if initial_proxies:
                    logger.info(f"Testing initial batch: {len(initial_proxies)} proxies")
                    _test_batch(initial_proxies)
                remaining = self.proxies[initial_batch:]
                if remaining and not self._stop_testing:
                    logger.info(f"Testing remaining {len(remaining)} proxies in background")
                    _test_batch(remaining)
            except Exception as e:
                logger.error(f"Proxy testing error: {e}")
                with self._lock:
                    self._testing_progress["status"] = "error"
                    self._testing_progress["error"] = str(e)
                _update_progress(finished=True)
            finally:
                self._testing_active = False
                _update_progress(finished=True)
                logger.info(f"Proxy testing complete: {self.working_count}/{self.count} working")

        self._testing_thread = threading.Thread(
            target=_run_testing,
            name="ProxyTester",
            daemon=True,
        )
        self._testing_thread.start()
        logger.info(f"Started proxy testing with {max_workers} workers")

    def stop_background_testing(self) -> None:
        self._stop_testing = True
        if self._testing_thread and self._testing_thread.is_alive():
            self._testing_thread.join(timeout=2.0)
        self._testing_active = False

    def test_all(
        self,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
    ) -> None:
        if not self.proxies:
            logger.warning("No proxies to test")
            return
        max_workers = max_workers or MAX_WORKERS_DEFAULT
        total = len(self.proxies)
        logger.info(f"Testing {total} proxies with {max_workers} workers...")
        self.working_proxies = []
        self.working_count = 0
        tested = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_proxy = {
                executor.submit(self._test_proxy_sync, proxy): proxy
                for proxy in self.proxies
            }
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    result = future.result(timeout=PROXY_TEST_TIMEOUT_SLOW)
                    if result:
                        self.working_proxies.append(proxy)
                except Exception:
                    pass
                tested += 1
                if tested % 10 == 0 and progress_callback:
                    working = len(self.working_proxies)
                    progress_callback(tested, working, total)
                elif tested % 50 == 0:
                    working = len(self.working_proxies)
                    percent = tested / total * 100
                    print(f"\r  {G}Testing:{RS} {tested}/{total} ({percent:.1f}%) | {G}Working:{RS} {working}", end="", flush=True)

        self.working_count = len(self.working_proxies)
        if progress_callback:
            progress_callback(tested, self.working_count, total)
        print(f"\r  {G}Complete:{RS} {self.working_count}/{total} working proxies\n")
        logger.info(f"Proxy test complete: {self.working_count}/{total} working")

    def get_proxy(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not self.working_proxies:
                return None
            available = [
                p for p in self.working_proxies
                if not self.should_rotate(p)
            ]
            if not available:
                for p in self.working_proxies:
                    self.rotate(p)
                available = self.working_proxies
            try:
                for p in available:
                    if "latency" not in p:
                        p["latency"] = 5000.0
                max_latency = max(p["latency"] for p in available) or 1.0
                weights = [max(1.0, max_latency - p["latency"] + 1.0) for p in available]
                return random.choices(available, weights=weights, k=1)[0]
            except Exception:
                return random.choice(available) if available else None

    def get_telethon_proxy(self) -> Optional[Tuple[str, str, int, bool, Optional[str], Optional[str]]]:
        proxy = self.get_proxy()
        if not proxy:
            return None
        ptype_map = {"http": "http", "socks4": "socks4", "socks5": "socks5"}
        return (
            ptype_map.get(proxy["type"], "socks5"),
            proxy["host"],
            proxy["port"],
            True,
            proxy.get("username"),
            proxy.get("password"),
        )

    def mark_failed(self, proxy: Dict[str, Any]) -> None:
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        with self._lock:
            self.failed_proxies[url] = self.failed_proxies.get(url, 0) + 1
            if self.failed_proxies[url] >= MAX_PROXY_FAILURES:
                if proxy in self.working_proxies:
                    self.working_proxies.remove(proxy)
                    self.working_count = len(self.working_proxies)
                logger.debug(f"Proxy {url} removed after {MAX_PROXY_FAILURES} failures")

    def rotate(self, proxy: Dict[str, Any]) -> None:
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        self.last_rotation[url] = time.time()

    def should_rotate(self, proxy: Dict[str, Any]) -> bool:
        url = proxy.get("url", f"{proxy['host']}:{proxy['port']}")
        last = self.last_rotation.get(url, 0.0)
        return (time.time() - last) > PROXY_ROTATION_WINDOW

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total": self.count,
                "working": self.working_count,
                "failed": len(self.failed_proxies),
                "testing": self._testing_active,
                "progress": self._testing_progress.copy(),
            }

    def clear_working(self) -> None:
        with self._lock:
            self.working_proxies = []
            self.working_count = 0

    def reload(self) -> None:
        self._load_proxies()

    def wait_for_first_proxy(self, timeout: float = 30.0) -> bool:
        start_time = time.time()
        while self._testing_active and self.working_count == 0:
            if time.time() - start_time > timeout:
                return False
            time.sleep(0.5)
        return self.working_count > 0

    # ── Legacy adapters (backward compatibility) ──

    @property
    def raw_proxies(self) -> List[str]:
        return [p["url"] for p in self.proxies]

    def parse_proxy_string(self, proxy_str: str) -> Optional[Dict[str, Any]]:
        return self._dict_to_proxy_node(proxy_str)

    @staticmethod
    def _dict_to_proxy_node(line: str) -> Optional[Dict[str, Any]]:
        """Parse a proxy string into dict format (backward compat)."""
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        protocol = "http"
        username: Optional[str] = None
        password: Optional[str] = None
        if "://" in line:
            proto_part, rest = line.split("://", 1)
            if proto_part.lower() in ("http", "https", "socks4", "socks5"):
                protocol = proto_part.lower()
                line = rest
        parts = line.split(":")
        if len(parts) < 2:
            return None
        try:
            host = parts[0]
            port = int(parts[1])
            if len(parts) >= 4:
                username = parts[2]
                password = parts[3]
        except (ValueError, IndexError):
            return None
        from urllib.parse import quote
        enc_user = quote(username, safe="") if username else None
        enc_pass = quote(password, safe="") if password else None
        return {
            "addr": host,
            "host": host,
            "port": port,
            "proxy_type": protocol,
            "type": protocol,
            "username": username,
            "password": password,
            "url": f"{protocol}://{enc_user}:{enc_pass}@{host}:{port}" if enc_user and enc_pass else f"{protocol}://{host}:{port}",
        }

    def get_secured_proxy(self) -> Optional[Dict[str, Any]]:
        return self.get_proxy()

    def flag_proxy_failure(self, proxy: Dict[str, Any]) -> None:
        self.mark_failed(proxy)
